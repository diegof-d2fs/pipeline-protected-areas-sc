"""Production-grade UCS pipeline service.

This module implements the full ETL flow for ``DAG_UCS``:
extract -> validate -> transform -> load_postgres -> load_silver -> load_gold.

Design goals:
- explicit stage contracts and state hand-off across tasks
- robust handling for bronze input as unzipped shapefile or zip package
- deterministic timestamped medallion outputs
- explicit geospatial validations (schema, geometry, CRS)
- transactional Postgres/PostGIS load with predictable idempotence per run slice
"""

from __future__ import annotations

import json
import hashlib
import logging
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import fiona
import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_batch
from shapely.geometry import MultiPolygon, Polygon, shape

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import DomainPipelineService, TaskExecutionContext
from scripts_python.manifest_input import load_manifest_input
from scripts_python.object_storage import push_quality

LOGGER = logging.getLogger("pipeline.ucs")

# Canonical timestamp folder format used by all medallion layers.
# Corrected from user-proposed order to: yyyy-mm-dd-hh-mm-ss
TEMPORAL_FOLDER_FORMAT = "%Y-%m-%d-%H-%M-%S"
LEGACY_TEMPORAL_FOLDER_FORMAT = "%Y-%m-%d-%S-%M-%H"

# Shapefile mandatory sidecars for a valid dataset.
REQUIRED_SHAPEFILE_EXTENSIONS = (".shp", ".shx", ".dbf")
QUALITY_FOLDER_NAME = "quality"

RESULT_REASON_DESCRIPTIONS = {
    "loaded_postgres": "Carga no Postgres concluida com sucesso.",
    "no_records_with_valid_uc_id": "Nenhum registro possui uc_id valido para carga.",
    "no_eligible_rows_after_merge_rules": "Registros foram rejeitados pelas regras de merge/update_geom.",
    "no_valid_geometry_for_insert": "Registros elegiveis foram rejeitados por geometria nula ou vazia.",
}

REJECTION_REASON_DESCRIPTIONS = {
    "NO_UC_ID": "Registro sem uc_id valido na origem.",
    "SKIPPED_BY_UPDATE_RULE": "Registro existente com update_geom diferente de TRUE.",
    "INVALID_OR_EMPTY_GEOMETRY": "Registro com geometria nula ou vazia apos transformacao.",
}

UF_NAME_TO_CODE = {
    "acre": "AC",
    "alagoas": "AL",
    "amapa": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceara": "CE",
    "distritofederal": "DF",
    "espiritosanto": "ES",
    "goias": "GO",
    "maranhao": "MA",
    "matogrosso": "MT",
    "matogrossodosul": "MS",
    "minasgerais": "MG",
    "para": "PA",
    "paraiba": "PB",
    "parana": "PR",
    "pernambuco": "PE",
    "piaui": "PI",
    "riodejaneiro": "RJ",
    "riograndedonorte": "RN",
    "riograndedosul": "RS",
    "rondonia": "RO",
    "roraima": "RR",
    "santacatarina": "SC",
    "saopaulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}
UF_CODES = set(UF_NAME_TO_CODE.values())

UC_DB_COLUMNS = [
    "uc_id",
    "cd_cnuc",
    "wdpa_pid",
    "nm_uc",
    "dt_criacao",
    "ds_ato_legal",
    "ds_grupo",
    "ds_categoria",
    "ds_esfera",
    "nm_orgao_gestor",
    "sg_uf",
    "area_total_ha",
    "area_ato_ha",
    "update_geom",
    "geom",
    "dt_bronze",
    "dt_silver",
    "dt_gold",
    "versao_dag",
]


class UcsPipelineError(RuntimeError):
    """Base domain exception with explicit operational semantics."""


class InputValidationError(UcsPipelineError):
    """Raised when bronze source payload violates expected contracts."""


@dataclass(frozen=True)
class BronzeSelection:
    """Represents the selected bronze batch and inferred source date."""

    batch_dir: Path
    dt_bronze: str


class UcsPipelineService(DomainPipelineService):
    """Specialized implementation for the UCS domain."""

    def __init__(self, config: PipelineConfig | None = None) -> None:
        super().__init__(domain_name="ucs", config=config)
        self.logger = LOGGER

    def extract(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Locate and materialize UCS source data from BRONZE.

        Supported source modes:
        - unzipped shapefile components already present in bronze batch directory
        - zip package containing shapefile components
        """
        source_root = Path(self.config.medallion_bronze_path)
        if not source_root.exists():
            raise InputValidationError(f"Bronze source path does not exist: {source_root}")

        run_dir = self._run_dir(context)
        run_dir.mkdir(parents=True, exist_ok=True)

        directed = load_manifest_input(source_root, context.conf, expected_domain="uc")
        if directed is not None:
            batch_dir = directed.manifest_path.parent
            source_path = directed.canonical_path
            source_mode = "manifest_geojson"
            dt_bronze = str(directed.payload.get("created_at", context.logical_date))
            manifest_payload = directed.payload
        else:
            requested_batch = str((context.conf or {}).get("bronze_batch", "")).strip()
            if requested_batch:
                bronze = self._select_bronze_batch_from_context(source_root, context.conf)
                batch_dir = bronze.batch_dir
                source_path, source_mode = self._locate_shapefile(bronze.batch_dir, run_dir)
                dt_bronze = bronze.dt_bronze
            else:
                selections = self._list_bronze_batches(source_root)
                batch_dir = source_root / self.domain_name
                source_path = self._materialize_bronze_union(selections, run_dir)
                source_mode = "bronze_union"
                dt_bronze = max(selection.dt_bronze for selection in selections)
            manifest_payload = None

        state = {
            "domain": self.domain_name,
            "run_id": context.run_id,
            "dag_id": context.dag_id,
            "logical_date": context.logical_date,
            "temporal_folder": self._temporal_folder_stamp(context),
            "bronze_batch_dir": str(batch_dir),
            "bronze_mode": source_mode,
            "source_path": str(source_path),
            "shapefile_path": str(source_path),
            "dt_bronze": dt_bronze,
            "import_id": directed.import_id if directed else None,
            "manifest": manifest_payload,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }

        self._save_state(context, state)
        self.logger.info(
            "UCS extract completed. mode=%s batch=%s shapefile=%s dt_bronze=%s",
            source_mode,
            batch_dir,
            source_path,
            dt_bronze,
        )
        return self._result(
            context,
            status="extracted",
            bronze_batch_dir=str(batch_dir),
            source_path=str(source_path),
            dt_bronze=dt_bronze,
            source_mode=source_mode,
            temporal_folder=state["temporal_folder"],
        )

    def validate(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Validate shapefile integrity, schema minimums, geometry and CRS."""
        state = self._load_state(context)
        source_path = Path(state.get("source_path", state["shapefile_path"]))

        if source_path.suffix.lower() == ".shp":
            self._validate_shapefile_components(source_path)

        gdf = self._apply_manifest_metadata(self._read_geodata(source_path), state)
        if gdf.empty:
            raise InputValidationError("UCS shapefile is empty.")

        if self._detect_source_column(gdf.columns, ["nm_uc", "nome_uc", "nome", "nm_unid_con", "name"]) is None:
            raise InputValidationError(
                "Minimum schema validation failed: no UC name-like column found (nm_uc/nome_uc/nome)."
            )

        if gdf.crs is None:
            raise InputValidationError(
                "CRS is missing in source data. Provide a valid .prj or source CRS metadata."
            )

        geom_null = int(gdf.geometry.isna().sum())
        geom_empty = int(gdf.geometry.is_empty.sum())
        geom_invalid = int((~gdf.geometry.is_valid).sum())

        if len(gdf) == geom_null + geom_empty:
            raise InputValidationError("All geometries are null or empty.")

        manifest = state.get("manifest") or {}
        if manifest.get("operation") in {"create", "create_with_zone"}:
            duplicate_matches = self._find_create_duplicates(gdf)
            if duplicate_matches:
                self._write_api_duplicate_result(context, state, duplicate_matches)
                raise InputValidationError(
                    "UC_ALREADY_EXISTS: create import matches an existing UC by a strong "
                    "identifier or equivalent geometry."
                )

        state["validation"] = {
            "records": int(len(gdf)),
            "crs": str(gdf.crs),
            "geom_null": geom_null,
            "geom_empty": geom_empty,
            "geom_invalid": geom_invalid,
        }
        self._save_state(context, state)

        self.logger.info(
            "UCS validate completed. records=%s crs=%s invalid=%s",
            len(gdf),
            gdf.crs,
            geom_invalid,
        )
        return self._result(context, status="validated", **state["validation"])

    def transform(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Normalize UCS schema and geometry for PostGIS/SILVER/GOLD outputs."""
        state = self._load_state(context)
        source_path = Path(state.get("source_path", state["shapefile_path"]))

        gdf = self._apply_manifest_metadata(self._read_geodata(source_path), state)
        gdf = self._ensure_epsg_4674(gdf)
        gdf = self._repair_geometries(gdf)

        transformed = self._map_to_uc_schema(gdf, state["dt_bronze"], context.dag_id)
        transformed = transformed[~transformed.geometry.isna()]
        transformed = transformed[~transformed.geometry.is_empty]

        if transformed.empty:
            raise InputValidationError("No valid UCS records remained after transformation.")

        transformed_path = self._run_dir(context) / "ucs_transformed.gpkg"
        transformed.to_file(transformed_path, layer="uc", driver="GPKG")

        state["transformed_path"] = str(transformed_path)
        state["transformed_records"] = int(len(transformed))
        self._save_state(context, state)

        self.logger.info("UCS transform completed. transformed_records=%s", len(transformed))
        return self._result(
            context,
            status="transformed",
            transformed_path=str(transformed_path),
            transformed_records=int(len(transformed)),
        )

    def load_postgres(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Load normalized UCS records into PostGIS table uc transactionally."""
        state = self._load_state(context)
        transformed = self._read_geodata(Path(state["transformed_path"]), layer="uc")
        source_records = int(len(transformed))

        # Postgres is the final stage for UCS DAG. Persist medallion lineage timestamps
        # directly from state so inserts are complete even when no prior DB rows exist.
        transformed["dt_bronze"] = pd.to_datetime(transformed["dt_bronze"], errors="coerce").dt.date
        transformed["dt_silver"] = state.get("dt_silver")
        transformed["dt_gold"] = state.get("dt_gold")
        transformed["versao_dag"] = context.dag_id

        if "uc_id" not in transformed.columns:
            raise UcsPipelineError("Column 'uc_id' is required for UCS merge/upsert semantics.")

        transformed["uc_id"] = transformed["uc_id"].apply(self._normalize_uc_id)
        rejected_no_uc_id = transformed[transformed["uc_id"].isna()].copy()
        transformed = transformed[~transformed["uc_id"].isna()].copy()
        skipped_no_uc_id = int(len(rejected_no_uc_id))
        rejected_frames: dict[str, pd.DataFrame] = {}
        if not rejected_no_uc_id.empty:
            rejected_frames["NO_UC_ID"] = rejected_no_uc_id

        if transformed.empty:
            skipped_records = skipped_no_uc_id
            self._write_quality_report(
                context=context,
                stage="load_postgres",
                source_records=source_records,
                inserted_records=0,
                inserted_new_records=0,
                inserted_updated_records=0,
                skipped_records=skipped_records,
                result_reason="no_records_with_valid_uc_id",
                rejected_frames=rejected_frames,
            )
            return self._result(
                context,
                status="loaded_postgres",
                inserted_records=0,
                skipped_records=skipped_records,
                reason="no_records_with_valid_uc_id",
            )

        if not self.config.project_db_url:
            raise UcsPipelineError("PROJECT_DB_URL is required for load_postgres.")

        manifest = state.get("manifest") or {}
        if manifest.get("operation") == "extinguish":
            return self._load_directed_extinguish(context, state)
        if manifest.get("operation") in {"create", "create_with_zone", "update", "replace_point"}:
            return self._load_directed_postgres(
                context=context,
                state=state,
                transformed=transformed,
                source_records=source_records,
                skipped_no_uc_id=skipped_no_uc_id,
                rejected_frames=rejected_frames,
            )

        transformed_to_insert = transformed.copy()
        rows: list[tuple[Any, ...]] = []
        uc_ids_to_refresh: list[str] = []
        total_records = int(len(transformed))
        inserted_new_records = 0
        inserted_updated_records = 0
        skipped_records = 0

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE uc
                    ADD COLUMN IF NOT EXISTS dt_bronze DATE,
                    ADD COLUMN IF NOT EXISTS dt_silver TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS dt_gold TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS update_geom TEXT;
                    """
                )

                candidate_uc_ids = transformed_to_insert["uc_id"].dropna().astype(str).unique().tolist()
                existing_uc_ids: set[str] = set()
                if candidate_uc_ids:
                    cur.execute(
                        """
                        SELECT uc_id
                        FROM uc
                        WHERE uc_id = ANY(%s);
                        """,
                        (candidate_uc_ids,),
                    )
                    existing_uc_ids = {str(row[0]) for row in cur.fetchall() if row[0] is not None}

                is_existing = transformed_to_insert["uc_id"].astype(str).isin(existing_uc_ids)
                is_update_true = transformed_to_insert["update_geom"].apply(self._is_truthy_update_geom)

                # Regra operacional:
                # - UC nova (nao existe no banco): SEMPRE insere (primeira carga).
                # - UC existente: so atualiza se update_geom == TRUE.
                should_insert = (~is_existing) | (is_existing & is_update_true)

                rejected_update = transformed_to_insert[~should_insert].copy()
                if not rejected_update.empty:
                    rejected_frames["SKIPPED_BY_UPDATE_RULE"] = rejected_update

                transformed_to_insert = transformed_to_insert[should_insert].copy()
                skipped_by_update_rules = int(len(rejected_update))
                skipped_records = int(skipped_no_uc_id + skipped_by_update_rules)

                if transformed_to_insert.empty:
                    self.logger.info(
                        "UCS load_postgres skipped. no eligible rows after merge rules. total_records=%s skipped_records=%s",
                        total_records,
                        skipped_records,
                    )
                    self._write_quality_report(
                        context=context,
                        stage="load_postgres",
                        source_records=source_records,
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        result_reason="no_eligible_rows_after_merge_rules",
                        rejected_frames=rejected_frames,
                    )
                    return self._result(
                        context,
                        status="loaded_postgres",
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        reason="no_eligible_rows_after_merge_rules",
                    )

                rows_existing_to_update = transformed_to_insert[
                    transformed_to_insert["uc_id"].astype(str).isin(existing_uc_ids)
                ]
                geom_column = str(transformed_to_insert.geometry.name)
                before_geom_records = int(len(transformed_to_insert))
                transformed_to_insert = transformed_to_insert[~transformed_to_insert[geom_column].isna()].copy()
                transformed_to_insert = transformed_to_insert[
                    ~gpd.GeoSeries(transformed_to_insert[geom_column], crs=transformed.crs).is_empty
                ].copy()
                skipped_by_invalid_geom = int(before_geom_records - len(transformed_to_insert))
                if skipped_by_invalid_geom > 0:
                    invalid_geom = transformed[should_insert].copy()
                    invalid_geom = invalid_geom[
                        invalid_geom[geom_column].isna()
                        | gpd.GeoSeries(invalid_geom[geom_column], crs=transformed.crs).is_empty
                    ].copy()
                    if not invalid_geom.empty:
                        rejected_frames["INVALID_OR_EMPTY_GEOMETRY"] = invalid_geom
                skipped_records += skipped_by_invalid_geom

                if transformed_to_insert.empty:
                    self._write_quality_report(
                        context=context,
                        stage="load_postgres",
                        source_records=source_records,
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        result_reason="no_valid_geometry_for_insert",
                        rejected_frames=rejected_frames,
                    )
                    return self._result(
                        context,
                        status="loaded_postgres",
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        reason="no_valid_geometry_for_insert",
                    )

                rows_existing_to_update = transformed_to_insert[
                    transformed_to_insert["uc_id"].astype(str).isin(existing_uc_ids)
                ]
                uc_ids_to_refresh = rows_existing_to_update["uc_id"].dropna().astype(str).unique().tolist()
                inserted_updated_records = int(len(rows_existing_to_update))
                inserted_new_records = int(len(transformed_to_insert) - inserted_updated_records)

                rows = [self._row_to_db_tuple(row, geom_column) for _, row in transformed_to_insert.iterrows()]

                try:
                    execute_batch(
                        cur,
                        """
                        INSERT INTO uc (
                            uc_id, cd_cnuc, wdpa_pid, nm_uc, dt_criacao, ds_ato_legal,
                            ds_grupo, ds_categoria, ds_esfera, nm_orgao_gestor, sg_uf,
                            area_total_ha, area_ato_ha, update_geom, geom, dt_bronze, dt_silver, dt_gold, versao_dag
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4674), %s, %s, %s, %s
                        )
                        ON CONFLICT (uc_id) WHERE uc_id IS NOT NULL DO UPDATE SET
                            cd_cnuc = EXCLUDED.cd_cnuc,
                            wdpa_pid = EXCLUDED.wdpa_pid,
                            nm_uc = EXCLUDED.nm_uc,
                            dt_criacao = EXCLUDED.dt_criacao,
                            ds_ato_legal = EXCLUDED.ds_ato_legal,
                            ds_grupo = EXCLUDED.ds_grupo,
                            ds_categoria = EXCLUDED.ds_categoria,
                            ds_esfera = EXCLUDED.ds_esfera,
                            nm_orgao_gestor = EXCLUDED.nm_orgao_gestor,
                            sg_uf = EXCLUDED.sg_uf,
                            area_total_ha = EXCLUDED.area_total_ha,
                            area_ato_ha = EXCLUDED.area_ato_ha,
                            update_geom = EXCLUDED.update_geom,
                            geom = EXCLUDED.geom,
                            dt_bronze = EXCLUDED.dt_bronze,
                            dt_silver = EXCLUDED.dt_silver,
                            dt_gold = EXCLUDED.dt_gold,
                            versao_dag = EXCLUDED.versao_dag,
                            versao_registro = uc.versao_registro + 1,
                            atualizado_em = CURRENT_TIMESTAMP;
                        """,
                        rows,
                        page_size=500,
                    )
                except psycopg2.errors.UniqueViolation as exc:
                    conn.rollback()
                    duplicate_matches = self._find_create_duplicates(transformed_to_insert)
                    self._write_api_duplicate_result(context, state, duplicate_matches)
                    raise InputValidationError(
                        "UC_ALREADY_EXISTS: a concurrent create inserted the same UC."
                    ) from exc

                loaded_uc_ids = transformed_to_insert["uc_id"].dropna().astype(str).unique().tolist()
                self._record_scheduled_geometry_versions(cur, context, loaded_uc_ids)

        self._write_quality_report(
            context=context,
            stage="load_postgres",
            source_records=source_records,
            inserted_records=len(rows),
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
            result_reason="loaded_postgres",
            rejected_frames=rejected_frames,
        )

        self.logger.info(
            "UCS load_postgres completed. inserted_records=%s inserted_new=%s inserted_updated=%s skipped_records=%s",
            len(rows),
            inserted_new_records,
            inserted_updated_records,
            skipped_records,
        )
        return self._result(
            context,
            status="loaded_postgres",
            inserted_records=len(rows),
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
        )

    def _record_scheduled_geometry_versions(
        self, cur: Any, context: TaskExecutionContext, uc_ids: list[str]
    ) -> None:
        """Keep geometry history and audit events for the scheduled Bronze load.

        Runs in the same transaction as the upsert. A UC without an active version receives
        its first version and `UC_CREATED`; a UC whose geometry changed has the active version
        closed and the next one opened with `UC_UPDATED`. Idempotency keys are bound to the
        Airflow run, so a retry of the same run never duplicates events.
        """
        if not uc_ids:
            return
        cur.execute(
            """
            UPDATE uc_geometry_version AS v
            SET fl_ativa = FALSE,
                dt_fim_vigencia = GREATEST(CURRENT_DATE, v.dt_inicio_vigencia)
            FROM uc AS u
            WHERE v.id_uc = u.id_uc
              AND u.uc_id = ANY(%s)
              AND v.fl_ativa
              AND NOT ST_Equals(v.geom, u.geom);
            """,
            (uc_ids,),
        )
        cur.execute(
            """
            INSERT INTO uc_geometry_version (
                id_uc, numero_versao, geom, fl_ativa, dt_inicio_vigencia,
                motivo, fonte, ator, correlation_id
            )
            SELECT
                u.id_uc,
                GREATEST(
                    u.versao_registro,
                    COALESCE(
                        (SELECT MAX(x.numero_versao) FROM uc_geometry_version AS x WHERE x.id_uc = u.id_uc),
                        0
                    ) + 1
                ),
                u.geom,
                TRUE,
                CURRENT_DATE,
                'Carga agendada a partir da Bronze',
                COALESCE('bronze:' || u.dt_bronze::text, 'bronze'),
                'airflow',
                %s
            FROM uc AS u
            WHERE u.uc_id = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1 FROM uc_geometry_version AS a WHERE a.id_uc = u.id_uc AND a.fl_ativa
              )
            RETURNING id_uc, numero_versao;
            """,
            (context.run_id, uc_ids),
        )
        for id_uc, numero_versao in cur.fetchall():
            first_version = not self._has_previous_geometry_version(cur, id_uc, numero_versao)
            event_type = "UC_CREATED" if first_version else "UC_UPDATED"
            new_state = {"id_uc": int(id_uc), "geometry_version": int(numero_versao), "dag_run_id": context.run_id}
            cur.execute(
                """
                INSERT INTO cadastral_event (
                    event_id, entity_type, entity_id, event_type, actor, reason, source,
                    correlation_id, idempotency_key, previous_state, new_state
                ) VALUES (%s, 'UC', %s, %s, 'airflow', 'Carga agendada a partir da Bronze', 'bronze',
                          %s, %s, NULL, %s)
                ON CONFLICT (idempotency_key) DO NOTHING;
                """,
                (
                    str(uuid4()),
                    int(id_uc),
                    event_type,
                    context.run_id,
                    f"scheduled:{context.run_id}:uc:{int(id_uc)}:v{int(numero_versao)}",
                    Json(new_state),
                ),
            )

    @staticmethod
    def _has_previous_geometry_version(cur: Any, id_uc: int, numero_versao: int) -> bool:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM uc_geometry_version WHERE id_uc = %s AND numero_versao < %s);",
            (id_uc, numero_versao),
        )
        return bool(cur.fetchone()[0])

    def _load_directed_extinguish(
        self, context: TaskExecutionContext, state: dict[str, Any]
    ) -> dict[str, Any]:
        manifest = state["manifest"]
        metadata = manifest.get("metadata") or {}
        key = str(manifest.get("idempotency_key") or manifest["import_id"])
        identifiers = [metadata.get(field) for field in ("official_identifier", "cd_cnuc", "wdpa_pid")]
        def fail(code: str, detail: str) -> None:
            self._write_api_error_result(context, state, code, detail)
            raise InputValidationError(f"{code}: {detail}")

        if not metadata.get("reason") or not metadata.get("expected_version") or not any(identifiers):
            fail("INVALID_EXTINGUISH_METADATA", "Fonte, justificativa, versão e identidade são obrigatórias.")
        with psycopg2.connect(self.config.project_db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT entity_id FROM cadastral_event WHERE idempotency_key = %s", (key,))
            replay = cur.fetchone()
            if not replay:
                cur.execute(
                    """SELECT id_uc, situacao, versao_registro FROM uc
                       WHERE (%s IS NOT NULL AND uc_id = %s)
                          OR (%s IS NOT NULL AND cd_cnuc = %s)
                          OR (%s IS NOT NULL AND wdpa_pid = %s)
                       ORDER BY id_uc FOR UPDATE""",
                    tuple(value for identifier in identifiers for value in (identifier, identifier)),
                )
                matches = cur.fetchall()
                if len(matches) != 1:
                    fail("UC_UPDATE_TARGET_NOT_FOUND" if not matches else "UC_UPDATE_TARGET_AMBIGUOUS",
                         "A identidade deve corresponder a exatamente uma UC.")
                uc_id, situation, version = matches[0]
                if situation != "ATIVA":
                    fail("UC_ALREADY_EXTINCT", "A UC já está extinta.")
                if int(version) != int(metadata["expected_version"]):
                    fail("UC_VERSION_CONFLICT", "A versão vigente diverge da versão esperada.")
                cur.execute(
                    """UPDATE uc SET situacao = 'EXTINTA', dt_fim_vigencia = CURRENT_DATE,
                       versao_registro = versao_registro + 1, atualizado_em = CURRENT_TIMESTAMP
                       WHERE id_uc = %s""", (uc_id,),
                )
                for table in ("za_oficial", "buffer_abrangencia", "uc_geometry_version"):
                    cur.execute(
                        f"""UPDATE {table} SET fl_ativa = FALSE,
                            dt_fim_vigencia = GREATEST(CURRENT_DATE, dt_inicio_vigencia)
                            WHERE id_uc = %s AND fl_ativa = TRUE""", (uc_id,),
                    )
                cur.execute(
                    """INSERT INTO cadastral_event (
                       event_id, entity_type, entity_id, event_type, actor, reason, source,
                       correlation_id, idempotency_key, previous_state, new_state)
                       VALUES (%s, 'UC', %s, 'UC_EXTINGUISHED', %s, %s, %s, %s, %s, %s, %s)""",
                    (str(uuid4()), uc_id, manifest.get("actor") or metadata.get("actor") or "api-client",
                     metadata["reason"], metadata.get("source"), manifest.get("correlation_id") or manifest["import_id"], key,
                     Json({"status": situation, "version": version}),
                     Json({"status": "EXTINTA", "version": version + 1,
                           "import_id": manifest["import_id"], "dag_run_id": context.run_id})),
                )
        return self._directed_load_result(
            context=context, source_records=1, inserted_new_records=0,
            inserted_updated_records=0 if replay else 1, skipped_records=0,
            rejected_frames={}, idempotent_replay=bool(replay),
        )

    def _load_directed_postgres(
        self,
        *,
        context: TaskExecutionContext,
        state: dict[str, Any],
        transformed: gpd.GeoDataFrame,
        source_records: int,
        skipped_no_uc_id: int,
        rejected_frames: dict[str, pd.DataFrame],
    ) -> dict[str, Any]:
        """Apply one API-directed UC mutation while preserving identity and geometry history."""
        manifest = state["manifest"]
        operation = str(manifest["operation"])
        with_zone = operation == "create_with_zone"
        if with_zone:
            operation = "create"
        metadata = manifest.get("metadata") or {}
        import_id = str(manifest.get("import_id") or state.get("import_id") or "")
        correlation_id = str(manifest.get("correlation_id") or import_id)
        idempotency_key = str(manifest.get("idempotency_key") or import_id)
        actor = str(manifest.get("actor") or metadata.get("actor") or "api-client")
        source = str(metadata.get("source") or "api-bronze-airflow")
        reason = str(
            metadata.get("reason")
            or (
                "Cadastro inicial dirigido pela API"
                if operation == "create"
                else "Substituição do ponto pelo polígono oficial"
                if operation == "replace_point"
                else "Atualização dirigida pela API"
            )
        )

        if operation == "create" and len(transformed) > 1:
            return self._load_directed_create_batch(
                context=context,
                state=state,
                transformed=transformed,
                source_records=source_records,
                skipped_no_uc_id=skipped_no_uc_id,
                rejected_frames=rejected_frames,
                import_id=import_id,
                correlation_id=correlation_id,
                idempotency_key=idempotency_key,
                actor=actor,
                source=source,
                reason=reason,
            )

        if len(transformed) != 1:
            error_code = "DIRECTED_IMPORT_REQUIRES_SINGLE_UC"
            detail = "Uma mutação dirigida pela API deve conter exatamente uma UC."
            self._write_api_error_result(context, state, error_code, detail)
            raise InputValidationError(f"{error_code}: {detail}")

        row = transformed.iloc[0]
        geom_column = str(transformed.geometry.name)
        geometry = row.get(geom_column)
        if geometry is None or geometry.is_empty:
            error_code = "INVALID_OR_EMPTY_GEOMETRY"
            detail = "A mutação dirigida não possui geometria válida para persistência."
            self._write_api_error_result(context, state, error_code, detail)
            raise InputValidationError(f"{error_code}: {detail}")

        official_identifier = self._none_if_nan(metadata.get("official_identifier"))
        cd_cnuc = self._none_if_nan(metadata.get("cd_cnuc"))
        wdpa_pid = self._none_if_nan(metadata.get("wdpa_pid"))
        expected_version = metadata.get("expected_version")
        if operation in {"update", "replace_point"} and expected_version is None:
            error_code = "UC_EXPECTED_VERSION_REQUIRED"
            detail = "O manifesto de atualização deve informar metadata.expected_version."
            self._write_api_error_result(context, state, error_code, detail)
            raise InputValidationError(f"{error_code}: {detail}")

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT entity_id FROM cadastral_event WHERE idempotency_key = %s",
                    (idempotency_key,),
                )
                replay = cur.fetchone()
                if replay:
                    self.logger.info(
                        "Directed UC mutation replay detected. import_id=%s id_uc=%s",
                        import_id,
                        replay[0],
                    )
                    return self._directed_load_result(
                        context=context,
                        source_records=source_records,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_no_uc_id,
                        rejected_frames=rejected_frames,
                        idempotent_replay=True,
                    )

                existing = None
                if operation in {"update", "replace_point"}:
                    cur.execute(
                        """
                        SELECT id_uc, uc_id, cd_cnuc, wdpa_pid, nm_uc, versao_registro,
                               ST_GeometryType(geom), situacao
                        FROM uc
                        WHERE (%s IS NOT NULL AND uc_id = %s)
                           OR (%s IS NOT NULL AND cd_cnuc = %s)
                           OR (%s IS NOT NULL AND wdpa_pid = %s)
                        ORDER BY id_uc
                        FOR UPDATE;
                        """,
                        (
                            official_identifier,
                            official_identifier,
                            cd_cnuc,
                            cd_cnuc,
                            wdpa_pid,
                            wdpa_pid,
                        ),
                    )
                    matches = cur.fetchall()
                    if len(matches) != 1:
                        error_code = "UC_UPDATE_TARGET_NOT_FOUND" if not matches else "UC_UPDATE_TARGET_AMBIGUOUS"
                        detail = (
                            "Nenhuma UC corresponde aos identificadores fortes informados."
                            if not matches
                            else "Os identificadores informados correspondem a mais de uma UC."
                        )
                        self._write_api_error_result(context, state, error_code, detail)
                        raise InputValidationError(f"{error_code}: {detail}")
                    existing = matches[0]
                    if existing[7] != "ATIVA":
                        error_code = "UC_NOT_ACTIVE"
                        detail = "Somente uma UC ativa pode ter sua geometria alterada."
                        self._write_api_error_result(context, state, error_code, detail)
                        raise InputValidationError(f"{error_code}: {detail}")
                    if operation == "replace_point" and existing[6] != "ST_Point":
                        error_code = "UC_REPLACE_POINT_TARGET_NOT_POINT"
                        detail = "A geometria vigente da UC-alvo não é Point."
                        self._write_api_error_result(context, state, error_code, detail)
                        raise InputValidationError(f"{error_code}: {detail}")
                    if operation == "replace_point" and geometry.geom_type not in {
                        "Polygon",
                        "MultiPolygon",
                    }:
                        error_code = "UC_REPLACE_POINT_REQUIRES_POLYGON"
                        detail = "A nova geometria deve ser Polygon ou MultiPolygon."
                        self._write_api_error_result(context, state, error_code, detail)
                        raise InputValidationError(f"{error_code}: {detail}")
                    if int(existing[5]) != int(expected_version):
                        error_code = "UC_VERSION_CONFLICT"
                        detail = (
                            f"A UC está na versão {existing[5]}, mas a solicitação esperava "
                            f"a versão {expected_version}."
                        )
                        self._write_api_error_result(context, state, error_code, detail)
                        raise InputValidationError(f"{error_code}: {detail}")

                values = self._row_to_db_tuple(row, geom_column)
                if operation == "create":
                    try:
                        cur.execute(
                            """
                            INSERT INTO uc (
                                uc_id, cd_cnuc, wdpa_pid, nm_uc, dt_criacao, ds_ato_legal,
                                ds_grupo, ds_categoria, ds_esfera, nm_orgao_gestor, sg_uf,
                                area_total_ha, area_ato_ha, update_geom, geom, dt_bronze,
                                dt_silver, dt_gold, versao_dag, criado_por
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4674),
                                %s, %s, %s, %s, %s
                            )
                            RETURNING id_uc, versao_registro, uc_id, cd_cnuc, wdpa_pid, nm_uc;
                            """,
                            (*values, actor),
                        )
                    except psycopg2.errors.UniqueViolation as exc:
                        conn.rollback()
                        duplicate_matches = self._find_create_duplicates(transformed)
                        self._write_api_duplicate_result(context, state, duplicate_matches)
                        raise InputValidationError(
                            "UC_ALREADY_EXISTS: a concurrent create inserted the same UC."
                        ) from exc
                    current = cur.fetchone()
                    event_type = "UC_CREATED"
                    previous_state = None
                    inserted_new_records = 1
                    inserted_updated_records = 0
                else:
                    assert existing is not None
                    id_uc = int(existing[0])
                    previous_state = {
                        "id_uc": id_uc,
                        "uc_id": existing[1],
                        "cd_cnuc": existing[2],
                        "wdpa_pid": existing[3],
                        "name": existing[4],
                        "version": int(existing[5]),
                    }
                    cur.execute(
                        """
                        UPDATE uc SET
                            cd_cnuc = COALESCE(%s, cd_cnuc),
                            wdpa_pid = COALESCE(%s, wdpa_pid),
                            nm_uc = COALESCE(%s, nm_uc),
                            dt_criacao = COALESCE(%s, dt_criacao),
                            ds_ato_legal = COALESCE(%s, ds_ato_legal),
                            ds_grupo = COALESCE(%s, ds_grupo),
                            ds_categoria = COALESCE(%s, ds_categoria),
                            ds_esfera = COALESCE(%s, ds_esfera),
                            nm_orgao_gestor = COALESCE(%s, nm_orgao_gestor),
                            sg_uf = COALESCE(%s, sg_uf),
                            area_total_ha = COALESCE(%s, area_total_ha),
                            area_ato_ha = COALESCE(%s, area_ato_ha),
                            update_geom = 'TRUE',
                            geom = ST_SetSRID(ST_GeomFromText(%s), 4674),
                            dt_bronze = %s,
                            dt_silver = %s,
                            dt_gold = %s,
                            versao_dag = %s,
                            versao_registro = versao_registro + 1,
                            atualizado_em = CURRENT_TIMESTAMP
                        WHERE id_uc = %s
                        RETURNING id_uc, versao_registro, uc_id, cd_cnuc, wdpa_pid, nm_uc;
                        """,
                        (
                            values[1], values[2], values[3], values[4], values[5], values[6],
                            values[7], values[8], values[9], values[10], values[11], values[12],
                            values[14], values[15], values[16], values[17], values[18], id_uc,
                        ),
                    )
                    current = cur.fetchone()
                    cur.execute(
                        """
                        UPDATE uc_geometry_version
                        SET fl_ativa = FALSE, dt_fim_vigencia = CURRENT_DATE
                        WHERE id_uc = %s AND fl_ativa = TRUE;
                        """,
                        (id_uc,),
                    )
                    event_type = (
                        "UC_POINT_REPLACED_BY_POLYGON"
                        if operation == "replace_point"
                        else "UC_UPDATED"
                    )
                    inserted_new_records = 0
                    inserted_updated_records = 1

                id_uc = int(current[0])
                version = int(current[1])
                if with_zone:
                    zone = metadata["official_zone"]
                    root = Path(self.config.medallion_bronze_path).resolve()
                    zone_path = (root / zone["canonical_key"]).resolve()
                    if root not in zone_path.parents:
                        raise InputValidationError("Official zone path escapes Bronze.")
                    features = json.loads(zone_path.read_text(encoding="utf-8"))["features"]
                    if len(features) != 1:
                        raise InputValidationError("Official zone must have exactly one feature.")
                    zone_geometry = shape(features[0]["geometry"])
                    if zone_geometry.geom_type not in {"Polygon", "MultiPolygon"} or not zone_geometry.is_valid or zone_geometry.is_empty:
                        raise InputValidationError("Invalid official zone geometry.")
                    cur.execute(
                        """INSERT INTO za_oficial (
                           id_uc, ds_fonte, geom, fl_ativa, numero_versao, dt_inicio_vigencia,
                           motivo, ator, correlation_id, import_id, dag_run_id, versao_dag)
                           VALUES (%s, %s, ST_Multi(ST_SetSRID(ST_GeomFromText(%s),4674)),
                                   TRUE, 1, COALESCE(%s::date, CURRENT_DATE), %s, %s, %s, %s, %s, %s)""",
                        (id_uc, zone["metadata"]["source"], zone_geometry.wkt,
                         zone["metadata"].get("valid_from"), reason, actor, correlation_id,
                         import_id, context.run_id, context.dag_id),
                    )
                    event_type = "UC_CREATED_WITH_OFFICIAL_ZA"
                cur.execute(
                    """
                    INSERT INTO uc_geometry_version (
                        id_uc, numero_versao, geom, fl_ativa, dt_inicio_vigencia,
                        motivo, fonte, ator, correlation_id
                    )
                    SELECT id_uc, versao_registro, geom, TRUE, CURRENT_DATE, %s, %s, %s, %s
                    FROM uc WHERE id_uc = %s;
                    """,
                    (reason, source, actor, correlation_id, id_uc),
                )
                new_state = {
                    "id_uc": id_uc,
                    "uc_id": current[2],
                    "cd_cnuc": current[3],
                    "wdpa_pid": current[4],
                    "name": current[5],
                    "version": version,
                    "import_id": import_id,
                    "dag_run_id": context.run_id,
                }
                cur.execute(
                    """
                    INSERT INTO cadastral_event (
                        event_id, entity_type, entity_id, event_type, actor, reason, source,
                        correlation_id, idempotency_key, previous_state, new_state
                    ) VALUES (%s, 'UC', %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        str(uuid4()), id_uc, event_type, actor, reason, source, correlation_id,
                        idempotency_key, Json(previous_state) if previous_state else None, Json(new_state),
                    ),
                )

        return self._directed_load_result(
            context=context,
            source_records=source_records,
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_no_uc_id,
            rejected_frames=rejected_frames,
            idempotent_replay=False,
        )

    def _load_directed_create_batch(
        self,
        *,
        context: TaskExecutionContext,
        state: dict[str, Any],
        transformed: gpd.GeoDataFrame,
        source_records: int,
        skipped_no_uc_id: int,
        rejected_frames: dict[str, pd.DataFrame],
        import_id: str,
        correlation_id: str,
        idempotency_key: str,
        actor: str,
        source: str,
        reason: str,
    ) -> dict[str, Any]:
        """Create all UCs from a directed batch in one PostGIS transaction."""
        geom_column = str(transformed.geometry.name)
        seen: dict[str, set[str]] = {"uc_id": set(), "cd_cnuc": set(), "wdpa_pid": set()}
        rows: list[tuple[pd.Series, str, str]] = []
        for index, row in transformed.iterrows():
            geometry = row.get(geom_column)
            if geometry is None or geometry.is_empty:
                self._write_api_error_result(context, state, "INVALID_OR_EMPTY_GEOMETRY", f"Feature {index} has no valid geometry.")
                raise InputValidationError("INVALID_OR_EMPTY_GEOMETRY in directed UC batch.")
            identities = {
                field: self._none_if_nan(row.get(field))
                for field in ("uc_id", "cd_cnuc", "wdpa_pid")
            }
            if not any(identities.values()):
                self._write_api_error_result(context, state, "MISSING_UC_IDENTITY", f"Feature {index} has no strong identity.")
                raise InputValidationError("MISSING_UC_IDENTITY in directed UC batch.")
            for field, value in identities.items():
                if value is None:
                    continue
                normalized = str(value).strip().casefold()
                if normalized in seen[field]:
                    self._write_api_error_result(context, state, "DUPLICATE_UC_IDENTITY_IN_BATCH", f"Feature {index} repeats {field}.")
                    raise InputValidationError("DUPLICATE_UC_IDENTITY_IN_BATCH in directed UC batch.")
                seen[field].add(normalized)
            primary_identity = next(str(value).strip() for value in identities.values() if value is not None)
            event_key = hashlib.sha256(
                f"{idempotency_key}|uc/create|{primary_identity}".encode("utf-8")
            ).hexdigest()
            rows.append((row, primary_identity, event_key))

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                event_keys = [event_key for _, _, event_key in rows]
                cur.execute(
                    "SELECT idempotency_key FROM cadastral_event WHERE idempotency_key = ANY(%s);",
                    (event_keys,),
                )
                applied_keys = {record[0] for record in cur.fetchall()}
                if applied_keys:
                    if len(applied_keys) != len(event_keys):
                        self._write_api_error_result(context, state, "PARTIAL_BATCH_REPLAY", "The batch has an inconsistent partial replay state.")
                        raise InputValidationError("PARTIAL_BATCH_REPLAY in directed UC batch.")
                    return self._directed_load_result(
                        context=context,
                        source_records=source_records,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_no_uc_id,
                        rejected_frames=rejected_frames,
                        idempotent_replay=True,
                    )

                duplicates = self._find_create_duplicates(transformed)
                if duplicates:
                    self._write_api_duplicate_result(context, state, duplicates)
                    raise InputValidationError("UC_ALREADY_EXISTS: directed UC batch contains existing UCs.")

                try:
                    for row, primary_identity, event_key in rows:
                        values = self._row_to_db_tuple(row, geom_column)
                        cur.execute(
                            """
                            INSERT INTO uc (
                                uc_id, cd_cnuc, wdpa_pid, nm_uc, dt_criacao, ds_ato_legal,
                                ds_grupo, ds_categoria, ds_esfera, nm_orgao_gestor, sg_uf,
                                area_total_ha, area_ato_ha, update_geom, geom, dt_bronze,
                                dt_silver, dt_gold, versao_dag, criado_por
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4674),
                                %s, %s, %s, %s, %s
                            ) RETURNING id_uc, versao_registro, uc_id, cd_cnuc, wdpa_pid, nm_uc;
                            """,
                            (*values, actor),
                        )
                        current = cur.fetchone()
                        id_uc = int(current[0])
                        version = int(current[1])
                        cur.execute(
                            """
                            INSERT INTO uc_geometry_version (
                                id_uc, numero_versao, geom, fl_ativa, dt_inicio_vigencia,
                                motivo, fonte, ator, correlation_id
                            ) SELECT id_uc, versao_registro, geom, TRUE, CURRENT_DATE, %s, %s, %s, %s
                            FROM uc WHERE id_uc = %s;
                            """,
                            (reason, source, actor, correlation_id, id_uc),
                        )
                        new_state = {
                            "id_uc": id_uc, "uc_id": current[2], "cd_cnuc": current[3],
                            "wdpa_pid": current[4], "name": current[5], "version": version,
                            "import_id": import_id, "dag_run_id": context.run_id,
                            "batch_identity": primary_identity,
                        }
                        cur.execute(
                            """
                            INSERT INTO cadastral_event (
                                event_id, entity_type, entity_id, event_type, actor, reason, source,
                                correlation_id, idempotency_key, previous_state, new_state
                            ) VALUES (%s, 'UC', %s, 'UC_CREATED', %s, %s, %s, %s, %s, NULL, %s);
                            """,
                            (str(uuid4()), id_uc, actor, reason, source, correlation_id, event_key, Json(new_state)),
                        )
                except psycopg2.errors.UniqueViolation as exc:
                    conn.rollback()
                    duplicates = self._find_create_duplicates(transformed)
                    self._write_api_duplicate_result(context, state, duplicates)
                    raise InputValidationError("UC_ALREADY_EXISTS: concurrent directed batch create.") from exc

        return self._directed_load_result(
            context=context,
            source_records=source_records,
            inserted_new_records=len(rows),
            inserted_updated_records=0,
            skipped_records=skipped_no_uc_id,
            rejected_frames=rejected_frames,
            idempotent_replay=False,
        )

    def _directed_load_result(
        self,
        *,
        context: TaskExecutionContext,
        source_records: int,
        inserted_new_records: int,
        inserted_updated_records: int,
        skipped_records: int,
        rejected_frames: dict[str, pd.DataFrame],
        idempotent_replay: bool,
    ) -> dict[str, Any]:
        inserted_records = inserted_new_records + inserted_updated_records
        self._write_quality_report(
            context=context,
            stage="load_postgres",
            source_records=source_records,
            inserted_records=inserted_records,
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
            result_reason="loaded_postgres",
            rejected_frames=rejected_frames,
        )
        return self._result(
            context,
            status="loaded_postgres",
            inserted_records=inserted_records,
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
            idempotent_replay=idempotent_replay,
        )

    def load_silver(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Publish transformed UCS records to SILVER using canonical temporal path."""
        state = self._load_state(context)
        transformed = self._read_geodata(Path(state["transformed_path"]), layer="uc")

        silver_dir = self._build_layer_output_dir(
            layer_root=Path(self.config.medallion_silver_path),
            temporal_folder=state["temporal_folder"],
        )

        silver_path = silver_dir / "uc_silver.gpkg"
        transformed.to_file(silver_path, layer="uc", driver="GPKG")

        dt_silver = datetime.now(timezone.utc).isoformat()
        metadata = {
            "domain": self.domain_name,
            "run_id": context.run_id,
            "dt_bronze": state["dt_bronze"],
            "dt_silver": dt_silver,
            "records": int(len(transformed)),
            "source": state["shapefile_path"],
        }
        (silver_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        state["silver_path"] = str(silver_path)
        state["dt_silver"] = dt_silver
        self._save_state(context, state)
        self._update_uc_layer_timestamp(
            dt_bronze=state["dt_bronze"],
            dag_version=context.dag_id,
            column_name="dt_silver",
            column_value=dt_silver,
        )

        self.logger.info("UCS load_silver completed. path=%s", silver_path)
        return self._result(
            context,
            status="loaded_silver",
            silver_path=str(silver_path),
            dt_silver=dt_silver,
        )

    def load_gold(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Publish analytics-ready UCS outputs to GOLD layer."""
        state = self._load_state(context)
        transformed = self._read_geodata(Path(state["transformed_path"]), layer="uc")

        gold_dir = self._build_layer_output_dir(
            layer_root=Path(self.config.medallion_gold_path),
            temporal_folder=state["temporal_folder"],
        )

        gold_geojson = gold_dir / "uc_gold.geojson"
        transformed.to_file(gold_geojson, driver="GeoJSON")

        attributes = transformed.drop(columns=[transformed.geometry.name]).copy()
        attributes.to_csv(gold_dir / "uc_gold_attributes.csv", index=False, encoding="utf-8")

        dt_gold = datetime.now(timezone.utc).isoformat()
        metadata = {
            "domain": self.domain_name,
            "run_id": context.run_id,
            "dt_bronze": state["dt_bronze"],
            "dt_silver": state.get("dt_silver"),
            "dt_gold": dt_gold,
            "records": int(len(transformed)),
        }
        (gold_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        state["dt_gold"] = dt_gold
        self._save_state(context, state)
        self._update_uc_layer_timestamp(
            dt_bronze=state["dt_bronze"],
            dag_version=context.dag_id,
            column_name="dt_gold",
            column_value=dt_gold,
        )

        self.logger.info("UCS load_gold completed. path=%s", gold_geojson)
        return self._result(
            context,
            status="loaded_gold",
            gold_geojson=str(gold_geojson),
            dt_gold=dt_gold,
        )

    def _run_dir(self, context: TaskExecutionContext) -> Path:
        safe_run_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", context.run_id)
        return Path(self.config.medallion_tmp_path) / self.domain_name / safe_run_id

    def _quality_stage_dir(self, context: TaskExecutionContext, stage: str) -> Path:
        safe_run_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", context.run_id)
        quality_root = Path(self.config.medallion_tmp_path).resolve().parent / QUALITY_FOLDER_NAME
        target = (
            quality_root
            / "rejections"
            / self.domain_name
            / f"run_id={safe_run_id}"
            / "branch=ucs"
            / f"stage={stage}"
        )
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _find_create_duplicates(self, gdf: gpd.GeoDataFrame) -> list[dict[str, Any]]:
        if not self.config.project_db_url:
            raise UcsPipelineError("PROJECT_DB_URL is required for duplicate verification.")
        normalized = self._ensure_epsg_4674(gdf)
        uc_ids = self._identity_values(normalized, ["uc_id", "id_uc", "id", "gid"])
        cnuc_codes = self._identity_values(normalized, ["cd_cnuc", "cod_cnuc", "cnuc"])
        wdpa_pids = self._identity_values(normalized, ["wdpa_pid", "wdpaid", "wdpa"])
        geometry_values = [
            geometry.wkt
            for geometry in normalized.geometry
            if geometry is not None and not geometry.is_empty
        ] or [None]
        matches: dict[int, dict[str, Any]] = {}
        query = """
            SELECT id_uc, uc_id, cd_cnuc, wdpa_pid, nm_uc,
                   CASE
                       WHEN uc_id::text = ANY(%s::text[]) THEN 'uc_id'
                       WHEN cd_cnuc::text = ANY(%s::text[]) THEN 'cd_cnuc'
                       WHEN wdpa_pid::text = ANY(%s::text[]) THEN 'wdpa_pid'
                       ELSE 'geometry'
                   END AS matched_by
            FROM uc
            WHERE uc_id::text = ANY(%s::text[])
               OR cd_cnuc::text = ANY(%s::text[])
               OR wdpa_pid::text = ANY(%s::text[])
               OR (%s::text IS NOT NULL AND ST_Equals(geom, ST_GeomFromText(%s, 4674)));
        """
        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                for geometry_wkt in geometry_values:
                    cur.execute(
                        query,
                        (
                            uc_ids,
                            cnuc_codes,
                            wdpa_pids,
                            uc_ids,
                            cnuc_codes,
                            wdpa_pids,
                            geometry_wkt,
                            geometry_wkt,
                        ),
                    )
                    for row in cur.fetchall():
                        matches[int(row[0])] = {
                            "uc_id": row[0],
                            "official_identifier": row[1],
                            "cd_cnuc": row[2],
                            "wdpa_pid": row[3],
                            "name": row[4],
                            "matched_by": row[5],
                        }
        return list(matches.values())

    def _identity_values(self, gdf: gpd.GeoDataFrame, candidates: list[str]) -> list[str]:
        source = self._get_source_value(gdf, candidates)
        if source is None:
            return []
        values: set[str] = set()
        for value in source:
            clean = self._none_if_nan(value)
            if clean is not None and str(clean).strip():
                values.add(str(clean).strip())
        return sorted(values)

    def _write_api_duplicate_result(
        self,
        context: TaskExecutionContext,
        state: dict[str, Any],
        duplicate_matches: list[dict[str, Any]],
    ) -> None:
        import_id = state.get("import_id")
        if not import_id:
            return
        target = (
            Path(self.config.medallion_bronze_path).resolve().parent
            / QUALITY_FOLDER_NAME
            / "api_results"
            / f"import_id={import_id}"
        )
        target.mkdir(parents=True, exist_ok=True)
        payload = {
            "import_id": import_id,
            "dag_id": context.dag_id,
            "dag_run_id": context.run_id,
            "status": "DUPLICATE",
            "error_code": "UC_ALREADY_EXISTS",
            "error_detail": (
                "A UC não foi cadastrada porque já existe registro correspondente no PostGIS."
            ),
            "duplicate_matches": duplicate_matches,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        temporary = target / ".result.json.tmp"
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(target / "result.json")
        push_quality(self.config)

    def _write_api_error_result(
        self,
        context: TaskExecutionContext,
        state: dict[str, Any],
        error_code: str,
        error_detail: str,
    ) -> None:
        import_id = state.get("import_id")
        if not import_id:
            return
        target = (
            Path(self.config.medallion_bronze_path).resolve().parent
            / QUALITY_FOLDER_NAME
            / "api_results"
            / f"import_id={import_id}"
        )
        target.mkdir(parents=True, exist_ok=True)
        payload = {
            "import_id": import_id,
            "dag_id": context.dag_id,
            "dag_run_id": context.run_id,
            "status": "FAILED",
            "error_code": error_code,
            "error_detail": error_detail,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        temporary = target / ".result.json.tmp"
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(target / "result.json")
        push_quality(self.config)

    def _write_quality_report(
        self,
        context: TaskExecutionContext,
        stage: str,
        source_records: int,
        inserted_records: int,
        inserted_new_records: int,
        inserted_updated_records: int,
        skipped_records: int,
        result_reason: str,
        rejected_frames: dict[str, pd.DataFrame],
    ) -> None:
        target = self._quality_stage_dir(context, stage=stage)
        generated_at = datetime.now(timezone.utc).isoformat()

        rejection_counts = {
            reason: int(len(frame))
            for reason, frame in rejected_frames.items()
            if frame is not None and not frame.empty
        }
        rejection_reason_details = self._build_rejection_reason_details(rejection_counts)

        summary = {
            "domain": self.domain_name,
            "branch": "ucs",
            "stage": stage,
            "dag_id": context.dag_id,
            "run_id": context.run_id,
            "logical_date": context.logical_date,
            "generated_at_utc": generated_at,
            "source_records": int(source_records),
            "inserted_records": int(inserted_records),
            "inserted_new_records": int(inserted_new_records),
            "inserted_updated_records": int(inserted_updated_records),
            "skipped_records": int(skipped_records),
            "result_reason": result_reason,
            "result_reason_description": self._describe_result_reason(result_reason),
            "rejection_counts": rejection_counts,
            "rejection_reason_details": rejection_reason_details,
        }
        (target / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

        rejected_combined = self._combine_rejections(rejected_frames)
        if rejected_combined is None or rejected_combined.empty:
            return

        geometry_column = self._detect_geometry_column(rejected_combined)
        rejected_csv = rejected_combined.copy()
        if geometry_column is not None and geometry_column in rejected_csv.columns:
            rejected_csv["geometry_wkt"] = rejected_csv[geometry_column].apply(
                lambda geom: geom.wkt if geom is not None and not pd.isna(geom) else None
            )
            rejected_csv = rejected_csv.drop(columns=[geometry_column])

        pd.DataFrame(rejected_csv).to_csv(target / "rejected_rows.csv", index=False, encoding="utf-8")

        if geometry_column is None:
            return

        geo_rejected = gpd.GeoDataFrame(rejected_combined, geometry=geometry_column, crs=getattr(rejected_combined, "crs", None))
        geo_rejected = geo_rejected[~geo_rejected[geometry_column].isna()].copy()
        if geo_rejected.empty:
            return

        geo_rejected = geo_rejected[
            ~gpd.GeoSeries(geo_rejected[geometry_column], crs=geo_rejected.crs).is_empty
        ].copy()
        if geo_rejected.empty:
            return

        for column in geo_rejected.columns:
            if column == geometry_column:
                continue
            if pd.api.types.is_datetime64_any_dtype(geo_rejected[column]):
                geo_rejected[column] = geo_rejected[column].dt.strftime("%Y-%m-%dT%H:%M:%S")
            elif geo_rejected[column].dtype == "object":
                geo_rejected[column] = geo_rejected[column].map(
                    lambda value: value.isoformat() if isinstance(value, (date, datetime)) else value
                )
        geo_rejected.to_file(target / "rejected_rows.geojson", driver="GeoJSON")

    def _combine_rejections(self, rejected_frames: dict[str, pd.DataFrame]) -> pd.DataFrame | gpd.GeoDataFrame | None:
        parts: list[pd.DataFrame] = []
        first_crs = None
        geometry_column = None

        for reason, frame in rejected_frames.items():
            if frame is None or frame.empty:
                continue
            part = frame.copy()
            part["reason_code"] = reason
            parts.append(part)

            if first_crs is None and isinstance(frame, gpd.GeoDataFrame):
                first_crs = frame.crs
            if geometry_column is None:
                geometry_column = self._detect_geometry_column(part)

        if not parts:
            return None

        combined = pd.concat(parts, axis=0, ignore_index=True)
        if geometry_column is not None and geometry_column in combined.columns:
            return gpd.GeoDataFrame(combined, geometry=geometry_column, crs=first_crs)
        return combined

    def _detect_geometry_column(self, frame: pd.DataFrame | gpd.GeoDataFrame) -> str | None:
        if isinstance(frame, gpd.GeoDataFrame):
            return str(frame.geometry.name)
        for candidate in ("geometry", "geom"):
            if candidate in frame.columns:
                return candidate
        return None

    def _describe_result_reason(self, reason_code: str) -> str:
        return RESULT_REASON_DESCRIPTIONS.get(reason_code, "Motivo operacional nao catalogado.")

    def _describe_rejection_reason(self, reason_code: str) -> str:
        return REJECTION_REASON_DESCRIPTIONS.get(reason_code, "Motivo de rejeicao nao catalogado.")

    def _build_rejection_reason_details(self, rejection_counts: dict[str, int]) -> list[dict[str, Any]]:
        return [
            {
                "reason_code": reason_code,
                "reason_description": self._describe_rejection_reason(reason_code),
                "count": int(count),
            }
            for reason_code, count in rejection_counts.items()
        ]

    def _state_path(self, context: TaskExecutionContext) -> Path:
        return self._run_dir(context) / "state.json"

    def _save_state(self, context: TaskExecutionContext, state: dict[str, Any]) -> None:
        path = self._state_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _load_state(self, context: TaskExecutionContext) -> dict[str, Any]:
        path = self._state_path(context)
        if not path.exists():
            raise UcsPipelineError(f"Missing pipeline state for stage '{context.stage}': {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _select_bronze_batch(self, source_root: Path) -> BronzeSelection:
        selections = self._list_bronze_batches(source_root)
        if selections:
            return selections[-1]

        domain_root = source_root / self.domain_name

        # Compatibility mode for current MVP datasets placed directly under bronze/ucs.
        # dt_bronze falls back to latest source file modification date.
        fallback_root = domain_root if domain_root.exists() and domain_root.is_dir() else source_root
        files = [p for p in fallback_root.iterdir() if p.is_file() and p.suffix.lower() in {".zip", ".shp", ".shx", ".dbf", ".prj"}]
        if not files:
            raise InputValidationError(
                f"No candidate UCS files found in bronze source path: {fallback_root}"
            )

        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        dt_bronze = datetime.fromtimestamp(latest_file.stat().st_mtime, tz=timezone.utc).date().isoformat()
        self.logger.warning(
            "Bronze batch is not in canonical timestamp folder format (%s). "
            "Compatibility mode enabled with dt_bronze inferred from file mtime=%s",
            TEMPORAL_FOLDER_FORMAT,
            dt_bronze,
        )
        return BronzeSelection(batch_dir=fallback_root, dt_bronze=dt_bronze)

    def _list_bronze_batches(self, source_root: Path) -> list[BronzeSelection]:
        """Return every canonical UC Bronze batch in deterministic temporal order."""
        dated_candidates: list[tuple[datetime, int, Path]] = []

        domain_root = source_root / self.domain_name

        # Canonical model: bronze/ucs/yyyy-mm-dd-hh-mm-ss
        if domain_root.exists() and domain_root.is_dir():
            for child in domain_root.iterdir():
                if not child.is_dir():
                    continue
                parsed = self._try_parse_temporal_folder(child.name)
                if parsed is not None:
                    dated_candidates.append((parsed, 1, child))

        # Legacy compatibility model: bronze/yyyy-mm-dd-hh-mm-ss/ucs
        for child in source_root.iterdir():
            if not child.is_dir():
                continue
            parsed = self._try_parse_temporal_folder(child.name)
            if parsed is not None:
                domain_dir = child / self.domain_name
                if domain_dir.exists() and domain_dir.is_dir():
                    self.logger.warning(
                        "Legacy bronze layout detected at '%s'. Canonical layout is bronze/%s/<timestamp>",
                        domain_dir,
                        self.domain_name,
                    )
                    dated_candidates.append((parsed, 0, domain_dir))

        dated_candidates.sort(key=lambda item: (item[0], item[1]))
        return [
            BronzeSelection(batch_dir=batch_dir, dt_bronze=batch_dt.date().isoformat())
            for batch_dt, _, batch_dir in dated_candidates
        ]

    def _materialize_bronze_union(self, selections: list[BronzeSelection], run_dir: Path) -> Path:
        """Build an auditable union of canonical UC Bronze batches for a full refresh."""
        frames: list[gpd.GeoDataFrame] = []
        for selection in selections:
            source_path, _ = self._locate_shapefile(selection.batch_dir, run_dir / selection.batch_dir.name)
            frame = self._ensure_epsg_4674(self._read_geodata(source_path))
            frame["_bronze_dt"] = pd.to_datetime(selection.dt_bronze)
            frames.append(frame)
        if not frames:
            raise InputValidationError("No canonical UCS Bronze batches are available for the full refresh.")
        union = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs="EPSG:4674")
        union_path = run_dir / "ucs_bronze_union.gpkg"
        union.to_file(union_path, layer="ucs_bronze_union", driver="GPKG")
        return union_path

    def _select_bronze_batch_from_context(
        self,
        source_root: Path,
        conf: dict[str, Any],
    ) -> BronzeSelection:
        """Select an explicit Bronze batch for an audited backfill when requested."""
        requested = str(conf.get("bronze_batch", "")).strip()
        if not requested:
            return self._select_bronze_batch(source_root)
        bronze_root = source_root.resolve()
        candidate = (bronze_root / requested).resolve()
        try:
            candidate.relative_to(bronze_root)
        except ValueError as exc:
            raise InputValidationError("bronze_batch must remain under the configured Bronze root.") from exc
        if not candidate.is_dir():
            raise InputValidationError(f"Requested Bronze batch is unavailable: {requested}")
        parsed = self._try_parse_temporal_folder(candidate.name)
        if parsed is None:
            raise InputValidationError(
                f"Requested Bronze batch does not use the required timestamp format: {requested}"
            )
        return BronzeSelection(batch_dir=candidate, dt_bronze=parsed.date().isoformat())

    def _try_parse_temporal_folder(self, folder_name: str) -> datetime | None:
        for fmt in (TEMPORAL_FOLDER_FORMAT, LEGACY_TEMPORAL_FOLDER_FORMAT):
            try:
                parsed = datetime.strptime(folder_name, fmt)
                if fmt == LEGACY_TEMPORAL_FOLDER_FORMAT:
                    self.logger.warning(
                        "Legacy bronze timestamp folder detected (%s). "
                        "Canonical format is %s",
                        folder_name,
                        TEMPORAL_FOLDER_FORMAT,
                    )
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def _temporal_folder_stamp(self, context: TaskExecutionContext) -> str:
        logical = datetime.fromisoformat(context.logical_date.replace("Z", "+00:00"))
        return logical.astimezone(timezone.utc).strftime(TEMPORAL_FOLDER_FORMAT)

    def _build_layer_output_dir(self, layer_root: Path, temporal_folder: str) -> Path:
        """Create output directory only when temporal folder matches canonical format.

        Canonical format: yyyy-MM-dd-HH-mm-ss.
        """
        if self._try_parse_temporal_folder(temporal_folder) is None:
            raise UcsPipelineError(
                "Invalid temporal folder for medallion publish: "
                f"'{temporal_folder}'. Expected format is {TEMPORAL_FOLDER_FORMAT}."
            )

        target = layer_root / self.domain_name / temporal_folder
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _locate_shapefile(self, batch_dir: Path, run_dir: Path) -> tuple[Path, str]:
        """Locate a supported Bronze UC vector dataset without converting its source format."""
        shapefile = self._find_shapefile_in_directory(batch_dir)
        if shapefile is not None:
            self._validate_shapefile_components(shapefile)
            return shapefile, "shapefile"

        geojson_files = sorted(
            [*batch_dir.rglob("*.geojson"), *batch_dir.rglob("*.json")],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if geojson_files:
            return geojson_files[0], "geojson"

        zip_files = sorted(batch_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not zip_files:
            raise InputValidationError(
                f"No UCS shapefile, GeoJSON or zip package found under bronze batch: {batch_dir}"
            )

        extraction_dir = run_dir / "unzipped"
        extraction_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_files[0]) as zf:
            zf.extractall(extraction_dir)

        shapefile = self._find_shapefile_in_directory(extraction_dir)
        if shapefile is None:
            raise InputValidationError(
                f"Zip extracted but no .shp file found: {zip_files[0]}"
            )

        self._validate_shapefile_components(shapefile)
        return shapefile, "zip"

    def _find_shapefile_in_directory(self, directory: Path) -> Path | None:
        shapefiles = list(directory.rglob("*.shp"))
        if not shapefiles:
            return None
        return sorted(shapefiles, key=lambda p: p.stat().st_mtime, reverse=True)[0]

    def _read_geodata(self, path: Path, layer: str | None = None) -> gpd.GeoDataFrame:
        """Read vector data through Fiona directly to avoid geopandas/fiona path incompatibility."""
        open_kwargs: dict[str, Any] = {}
        if layer is not None:
            open_kwargs["layer"] = layer

        with fiona.open(str(path), **open_kwargs) as src:
            features = list(src)
            if not features:
                return gpd.GeoDataFrame(geometry=[], crs=src.crs)
            return gpd.GeoDataFrame.from_features(features, crs=src.crs)

    def _validate_shapefile_components(self, shp_path: Path) -> None:
        missing = []
        for ext in REQUIRED_SHAPEFILE_EXTENSIONS:
            candidate = shp_path.with_suffix(ext)
            if not candidate.exists():
                missing.append(candidate.name)

        if missing:
            raise InputValidationError(
                f"Shapefile is missing mandatory sidecars for '{shp_path.name}': {', '.join(missing)}"
            )

    def _ensure_epsg_4674(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        source_epsg = gdf.crs.to_epsg() if gdf.crs else None
        if source_epsg != 4674:
            self.logger.info("Reprojecting UCS data from EPSG:%s to EPSG:4674", source_epsg)
            return gdf.to_crs(epsg=4674)
        return gdf

    def _repair_geometries(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        repaired = gdf.copy()

        repaired.geometry = repaired.geometry.apply(self._make_valid_geometry)
        repaired.geometry = repaired.geometry.apply(self._normalize_uc_geometry)

        return repaired

    def _make_valid_geometry(self, geom):
        if geom is None:
            return None
        if geom.is_valid:
            return geom
        try:
            return geom.make_valid()
        except Exception:
            # Conservative fallback for polygonal invalidity in Shapely compatibility scenarios.
            return geom.buffer(0)

    def _normalize_uc_geometry(self, geom):
        if geom is None:
            return None
        if isinstance(geom, Polygon):
            return MultiPolygon([geom])
        return geom

    def _apply_manifest_metadata(
        self, gdf: gpd.GeoDataFrame, state: dict[str, Any]
    ) -> gpd.GeoDataFrame:
        manifest = state.get("manifest")
        if not manifest:
            return gdf
        enriched = gdf.copy()
        metadata = manifest.get("metadata", {})
        if self._detect_source_column(
            enriched.columns, ["nm_uc", "nome_uc", "nome", "nm_unid_con", "name"]
        ) is None:
            enriched["nm_uc"] = metadata.get("name")
        if self._detect_source_column(enriched.columns, ["uc_id", "id_uc", "id", "gid"]) is None:
            enriched["uc_id"] = metadata.get("official_identifier") or manifest.get("import_id")
        if self._detect_source_column(enriched.columns, ["cd_cnuc", "cod_cnuc", "cnuc"]) is None:
            enriched["cd_cnuc"] = metadata.get("cd_cnuc")
        if self._detect_source_column(enriched.columns, ["dt_criacao", "data_criacao"]) is None:
            enriched["dt_criacao"] = metadata.get("legal_act_date")
        operation = manifest.get("operation")
        if operation in {"create", "create_with_zone"}:
            enriched["update_geom"] = False
        elif operation in {"update", "replace_point"}:
            # Em lotes dirigidos, a operação vem do manifesto imutável; nunca se confia
            # em um campo update_geom fornecido dentro do arquivo.
            enriched["update_geom"] = True
            target_uc_id = metadata.get("official_identifier")
            if target_uc_id:
                enriched["uc_id"] = target_uc_id
        return enriched

    def _map_to_uc_schema(
        self,
        gdf: gpd.GeoDataFrame,
        dt_bronze: str,
        dag_version: str,
    ) -> gpd.GeoDataFrame:
        src = gdf.copy()

        out = gpd.GeoDataFrame(geometry=src.geometry, crs=src.crs)

        out["uc_id"] = self._get_source_value(src, ["uc_id", "id_uc", "id", "gid"])
        out["cd_cnuc"] = self._get_source_value(src, ["cd_cnuc", "cod_cnuc", "cnuc"])
        out["wdpa_pid"] = self._get_source_value(src, ["wdpa_pid", "wdpaid", "wdpa"])

        nm_uc = self._get_source_value(src, ["nm_uc", "nome_uc", "nm_unid_con", "nome", "name"])
        if nm_uc is None:
            raise InputValidationError("Unable to map UCS name column to nm_uc.")
        out["nm_uc"] = nm_uc.astype(str)

        dt_criacao = self._resolve_dt_criacao(src)
        if dt_criacao is not None:
            out["dt_criacao"] = pd.to_datetime(dt_criacao, errors="coerce").dt.strftime("%Y-%m-%d")
        else:
            out["dt_criacao"] = None
        out["ds_ato_legal"] = self._compose_ato_legal(src)
        out["ds_grupo"] = self._get_source_value(src, ["ds_grupo", "grupo"])
        out["ds_categoria"] = self._get_source_value(src, ["ds_categoria", "categoria", "cat_manejo"])
        out["ds_esfera"] = self._get_source_value(src, ["ds_esfera", "esfera"])
        out["nm_orgao_gestor"] = self._get_source_value(
            src,
            ["nm_orgao_gestor", "org_gestor", "orgao_gest", "gestor", "orgao"],
        )

        sg_uf = self._get_source_value(src, ["sg_uf", "uf"])
        if sg_uf is not None:
            out["sg_uf"] = sg_uf.apply(self._normalize_uf_value).fillna("SC")
        else:
            out["sg_uf"] = "SC"

        out["area_total_ha"] = self._to_numeric_series(
            self._get_source_value(src, ["area_total_ha", "ha_total", "area_ha", "area_total"])
        )
        out["area_ato_ha"] = self._to_numeric_series(
            self._get_source_value(src, ["area_ato_ha", "ha_ato", "area_ato"])
        )

        update_geom_source = self._get_source_value(src, ["update_geom", "update_geo", "updategeometry"])
        out["update_geom"] = (
            update_geom_source.apply(self._normalize_update_geom_text)
            if update_geom_source is not None
            else None
        )

        out["geom"] = out.geometry
        out["dt_bronze"] = pd.to_datetime(src["_bronze_dt"], errors="coerce") if "_bronze_dt" in src else pd.to_datetime(dt_bronze)
        out["dt_silver"] = None
        out["dt_gold"] = None
        out["versao_dag"] = dag_version

        for column in UC_DB_COLUMNS:
            if column not in out.columns:
                out[column] = None

        out = out[UC_DB_COLUMNS]
        return gpd.GeoDataFrame(out, geometry="geom", crs=src.crs)

    def _row_to_db_tuple(self, row: pd.Series, geometry_column: str) -> tuple[Any, ...]:
        geom_value = row.get(geometry_column)
        sg_uf = self._normalize_uf_value(row.get("sg_uf")) or "SC"
        update_geom = self._normalize_update_geom_text(row.get("update_geom"))
        return (
            self._none_if_nan(row.get("uc_id")),
            self._none_if_nan(row.get("cd_cnuc")),
            self._none_if_nan(row.get("wdpa_pid")),
            self._none_if_nan(row.get("nm_uc")),
            self._none_if_nan(row.get("dt_criacao")),
            self._none_if_nan(row.get("ds_ato_legal")),
            self._none_if_nan(row.get("ds_grupo")),
            self._none_if_nan(row.get("ds_categoria")),
            self._none_if_nan(row.get("ds_esfera")),
            self._none_if_nan(row.get("nm_orgao_gestor")),
            sg_uf,
            self._none_if_nan(row.get("area_total_ha")),
            self._none_if_nan(row.get("area_ato_ha")),
            update_geom,
            geom_value.wkt if geom_value is not None else None,
            self._none_if_nan(row.get("dt_bronze")),
            self._none_if_nan(row.get("dt_silver")),
            self._none_if_nan(row.get("dt_gold")),
            self._none_if_nan(row.get("versao_dag")),
        )

    def _update_uc_layer_timestamp(
        self,
        dt_bronze: str,
        dag_version: str,
        column_name: str,
        column_value: str,
    ) -> None:
        if column_name not in {"dt_silver", "dt_gold"}:
            raise UcsPipelineError(f"Unsupported layer timestamp column: {column_name}")
        if not self.config.project_db_url:
            raise UcsPipelineError("PROJECT_DB_URL is required for layer timestamp updates.")

        sql = f"""
            UPDATE uc
            SET {column_name} = %s
            WHERE dt_bronze = %s AND versao_dag = %s;
        """

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (column_value, dt_bronze, dag_version))

    def _get_source_value(self, gdf: gpd.GeoDataFrame, candidates: list[str]) -> pd.Series | None:
        source_col = self._detect_source_column(gdf.columns, candidates)
        if source_col is None:
            return None
        return gdf[source_col]

    def _detect_source_column(self, columns, candidates: list[str]) -> str | None:
        normalized = {self._normalize_name(col): col for col in columns}
        for candidate in candidates:
            found = normalized.get(self._normalize_name(candidate))
            if found is not None:
                return found
        return None

    def _normalize_name(self, name: str) -> str:
        return re.sub(r"[^a-z0-9]", "", name.lower())

    def _to_date_series(self, value: pd.Series | None) -> pd.Series | None:
        if value is None:
            return None
        text = value.astype(str).str.strip()
        text = text.where(~text.isin(["", "nan", "None"]), None)

        # First pass: strict Brazilian format (dd-mm-yyyy / dd/mm/yyyy).
        parsed_br = pd.to_datetime(text, errors="coerce", dayfirst=True, format="%d-%m-%Y")
        missing = parsed_br.isna()
        if missing.any():
            parsed_br.loc[missing] = pd.to_datetime(
                text.loc[missing].str.replace("/", "-", regex=False),
                errors="coerce",
                dayfirst=True,
                format="%d-%m-%Y",
            )

        # Second pass fallback for any other ISO-like representation.
        still_missing = parsed_br.isna()
        if still_missing.any():
            parsed_br.loc[still_missing] = pd.to_datetime(
                text.loc[still_missing],
                errors="coerce",
                dayfirst=True,
            )

        return parsed_br.dt.date

    def _resolve_dt_criacao(self, gdf: gpd.GeoDataFrame) -> pd.Series | None:
        """Resolve creation date prioritizing cria_ano and fallback extraction from ato text."""
        cria_ano = self._get_source_value(gdf, ["cria_ano", "dt_criacao", "data_criaca", "data_criac", "dt_criacao_ano"])
        cria_ato = self._get_source_value(gdf, ["cria_ato", "ds_ato_legal", "ato_legal", "instrumento"])
        if cria_ano is None and cria_ato is None:
            return None

        resolved = pd.Series([None] * len(gdf), index=gdf.index)
        for idx in gdf.index:
            candidates: list[Any] = []
            if cria_ano is not None:
                candidates.append(cria_ano.get(idx))
            if cria_ato is not None:
                candidates.append(cria_ato.get(idx))

            parsed_value = None
            for candidate in candidates:
                parsed_value = self._parse_br_date_value(candidate)
                if parsed_value is not None:
                    break

            resolved.at[idx] = parsed_value

        return resolved

    def _parse_br_date_value(self, value: Any):
        clean = self._none_if_nan(value)
        if clean is None:
            return None

        text = str(clean).strip()
        if not text:
            return None

        date_token_match = re.search(r"(\d{2}[/-]\d{2}[/-]\d{4})", text)
        date_token = date_token_match.group(1) if date_token_match else text
        normalized = date_token.replace("/", "-")

        try:
            return datetime.strptime(normalized, "%d-%m-%Y").date()
        except ValueError:
            try:
                return datetime.strptime(normalized, "%Y-%m-%d").date()
            except ValueError:
                return None

    def _to_numeric_series(self, value: pd.Series | None) -> pd.Series | None:
        if value is None:
            return None
        cleaned = value.astype(str).str.strip()
        cleaned = cleaned.str.replace(r"\.(?=\d{3}(\D|$))", "", regex=True)
        cleaned = cleaned.str.replace(",", ".", regex=False)
        cleaned = cleaned.where(~cleaned.isin(["", "nan", "None"]), None)
        return pd.to_numeric(cleaned, errors="coerce")

    def _none_if_nan(self, value: Any) -> Any:
        if value is None:
            return None
        if pd.isna(value):
            return None
        # psycopg2 não adapta escalares NumPy (por exemplo numpy.int64) diretamente.
        # Converta-os para o escalar nativo sem alterar strings, datas ou geometrias.
        if type(value).__module__.startswith("numpy") and hasattr(value, "item"):
            return value.item()
        return value

    def _normalize_uf_value(self, value: Any) -> str | None:
        """Normalize UF values to a valid 2-letter Brazilian code."""
        clean = self._none_if_nan(value)
        if clean is None:
            return None

        text = str(clean).strip().upper()
        if not text:
            return None

        if len(text) == 2 and text in UF_CODES:
            return text

        token_match = re.search(r"\b([A-Z]{2})\b", text)
        if token_match:
            token = token_match.group(1)
            if token in UF_CODES:
                return token

        normalized = unicodedata.normalize("NFKD", text)
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = re.sub(r"[^A-Za-z]", "", normalized).lower()
        return UF_NAME_TO_CODE.get(normalized)

    def _compose_ato_legal(self, gdf: gpd.GeoDataFrame) -> pd.Series | None:
        primary = self._get_source_value(gdf, ["ds_ato_legal", "cria_ato", "ato_legal", "instrumento"])
        secondary = self._get_source_value(gdf, ["outro_ato"])

        if primary is None and secondary is None:
            return None

        if primary is None:
            primary = pd.Series([None] * len(gdf), index=gdf.index)
        if secondary is None:
            secondary = pd.Series([None] * len(gdf), index=gdf.index)

        def _clean_text(raw: Any) -> str | None:
            val = self._none_if_nan(raw)
            if val is None:
                return None
            txt = str(val).strip()
            if not txt:
                return None
            if txt.lower() in {"sem informacao", "sem informação", "nao informado", "não informado"}:
                return None
            return txt

        merged: list[str | None] = []
        for idx in gdf.index:
            a = _clean_text(primary.get(idx))
            b = _clean_text(secondary.get(idx))
            if a and b and a != b:
                merged.append(f"{a} | {b}")
            else:
                merged.append(a or b)

        return pd.Series(merged, index=gdf.index)

    def _normalize_update_geom_text(self, value: Any) -> str | None:
        clean = self._none_if_nan(value)
        if clean is None:
            return None

        text = str(clean).strip()
        if not text:
            return None

        lowered = text.lower()
        if lowered in {"true", "t", "1", "sim", "s", "yes", "y"}:
            return "TRUE"
        if lowered in {"false", "f", "0", "nao", "não", "n", "no"}:
            return "FALSE"

        return text.upper()

    def _is_truthy_update_geom(self, value: Any) -> bool:
        normalized = self._normalize_update_geom_text(value)
        return normalized == "TRUE"

    def _normalize_uc_id(self, value: Any) -> str | None:
        clean = self._none_if_nan(value)
        if clean is None:
            return None
        text = str(clean).strip()
        return text or None
