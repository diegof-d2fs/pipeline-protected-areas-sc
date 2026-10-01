"""Pipeline service for parallel ZA and Buffer de Abrangência processing branches."""

from __future__ import annotations

import json
import logging
import re
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
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import DomainPipelineService, TaskExecutionContext
from scripts_python.manifest_input import load_manifest_input
from scripts_python.object_storage import push_quality

LOGGER = logging.getLogger("pipeline.za_buffer")

TEMPORAL_FOLDER_FORMAT = "%Y-%m-%d-%H-%M-%S"
LEGACY_TEMPORAL_FOLDER_FORMAT = "%Y-%m-%d-%S-%M-%H"
REQUIRED_SHAPEFILE_EXTENSIONS = (".shp", ".shx", ".dbf")
QUALITY_FOLDER_NAME = "quality"

RESULT_REASON_DESCRIPTIONS = {
    "loaded_postgres": "Carga no Postgres concluida com sucesso.",
    "no_transformed_input": "Nao ha dataset transformado disponivel para carga.",
    "no_uc_fk_match": "Nenhum registro conseguiu vincular com UC (id_uc).",
    "uc_has_official_za": "Registros de Buffer de Abrangência foram descartados por ja existir ZA oficial ativa na UC.",
    "no_eligible_rows_after_merge_rules": "Registros foram rejeitados pelas regras de merge/update_geom.",
    "no_valid_geometry_for_insert": "Registros elegiveis foram rejeitados por geometria nula ou vazia.",
    "active_buffer_unchanged": "Todos os Buffers de Abrangência ativos ja correspondem a geometria vigente da UC.",
}

REJECTION_REASON_DESCRIPTIONS = {
    "NO_UC_FK_MATCH": "Registro sem correspondencia de UC (chave e intersecao espacial).",
    "NO_TRUSTED_UC_FK_MATCH": (
        "ZA oficial sem correspondencia de UC por identificador ou nome confiavel; "
        "associacao apenas espacial nao e aceita."
    ),
    "SKIPPED_BY_UPDATE_RULE": "Registro existente com update_geom diferente de TRUE.",
    "UC_HAS_OFFICIAL_ZA": "Registro de Buffer de Abrangência bloqueado pois a UC possui ZA oficial ativa.",
    "INVALID_OR_EMPTY_GEOMETRY": "Registro com geometria nula ou vazia apos transformacao.",
    "ACTIVE_BUFFER_UNCHANGED": "Buffer de Abrangência ativo ja tem a mesma geometria e distancia; nenhuma versao nova.",
}

BUFFER_DISTANCE_M = 3000
GEOMETRY_EQUALITY_TOLERANCE_DEGREES = 1e-9


@dataclass(frozen=True)
class BranchSelection:
    batch_dir: Path
    dt_bronze: str


class ZaBufferPipelineError(RuntimeError):
    """Raised for ZA/Buffer de Abrangência pipeline errors."""


class ZaBufferPipelineService(DomainPipelineService):
    """Parallel branch service for ZA and Buffer de Abrangência stages."""

    def __init__(self, config: PipelineConfig | None = None) -> None:
        super().__init__(domain_name="za_buffer", config=config)
        self.logger = LOGGER

    # ===== ZA branch =====
    def extract_za(self, context: TaskExecutionContext) -> dict[str, Any]:
        requested_domain = (context.conf or {}).get("domain")
        if requested_domain == "za_oficial":
            return self._extract_branch(context, branch="za")
        source_root = Path(self.config.medallion_bronze_path)
        directed_uc = load_manifest_input(source_root, context.conf, expected_domain="uc")
        if directed_uc is not None:
            return self._extract_historical_za_for_directed_uc(
                context=context,
                source_root=source_root,
                directed_uc=directed_uc,
            )
        return self._extract_branch(context, branch="za")

    def _extract_historical_za_for_directed_uc(
        self,
        *,
        context: TaskExecutionContext,
        source_root: Path,
        directed_uc: Any,
    ) -> dict[str, Any]:
        """Select historical Bronze ZA features whose winning UC belongs to this import."""
        za_selections = self._list_branch_batches(source_root, "za")
        uc_selections = self._list_branch_batches(source_root, "ucs")
        if not za_selections or not uc_selections:
            state = {
                "branch": "za",
                "available": False,
                "run_id": context.run_id,
                "logical_date": context.logical_date,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            }
            self._save_state(context, branch="za", state=state)
            return self._result(
                context,
                status="skipped",
                branch="za",
                reason="no_historical_za_for_directed_uc",
            )

        run_dir = self._run_dir(context)
        run_dir.mkdir(parents=True, exist_ok=True)
        za_union_path = self._materialize_branch_union(
            za_selections, run_dir / "za_for_directed_uc"
        )
        uc_union_path = self._materialize_branch_union(
            uc_selections, run_dir / "ucs_for_directed_za_selection"
        )
        za_gdf = self._repair_geometries(
            self._ensure_epsg_4674(self._read_geodata(za_union_path))
        )
        all_uc_gdf = self._repair_geometries(
            self._ensure_epsg_4674(self._read_geodata(uc_union_path))
        )
        directed_uc_gdf = self._apply_manifest_metadata(
            self._read_geodata(directed_uc.canonical_path),
            {"manifest": directed_uc.payload},
            branch="buffer",
        )
        target_uc_ids = {
            str(value)
            for value in directed_uc_gdf["uc_id"].dropna().astype(str).tolist()
        }
        selected_matches = self._selected_za_matches(all_uc_gdf, za_gdf)
        selected_indexes = [
            source_index
            for source_index, uc_id in selected_matches.items()
            if uc_id in target_uc_ids
        ]
        if not selected_indexes:
            state = {
                "branch": "za",
                "available": False,
                "run_id": context.run_id,
                "logical_date": context.logical_date,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            }
            self._save_state(context, branch="za", state=state)
            return self._result(
                context,
                status="skipped",
                branch="za",
                reason="no_historical_za_for_directed_uc",
            )

        selected_za = za_gdf.loc[selected_indexes].copy()
        selected_path = run_dir / "za_for_directed_uc.gpkg"
        selected_za.to_file(selected_path, layer="bronze_union", driver="GPKG")
        bronze_dates = pd.to_datetime(selected_za["_bronze_dt"], errors="coerce")
        dt_bronze = bronze_dates.max().date().isoformat()
        state = {
            "branch": "za",
            "available": True,
            "run_id": context.run_id,
            "logical_date": context.logical_date,
            "dt_bronze": dt_bronze,
            "bronze_batch_dir": str(source_root / "za"),
            "source_path": str(selected_path),
            "shapefile_path": str(selected_path),
            "source_mode": "historical_za_bronze_for_uc_manifest",
            "trigger_import_id": directed_uc.import_id,
            "target_uc_ids": sorted(target_uc_ids),
            "temporal_folder": self._temporal_folder_stamp(context),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        self._save_state(context, branch="za", state=state)
        return self._result(
            context,
            status="extracted",
            branch="za",
            source_mode=state["source_mode"],
            selected_records=int(len(selected_za)),
            target_uc_ids=state["target_uc_ids"],
        )

    def validate_za(self, context: TaskExecutionContext) -> dict[str, Any]:
        return self._validate_branch(context, branch="za")

    def transform_za(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context, branch="za")
        if not state.get("available", False):
            return self._result(context, status="skipped", branch="za", reason="no_bronze_input")

        gdf = self._apply_manifest_metadata(
            self._read_geodata(Path(state["source_path"])), state, branch="za"
        )
        gdf = self._ensure_epsg_4674(gdf)
        gdf = self._repair_geometries(gdf)

        transformed = pd.DataFrame(index=gdf.index)
        transformed["uc_id_source"] = self._get_source_value(gdf, ["uc_id", "id_uc"])
        transformed["source_uc_code"] = self._get_source_value(gdf, ["zam_uco_cd"])
        transformed["cd_cnuc_source"] = self._get_source_value(gdf, ["cd_cnuc", "cod_cnuc"])
        transformed["nm_uc_source"] = self._get_source_value(gdf, ["nm_uc", "nome_uc", "nome", "label"])
        transformed["id_za_oficial_source"] = self._to_int_series(self._get_source_value(gdf, ["id_za_ofic", "id"]))
        transformed["ds_fonte"] = self._get_source_value(gdf, ["ds_fonte", "obs"]) 
        source_update_geom = self._get_source_value(gdf, ["update_geo", "update_geom"])
        transformed["update_geom"] = (
            source_update_geom.apply(self._normalize_update_geom_text) if source_update_geom is not None else None
        )
        transformed["fl_ativa"] = True
        transformed["geom"] = gdf.geometry.apply(self._normalize_polygon_to_multipolygon)
        transformed["dt_bronze"] = state["dt_bronze"]
        transformed["dt_silver"] = state.get("dt_silver")
        transformed["dt_gold"] = state.get("dt_gold")
        transformed["versao_dag"] = context.dag_id
        source_dates = pd.to_datetime(
            self._get_source_value(gdf, ["_bronze_dt"]), errors="coerce"
        )
        source_indexes = self._get_source_value(gdf, ["_bronze_union_index"])
        transformed["source_feature_key"] = [
            f"{(source_dates.iloc[position].date().isoformat() if source_dates is not None and not pd.isna(source_dates.iloc[position]) else state['dt_bronze'])}:za:"
            f"{int(source_indexes.iloc[position]) if source_indexes is not None and not pd.isna(source_indexes.iloc[position]) else source_index}"
            for position, source_index in enumerate(transformed.index)
        ]

        transformed = transformed[~transformed["geom"].isna()]
        transformed = transformed[~gpd.GeoSeries(transformed["geom"], crs=gdf.crs).is_empty]

        transformed_path = self._run_dir(context) / "za_transformed.gpkg"
        transformed_gdf = gpd.GeoDataFrame(transformed, geometry="geom", crs=gdf.crs)
        candidates_gdf = self._build_za_spatial_candidates(
            context=context,
            state=state,
            za_gdf=transformed_gdf,
        )
        transformed_gdf = self._exclude_all_uc_geometries(
            context=context,
            zones=transformed_gdf,
            source_root=Path(self.config.medallion_bronze_path),
            artifact_name="ucs_for_za_clipping",
        )
        transformed_gdf.to_file(
            transformed_path,
            layer="za_oficial_stage",
            driver="GPKG",
        )
        candidates_path = self._run_dir(context) / "za_uc_candidates.gpkg"
        candidates_gdf.to_file(
            candidates_path,
            layer="za_uc_candidates",
            driver="GPKG",
        )

        state["transformed_path"] = str(transformed_path)
        state["transformed_records"] = int(len(transformed_gdf))
        state["candidates_path"] = str(candidates_path)
        state["candidate_records"] = int(len(candidates_gdf))
        self._save_state(context, branch="za", state=state)

        return self._result(
            context,
            status="transformed",
            branch="za",
            transformed_path=str(transformed_path),
            transformed_records=int(len(transformed_gdf)),
            candidate_records=int(len(candidates_gdf)),
        )

    def load_silver_za(self, context: TaskExecutionContext) -> dict[str, Any]:
        return self._load_silver_branch(context, branch="za", layer_name="za_oficial_stage", file_name="za_silver.gpkg")

    def load_gold_za(self, context: TaskExecutionContext) -> dict[str, Any]:
        result = self._load_gold_branch(context, branch="za", layer_name="za_oficial_stage", file_geojson="za_gold.geojson")
        state = self._load_state(context, branch="za")
        candidates_path = state.get("candidates_path")
        if candidates_path:
            target_dir = self._build_layer_output_dir(
                Path(self.config.medallion_gold_path), "za_revisao", state["temporal_folder"]
            )
            review_path = target_dir / "za_uc_candidates.gpkg"
            candidates = self._read_geodata(Path(candidates_path), layer="za_uc_candidates")
            candidates.to_file(review_path, layer="za_uc_candidates", driver="GPKG")
            result["review_gpkg"] = str(review_path)
        return result

    def load_postgres_za(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context, branch="za")
        if not state.get("available", False) or not state.get("transformed_path"):
            self._write_quality_report(
                context=context,
                branch="za",
                stage="load_postgres_za",
                source_records=0,
                inserted_records=0,
                inserted_new_records=0,
                inserted_updated_records=0,
                skipped_records=0,
                result_reason="no_transformed_input",
                rejected_frames={},
            )
            return self._result(context, status="skipped", branch="za", reason="no_transformed_input")

        if not state.get("manifest"):
            return self._load_za_review_candidates(context, state)

        transformed = self._read_geodata(Path(state["transformed_path"]), layer="za_oficial_stage")
        source_records = int(len(transformed))
        transformed["id_uc"] = self._resolve_uc_fk(
            transformed,
            allow_spatial=False,
            require_authoritative_source_for_spatial=True,
        )
        rejected_no_fk = transformed[transformed["id_uc"].isna()].copy()
        transformed = transformed[~transformed["id_uc"].isna()].copy()
        skipped_no_fk = int(len(rejected_no_fk))
        rejected_frames: dict[str, pd.DataFrame] = {}
        if not rejected_no_fk.empty:
            rejected_frames["NO_TRUSTED_UC_FK_MATCH"] = rejected_no_fk

        if transformed.empty:
            if state.get("manifest"):
                error_code = "ZA_TARGET_UC_NOT_FOUND"
                detail = (
                    "A ZA oficial não corresponde a uma UC por `uc_identifier`, `cd_cnuc` "
                    "ou nome exato. Vínculo cadastral por mera interseção espacial é proibido."
                )
                self._write_api_error_result(context, state, error_code, detail)
                raise ZaBufferPipelineError(f"{error_code}: {detail}")
            skipped_records = skipped_no_fk
            self._write_quality_report(
                context=context,
                branch="za",
                stage="load_postgres_za",
                source_records=source_records,
                inserted_records=0,
                inserted_new_records=0,
                inserted_updated_records=0,
                skipped_records=skipped_records,
                result_reason="no_uc_fk_match",
                rejected_frames=rejected_frames,
            )
            return self._result(
                context,
                status="loaded_postgres",
                branch="za",
                inserted_records=0,
                skipped_records=skipped_no_fk,
                reason="no_uc_fk_match",
            )

        manifest = state.get("manifest") or {}
        if manifest:
            operation = str(manifest.get("operation") or "")
            if operation != "replace_buffer_abrangencia":
                error_code = "DIRECTED_ZA_OPERATION_NOT_ENABLED"
                detail = (
                    "A DAG_ZA_BUFFER aceita publicação isolada de ZA somente com "
                    "operation=replace_buffer_abrangencia. Uma nova UC com ZA deve usar o lote atômico UC+ZA."
                )
                self._write_api_error_result(context, state, error_code, detail)
                raise ZaBufferPipelineError(f"{error_code}: {detail}")
            return self._load_directed_replace_buffer(
                context=context,
                state=state,
                transformed=transformed,
                source_records=source_records,
                skipped_no_fk=skipped_no_fk,
                rejected_frames=rejected_frames,
            )

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE za_oficial
                    ADD COLUMN IF NOT EXISTS update_geom TEXT;
                    """
                )

                candidate_uc_ids = transformed["id_uc"].dropna().astype(int).unique().tolist()
                existing_uc_ids: set[int] = set()
                current_versions: dict[int, int] = {}
                if candidate_uc_ids:
                    cur.execute(
                        """
                        SELECT id_uc, COALESCE(MAX(numero_versao), 0)
                        FROM za_oficial
                        WHERE id_uc = ANY(%s)
                        GROUP BY id_uc;
                        """,
                        (candidate_uc_ids,),
                    )
                    current_versions = {
                        int(db_row[0]): int(db_row[1])
                        for db_row in cur.fetchall()
                        if db_row[0] is not None
                    }
                    existing_uc_ids = set(current_versions)

                total_records = int(len(transformed))
                is_existing = transformed["id_uc"].astype(int).isin(existing_uc_ids)
                is_update_true = transformed["update_geom"].apply(self._is_truthy_update_geom)
                should_insert = (~is_existing) | (is_existing & is_update_true)

                rejected_update = transformed[~should_insert].copy()
                if not rejected_update.empty:
                    rejected_frames["SKIPPED_BY_UPDATE_RULE"] = rejected_update

                transformed_to_insert = transformed[should_insert].copy()
                skipped_by_update_rules = int(len(rejected_update))
                skipped_records = int(skipped_no_fk + skipped_by_update_rules)

                if transformed_to_insert.empty:
                    self._write_quality_report(
                        context=context,
                        branch="za",
                        stage="load_postgres_za",
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
                        branch="za",
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        reason="no_eligible_rows_after_merge_rules",
                    )

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
                        branch="za",
                        stage="load_postgres_za",
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
                        branch="za",
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        reason="no_valid_geometry_for_insert",
                    )

                rows_existing_to_update = transformed_to_insert[
                    transformed_to_insert["id_uc"].astype(int).isin(existing_uc_ids)
                ]
                uc_ids_to_refresh = rows_existing_to_update["id_uc"].dropna().astype(int).unique().tolist()
                inserted_updated_records = int(len(rows_existing_to_update))
                inserted_new_records = int(len(transformed_to_insert) - inserted_updated_records)

                rows = [
                    (
                        int(row["id_uc"]),
                        self._none_if_nan(row.get("ds_fonte")),
                        self._none_if_nan(row.get("update_geom")),
                        True,
                        row[geom_column].wkt if row.get(geom_column) is not None else None,
                        state["dt_bronze"],
                        state.get("dt_silver"),
                        state.get("dt_gold"),
                        context.dag_id,
                        current_versions.get(int(row["id_uc"]), 0) + 1,
                        "ZA oficial carregada pelo pipeline agendado",
                        "airflow",
                        context.run_id,
                        context.run_id,
                    )
                    for _, row in transformed_to_insert.iterrows()
                ]

                uc_ids_to_activate = transformed_to_insert["id_uc"].dropna().astype(int).unique().tolist()
                cur.execute(
                    "SELECT id_uc FROM uc WHERE id_uc = ANY(%s) ORDER BY id_uc FOR UPDATE",
                    (uc_ids_to_activate,),
                )

                cur.execute(
                    """
                    UPDATE buffer_abrangencia
                    SET fl_ativa = FALSE,
                        dt_fim_vigencia = GREATEST(CURRENT_DATE, dt_inicio_vigencia)
                    WHERE id_uc = ANY(%s) AND fl_ativa = TRUE
                    RETURNING id_buffer_abrangencia, id_uc;
                    """,
                    (uc_ids_to_activate,),
                )
                deactivated_legacy_buffer = cur.fetchall()
                for id_buffer_abrangencia, id_uc in deactivated_legacy_buffer:
                    event_key = f"{context.run_id}:buffer:{id_buffer_abrangencia}"[:255]
                    cur.execute(
                        """
                        INSERT INTO cadastral_event (
                            event_id, entity_type, entity_id, event_type, actor, reason, source,
                            correlation_id, idempotency_key, previous_state, new_state
                        ) VALUES (
                            %s, 'BUFFER_ABRANGENCIA', %s, 'BUFFER_ABRANGENCIA_DEACTIVATED_BY_OFFICIAL_ZA', 'airflow',
                            'Precedência de ZA oficial ativa', 'pipeline-agendado', %s, %s,
                            %s, %s
                        )
                        ON CONFLICT (idempotency_key) DO NOTHING;
                        """,
                        (
                            str(uuid4()),
                            int(id_buffer_abrangencia),
                            context.run_id,
                            event_key,
                            Json({"id_uc": int(id_uc), "id_buffer_abrangencia": int(id_buffer_abrangencia), "active": True}),
                            Json({"id_uc": int(id_uc), "id_buffer_abrangencia": int(id_buffer_abrangencia), "active": False}),
                        ),
                    )

                if uc_ids_to_refresh:
                    cur.execute(
                        """
                        UPDATE za_oficial
                        SET fl_ativa = FALSE,
                            dt_fim_vigencia = GREATEST(CURRENT_DATE, dt_inicio_vigencia)
                        WHERE id_uc = ANY(%s) AND fl_ativa = TRUE;
                        """,
                        (uc_ids_to_refresh,),
                    )

                execute_batch(
                    cur,
                    """
                    INSERT INTO za_oficial (
                        id_uc, ds_fonte, update_geom, fl_ativa, geom, dt_bronze, dt_silver,
                        dt_gold, versao_dag, numero_versao, motivo, ator, correlation_id,
                        dag_run_id
                    ) VALUES (
                        %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4674), %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    );
                    """,
                    rows,
                    page_size=200,
                )
                cur.execute(
                    """
                    UPDATE buffer_abrangencia AS buffer
                    SET geom = ST_Multi(ST_CollectionExtract(ST_Difference(buffer.geom, uc.geom), 3))
                    FROM uc
                    WHERE uc.id_uc = buffer.id_uc
                      AND buffer.id_uc = ANY(%s)
                      AND buffer.fl_ativa = TRUE;
                    """,
                    (uc_ids_to_activate,),
                )

        self._write_quality_report(
            context=context,
            branch="za",
            stage="load_postgres_za",
            source_records=source_records,
            inserted_records=len(rows),
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
            result_reason="loaded_postgres",
            rejected_frames=rejected_frames,
        )

        return self._result(
            context,
            status="loaded_postgres",
            branch="za",
            inserted_records=len(rows),
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
        )

    def _build_za_spatial_candidates(
        self,
        *,
        context: TaskExecutionContext,
        state: dict[str, Any],
        za_gdf: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Build ZA-to-UC candidates from every Bronze batch using the approved spatial rule."""
        source_root = Path(self.config.medallion_bronze_path)
        uc_selections = self._list_branch_batches(source_root, "ucs")
        if not uc_selections:
            raise ZaBufferPipelineError("UC Bronze input is required to derive ZA review candidates.")

        run_dir = self._run_dir(context)
        uc_path = self._materialize_branch_union(uc_selections, run_dir / "ucs_for_za_candidates")
        uc_gdf = self._repair_geometries(self._ensure_epsg_4674(self._read_geodata(uc_path)))
        za_metric = za_gdf.to_crs(epsg=31982)
        uc_metric = uc_gdf.to_crs(epsg=31982)
        records: list[dict[str, Any]] = []
        za_geometry_column = za_metric.geometry.name
        uc_geometry_column = uc_metric.geometry.name

        for _, za_row in za_metric.iterrows():
            za_geometry = za_row[za_geometry_column]
            if za_geometry is None or za_geometry.is_empty:
                continue
            for _, uc_row in uc_metric.iterrows():
                uc_geometry = uc_row[uc_geometry_column]
                if uc_geometry is None or uc_geometry.is_empty:
                    continue
                intersection_area = float(za_geometry.intersection(uc_geometry).area)
                distance_m = float(za_geometry.distance(uc_geometry))
                if intersection_area <= 0 and distance_m > 0.01:
                    continue
                records.append(
                    {
                        "source_feature_key": za_row["source_feature_key"],
                        "uc_id_source": str(uc_row.get("uc_id")),
                        "cd_cnuc_source": uc_row.get("cd_cnuc"),
                        "nm_uc_source": uc_row.get("nome_uc"),
                        "id_za_origem": self._none_if_nan(za_row.get("id_za_oficial_source")),
                        "codigo_uc_origem": self._none_if_nan(za_row.get("source_uc_code")),
                        "nm_za_origem": self._none_if_nan(za_row.get("nm_za_source")),
                        "criterio_vinculo": "MAIOR_INTERSECAO_BRONZE" if intersection_area > 0 else "ADJACENCIA_EXATA_BRONZE",
                        "area_intersecao_m2": intersection_area,
                        "distancia_minima_m": distance_m,
                        "status_revisao": "PENDENTE_REVISAO",
                        "geometry": za_geometry,
                    }
                )

        if not records:
            return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs="EPSG:4674")
        candidates = gpd.GeoDataFrame(records, geometry="geometry", crs=za_metric.crs)
        winner_indexes = (
            candidates.sort_values(
                ["source_feature_key", "area_intersecao_m2", "distancia_minima_m"],
                ascending=[True, False, True],
            )
            .groupby("source_feature_key", sort=False)
            .head(1)
            .index
        )
        candidates["fl_vinculo_selecionado"] = candidates.index.isin(winner_indexes)
        candidates.loc[candidates["fl_vinculo_selecionado"], "status_revisao"] = "CONFIRMADA"
        candidates.loc[~candidates["fl_vinculo_selecionado"], "status_revisao"] = "REJEITADA"
        return candidates.to_crs(epsg=4674)

    def _exclude_all_uc_geometries(
        self,
        *,
        context: TaskExecutionContext,
        zones: gpd.GeoDataFrame,
        source_root: Path,
        artifact_name: str,
    ) -> gpd.GeoDataFrame:
        """Clip a zone layer by all current UC Bronze geometries to preserve exclusivity."""
        selections = self._list_branch_batches(source_root, "ucs")
        if not selections:
            raise ZaBufferPipelineError("UC Bronze input is required to enforce spatial exclusivity.")
        source_path = self._materialize_branch_union(selections, self._run_dir(context) / artifact_name)
        uc_gdf = self._repair_geometries(self._ensure_epsg_4674(self._read_geodata(source_path)))
        uc_metric = uc_gdf.to_crs(epsg=31982)
        uc_union = unary_union([geometry for geometry in uc_metric.geometry if geometry is not None and not geometry.is_empty])
        metric_zones = zones.to_crs(epsg=31982).copy()
        metric_zones.geometry = metric_zones.geometry.apply(lambda geometry: geometry.difference(uc_union))
        result = metric_zones.to_crs(epsg=4674)
        result.geometry = result.geometry.apply(self._make_valid_geometry).apply(self._normalize_polygon_to_multipolygon)
        return result

    def _load_za_review_candidates(
        self,
        context: TaskExecutionContext,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        """Persist pending spatial candidates without publishing an active official ZA."""
        candidates_path = state.get("candidates_path")
        if not candidates_path:
            raise ZaBufferPipelineError("ZA candidate stage is required for scheduled ZA ingestion.")

        candidates = self._read_geodata(Path(candidates_path), layer="za_uc_candidates")
        published_zones = self._read_geodata(
            Path(state["transformed_path"]), layer="za_oficial_stage"
        )
        published_geometry_by_key = {
            str(row["source_feature_key"]): row[published_zones.geometry.name]
            for _, row in published_zones.iterrows()
        }
        if candidates.empty:
            return self._result(context, status="loaded_postgres", branch="za", candidate_records=0)

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_uc, uc_id FROM uc")
                uc_by_source_id = {str(uc_id): int(id_uc) for id_uc, uc_id in cur.fetchall() if uc_id is not None}
                persisted = 0
                published = 0
                geometry_column = candidates.geometry.name
                for _, row in candidates.iterrows():
                    id_uc = uc_by_source_id.get(str(row["uc_id_source"]))
                    if id_uc is None:
                        continue
                    published_geometry = published_geometry_by_key.get(
                        str(row["source_feature_key"]), row[geometry_column]
                    )
                    cur.execute(
                        """
                        INSERT INTO za_oficial_fonte (
                            chave_origem, id_za_origem, codigo_uc_origem, nm_za_origem,
                            ds_fonte, geom, dt_bronze, versao_dag
                        ) VALUES (
                            %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4674), %s, %s
                        )
                        ON CONFLICT (chave_origem) DO UPDATE SET
                            id_za_origem = EXCLUDED.id_za_origem,
                            codigo_uc_origem = EXCLUDED.codigo_uc_origem,
                            nm_za_origem = EXCLUDED.nm_za_origem,
                            geom = EXCLUDED.geom,
                            dt_bronze = EXCLUDED.dt_bronze,
                            versao_dag = EXCLUDED.versao_dag
                        RETURNING id_za_fonte;
                        """,
                        (
                            row["source_feature_key"],
                            self._none_if_nan(row.get("id_za_origem")),
                            self._none_if_nan(row.get("codigo_uc_origem")),
                            self._none_if_nan(row.get("nm_za_origem")),
                            "bronze_za",
                            published_geometry.wkt,
                            state["dt_bronze"],
                            context.dag_id,
                        ),
                    )
                    id_za_fonte = int(cur.fetchone()[0])
                    cur.execute(
                        """
                        INSERT INTO za_oficial_uc (
                            id_za_fonte, id_uc, status_revisao, criterio_vinculo,
                            area_intersecao_m2, distancia_minima_m
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id_za_fonte, id_uc) DO UPDATE SET
                            status_revisao = EXCLUDED.status_revisao,
                            criterio_vinculo = EXCLUDED.criterio_vinculo,
                            area_intersecao_m2 = EXCLUDED.area_intersecao_m2,
                            distancia_minima_m = EXCLUDED.distancia_minima_m;
                        """,
                        (
                            id_za_fonte,
                            id_uc,
                            row["status_revisao"],
                            row["criterio_vinculo"],
                            row["area_intersecao_m2"],
                            row["distancia_minima_m"],
                        ),
                    )
                    if bool(row["fl_vinculo_selecionado"]):
                        cur.execute(
                            "SELECT 1 FROM za_oficial WHERE id_uc = %s AND fl_ativa = TRUE",
                            (id_uc,),
                        )
                        if cur.fetchone() is None:
                            cur.execute(
                                """
                                INSERT INTO za_oficial (
                                    id_uc, ds_fonte, fl_ativa, geom, dt_bronze, versao_dag,
                                    numero_versao, motivo, ator, correlation_id, import_id, dag_run_id
                                ) VALUES (
                                    %s, %s, TRUE, ST_SetSRID(ST_GeomFromText(%s), 4674), %s, %s,
                                    1, %s, 'airflow', %s, %s, %s
                                );
                                """,
                                (
                                    id_uc,
                                    "bronze_za_maior_intersecao",
                                    published_geometry.wkt,
                                    state["dt_bronze"],
                                    context.dag_id,
                                    "ZA oficial vinculada pela maior interseção Bronze",
                                    context.run_id,
                                    state.get("trigger_import_id"),
                                    context.run_id,
                                ),
                            )
                            published += 1
                    persisted += 1

        return self._result(
            context,
            status="loaded_postgres",
            branch="za",
            candidate_records=persisted,
            published_official_zones=published,
        )

    def _load_directed_replace_buffer(
        self,
        *,
        context: TaskExecutionContext,
        state: dict[str, Any],
        transformed: gpd.GeoDataFrame,
        source_records: int,
        skipped_no_fk: int,
        rejected_frames: dict[str, pd.DataFrame],
    ) -> dict[str, Any]:
        manifest = state["manifest"]
        metadata = manifest.get("metadata") or {}
        import_id = str(manifest.get("import_id") or state.get("import_id") or "")
        correlation_id = str(manifest.get("correlation_id") or import_id)
        idempotency_key = str(manifest.get("idempotency_key") or import_id)
        actor = str(manifest.get("actor") or metadata.get("actor") or "api-client")
        source = str(metadata.get("source") or "api-bronze-airflow")
        reason = str(metadata.get("reason") or "Substituição do Buffer de Abrangência por ZA oficial")
        valid_from = metadata.get("valid_from") or datetime.now(timezone.utc).date().isoformat()

        if len(transformed) != 1:
            error_code = "DIRECTED_IMPORT_REQUIRES_SINGLE_ZA"
            detail = "A substituição dirigida deve conter exatamente uma ZA oficial."
            self._write_api_error_result(context, state, error_code, detail)
            raise ZaBufferPipelineError(f"{error_code}: {detail}")

        row = transformed.iloc[0]
        id_uc = int(row["id_uc"])
        geom_column = str(transformed.geometry.name)
        geometry = row.get(geom_column)
        if geometry is None or geometry.is_empty:
            error_code = "INVALID_OR_EMPTY_GEOMETRY"
            detail = "A ZA oficial dirigida não possui geometria válida para persistência."
            self._write_api_error_result(context, state, error_code, detail)
            raise ZaBufferPipelineError(f"{error_code}: {detail}")

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT entity_id FROM cadastral_event WHERE idempotency_key = %s",
                    (idempotency_key,),
                )
                replay = cur.fetchone()
                if replay:
                    self.logger.info(
                        "Directed ZA mutation replay detected. import_id=%s id_uc=%s",
                        import_id,
                        replay[0],
                    )
                    return self._directed_za_load_result(
                        context=context,
                        source_records=source_records,
                        inserted_records=0,
                        skipped_records=skipped_no_fk,
                        rejected_frames=rejected_frames,
                        deactivated_buffer=0,
                        idempotent_replay=True,
                    )

                cur.execute(
                    "SELECT id_uc, uc_id, nm_uc FROM uc WHERE id_uc = %s FOR UPDATE",
                    (id_uc,),
                )
                uc = cur.fetchone()
                if not uc:
                    error_code = "ZA_TARGET_UC_NOT_FOUND"
                    detail = "A UC vinculada à ZA oficial não existe mais."
                    self._write_api_error_result(context, state, error_code, detail)
                    raise ZaBufferPipelineError(f"{error_code}: {detail}")

                cur.execute(
                    """
                    SELECT id_za_oficial
                    FROM za_oficial
                    WHERE id_uc = %s AND fl_ativa = TRUE
                    FOR UPDATE;
                    """,
                    (id_uc,),
                )
                active_za = cur.fetchone()
                if active_za:
                    error_code = "UC_ALREADY_HAS_ACTIVE_OFFICIAL_ZA"
                    detail = (
                        f"A UC {id_uc} já possui a ZA oficial ativa {active_za[0]}; "
                        "replace_buffer_abrangencia não atualiza uma ZA oficial existente."
                    )
                    self._write_api_error_result(context, state, error_code, detail)
                    raise ZaBufferPipelineError(f"{error_code}: {detail}")

                cur.execute(
                    """
                    SELECT id_buffer_abrangencia, numero_versao, dt_inicio_vigencia, dt_fim_vigencia
                    FROM buffer_abrangencia
                    WHERE id_uc = %s AND fl_ativa = TRUE
                    FOR UPDATE;
                    """,
                    (id_uc,),
                )
                active_buffer = cur.fetchall()
                if len(active_buffer) != 1:
                    error_code = "ACTIVE_BUFFER_ABRANGENCIA_NOT_FOUND" if not active_buffer else "MULTIPLE_ACTIVE_BUFFER_ABRANGENCIA"
                    detail = (
                        f"A UC {id_uc} precisa possuir exatamente um Buffer de Abrangência ativo para "
                        "executar replace_buffer_abrangencia."
                    )
                    self._write_api_error_result(context, state, error_code, detail)
                    raise ZaBufferPipelineError(f"{error_code}: {detail}")

                old_buffer = active_buffer[0]
                cur.execute(
                    "SELECT COALESCE(MAX(numero_versao), 0) FROM za_oficial WHERE id_uc = %s",
                    (id_uc,),
                )
                next_version = int(cur.fetchone()[0]) + 1

                cur.execute(
                    """
                    UPDATE buffer_abrangencia
                    SET
                        fl_ativa = FALSE,
                        dt_fim_vigencia = GREATEST(%s::date, dt_inicio_vigencia)
                    WHERE id_buffer_abrangencia = %s AND fl_ativa = TRUE;
                    """,
                    (valid_from, int(old_buffer[0])),
                )
                if cur.rowcount != 1:
                    raise ZaBufferPipelineError(
                        "BUFFER_ABRANGENCIA_CONCURRENT_CHANGE: o Buffer de Abrangência ativo mudou durante a substituição."
                    )

                cur.execute(
                    """
                    INSERT INTO za_oficial (
                        id_uc, ds_fonte, update_geom, dt_inicio_vigencia, fl_ativa, geom,
                        dt_bronze, dt_silver, dt_gold, versao_dag, numero_versao,
                        motivo, ator, correlation_id, import_id, dag_run_id
                    ) VALUES (
                        %s, %s, 'TRUE', %s, TRUE,
                        ST_SetSRID(ST_GeomFromText(%s), 4674), %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    RETURNING id_za_oficial;
                    """,
                    (
                        id_uc,
                        self._none_if_nan(row.get("ds_fonte")) or source,
                        valid_from,
                        geometry.wkt,
                        state["dt_bronze"],
                        state.get("dt_silver"),
                        state.get("dt_gold"),
                        context.dag_id,
                        next_version,
                        reason,
                        actor,
                        correlation_id,
                        import_id,
                        context.run_id,
                    ),
                )
                id_za_oficial = int(cur.fetchone()[0])

                previous_state = {
                    "id_uc": id_uc,
                    "uc_identifier": uc[1],
                    "uc_name": uc[2],
                    "active_zone_type": "BUFFER_ABRANGENCIA",
                    "id_buffer_abrangencia": int(old_buffer[0]),
                    "buffer_version": int(old_buffer[1]),
                    "valid_from": str(old_buffer[2]),
                    "valid_until": str(old_buffer[3]) if old_buffer[3] else None,
                }
                new_state = {
                    "id_uc": id_uc,
                    "active_zone_type": "ZA_OFICIAL",
                    "id_za_oficial": id_za_oficial,
                    "za_version": next_version,
                    "valid_from": str(valid_from),
                    "replaced_buffer_id": int(old_buffer[0]),
                    "import_id": import_id,
                    "dag_run_id": context.run_id,
                }
                cur.execute(
                    """
                    INSERT INTO cadastral_event (
                        event_id, entity_type, entity_id, event_type, actor, reason, source,
                        correlation_id, idempotency_key, previous_state, new_state
                    ) VALUES (
                        %s, 'UC', %s, 'BUFFER_ABRANGENCIA_REPLACED_BY_OFFICIAL_ZA', %s, %s, %s,
                        %s, %s, %s, %s
                    );
                    """,
                    (
                        str(uuid4()),
                        id_uc,
                        actor,
                        reason,
                        source,
                        correlation_id,
                        idempotency_key,
                        Json(previous_state),
                        Json(new_state),
                    ),
                )

        return self._directed_za_load_result(
            context=context,
            source_records=source_records,
            inserted_records=1,
            skipped_records=skipped_no_fk,
            rejected_frames=rejected_frames,
            deactivated_buffer=1,
            idempotent_replay=False,
        )

    def _directed_za_load_result(
        self,
        *,
        context: TaskExecutionContext,
        source_records: int,
        inserted_records: int,
        skipped_records: int,
        rejected_frames: dict[str, pd.DataFrame],
        deactivated_buffer: int,
        idempotent_replay: bool,
    ) -> dict[str, Any]:
        self._write_quality_report(
            context=context,
            branch="za",
            stage="load_postgres_za",
            source_records=source_records,
            inserted_records=inserted_records,
            inserted_new_records=inserted_records,
            inserted_updated_records=0,
            skipped_records=skipped_records,
            result_reason="loaded_postgres",
            rejected_frames=rejected_frames,
        )
        return self._result(
            context,
            status="loaded_postgres",
            branch="za",
            inserted_records=inserted_records,
            inserted_new_records=inserted_records,
            inserted_updated_records=0,
            skipped_records=skipped_records,
            deactivated_buffer=deactivated_buffer,
            idempotent_replay=idempotent_replay,
        )

    def validate_zone_readiness(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Block thematic triggers until every active UC has one valid active ZA or Buffer de Abrangência."""
        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        u.id_uc,
                        u.uc_id,
                        COUNT(DISTINCT za.id_za_oficial) FILTER (WHERE za.fl_ativa = TRUE),
                        COUNT(DISTINCT buffer.id_buffer_abrangencia) FILTER (WHERE buffer.fl_ativa = TRUE),
                        COUNT(DISTINCT za.id_za_oficial) FILTER (
                            WHERE za.fl_ativa = TRUE
                              AND (za.geom IS NULL OR ST_IsEmpty(za.geom) OR NOT ST_IsValid(za.geom))
                        )
                        + COUNT(DISTINCT buffer.id_buffer_abrangencia) FILTER (
                            WHERE buffer.fl_ativa = TRUE
                              AND (buffer.geom IS NULL OR ST_IsEmpty(buffer.geom) OR NOT ST_IsValid(buffer.geom))
                        ) AS invalid_active_geometries
                    FROM uc AS u
                    LEFT JOIN za_oficial AS za ON za.id_uc = u.id_uc
                    LEFT JOIN buffer_abrangencia AS buffer ON buffer.id_uc = u.id_uc
                    WHERE COALESCE(u.situacao, 'ATIVA') = 'ATIVA'
                    GROUP BY u.id_uc, u.uc_id
                    ORDER BY u.id_uc;
                    """
                )
                rows = cur.fetchall()

        self._assert_zone_readiness_rows(rows)
        return self._result(
            context,
            status="zone_ready",
            active_uc_records=len(rows),
            invariant="exactly_one_valid_active_official_zone_or_buffer",
        )

    @staticmethod
    def _assert_zone_readiness_rows(rows: list[tuple[Any, ...]]) -> None:
        violations = [
            {
                "id_uc": int(id_uc),
                "uc_id": str(uc_id),
                "active_official_zones": int(active_official_zones),
                "active_buffer_abrangencia": int(active_buffer_abrangencia),
                "invalid_active_geometries": int(invalid_active_geometries),
            }
            for (
                id_uc,
                uc_id,
                active_official_zones,
                active_buffer_abrangencia,
                invalid_active_geometries,
            ) in rows
            if int(active_official_zones) + int(active_buffer_abrangencia) != 1
            or int(invalid_active_geometries) != 0
        ]
        if violations:
            raise ZaBufferPipelineError(
                "ZONE_READINESS_FAILED: thematic DAGs are blocked because active UCs "
                f"do not have exactly one valid active ZA or Buffer de Abrangência: {violations}"
            )

    # ===== Buffer de Abrangência branch =====
    def extract_buffer(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Prepare Buffers de Abrangência from the complete canonical UC and ZA Bronze history."""
        requested_domain = (context.conf or {}).get("domain")
        if requested_domain and requested_domain != "uc":
            return self._extract_branch(context, branch="buffer")
        source_root = Path(self.config.medallion_bronze_path)
        directed = load_manifest_input(source_root, context.conf, expected_domain="uc")
        uc_selections = self._list_branch_batches(source_root, "ucs")
        if directed is None and not uc_selections:
            state = {"branch": "buffer", "available": False, "run_id": context.run_id}
            self._save_state(context, branch="buffer", state=state)
            return self._result(context, status="skipped", branch="buffer", reason="no_uc_bronze_input")
        run_dir = self._run_dir(context)
        run_dir.mkdir(parents=True, exist_ok=True)
        all_uc_source_path = (
            self._materialize_branch_union(uc_selections, run_dir / "buffer_from_all_uc")
            if uc_selections else directed.canonical_path
        )
        source_path = directed.canonical_path if directed is not None else all_uc_source_path
        za_selections = self._list_branch_batches(source_root, "za")
        za_source_path = (
            self._materialize_branch_union(za_selections, run_dir / "za_for_buffer_eligibility")
            if za_selections else None
        )
        state = {
            "branch": "buffer",
            "available": True,
            "run_id": context.run_id,
            "logical_date": context.logical_date,
            "dt_bronze": (
                str(directed.payload.get("created_at", context.logical_date)).split("T", 1)[0]
                if directed is not None else max(selection.dt_bronze for selection in uc_selections)
            ),
            "bronze_batch_dir": str(directed.manifest_path.parent if directed is not None else source_root / "ucs"),
            "source_path": str(source_path),
            "all_uc_source_path": str(all_uc_source_path),
            "shapefile_path": str(source_path),
            "za_bronze_batch_dir": str(source_root / "za") if za_selections else None,
            "za_source_path": str(za_source_path) if za_source_path else None,
            "source_mode": "manifest_geojson" if directed is not None else "derived_from_uc_bronze:bronze_union",
            "manifest": directed.payload if directed is not None else None,
            "import_id": directed.import_id if directed is not None else None,
            "temporal_folder": self._temporal_folder_stamp(context),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        self._save_state(context, branch="buffer", state=state)
        return self._result(context, status="extracted", branch="buffer", source_mode=state["source_mode"])

    def validate_buffer(self, context: TaskExecutionContext) -> dict[str, Any]:
        return self._validate_branch(context, branch="buffer")

    def transform_buffer(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Create 3 km exclusive Buffer de Abrangência rings only for UCs without an official Bronze ZA."""
        state = self._load_state(context, branch="buffer")
        if not state.get("available", False):
            return self._result(context, status="skipped", branch="buffer", reason="no_bronze_input")

        gdf = self._apply_manifest_metadata(
            self._read_geodata(Path(state["source_path"])), state, branch="buffer"
        )
        gdf = self._ensure_epsg_4674(gdf)
        gdf = self._repair_geometries(gdf)
        all_uc_gdf = self._repair_geometries(
            self._ensure_epsg_4674(
                self._read_geodata(Path(state.get("all_uc_source_path", state["source_path"])))
            )
        )

        official_za_source = state.get("za_source_path")
        if official_za_source:
            za_gdf = self._repair_geometries(
                self._ensure_epsg_4674(self._read_geodata(Path(official_za_source)))
            )
            uc_ids_with_za = self._selected_za_uc_source_ids(all_uc_gdf, za_gdf)
            gdf = gdf.loc[~gdf["uc_id"].astype(str).isin(uc_ids_with_za)].copy()

        all_uc_metric = all_uc_gdf.to_crs(epsg=31982)
        source_metric = gdf.to_crs(epsg=31982)
        buffered = source_metric.copy()
        all_uc_union = unary_union([geometry for geometry in all_uc_metric.geometry if geometry is not None and not geometry.is_empty])
        buffered["geometry"] = source_metric.geometry.buffer(3000).difference(all_uc_union)
        buffered = buffered.to_crs(epsg=4674)
        buffered["geometry"] = buffered.geometry.apply(self._make_valid_geometry)

        transformed = pd.DataFrame(index=buffered.index)
        transformed["uc_id_source"] = self._get_source_value(gdf, ["uc_id", "id_uc", "id", "gid"])
        transformed["cd_cnuc_source"] = self._get_source_value(gdf, ["cd_cnuc", "cod_cnuc"])
        transformed["nm_uc_source"] = self._get_source_value(gdf, ["nm_uc", "nome_uc", "nome", "label"])
        transformed["nm_za_source"] = self._get_source_value(gdf, ["zam_nm_mic", "nome", "label"])
        transformed["ds_fonte"] = "buffer_3000m_from_uc_bronze"
        source_update_geom = self._get_source_value(gdf, ["update_geo", "update_geom"])
        transformed["update_geom"] = source_update_geom.apply(self._normalize_update_geom_text) if source_update_geom is not None else None
        transformed["dist_buffer_m"] = 3000
        transformed["fl_ativa"] = True
        transformed["geom"] = buffered.geometry.apply(self._normalize_polygon_to_multipolygon)
        transformed["dt_bronze"] = state["dt_bronze"]
        transformed["dt_silver"] = state.get("dt_silver")
        transformed["dt_gold"] = state.get("dt_gold")
        transformed["versao_dag"] = context.dag_id

        transformed = transformed[~transformed["geom"].isna()]
        transformed = transformed[~gpd.GeoSeries(transformed["geom"], crs=buffered.crs).is_empty]

        transformed_path = self._run_dir(context) / "buffer_transformed.gpkg"
        transformed_gdf = gpd.GeoDataFrame(transformed, geometry="geom", crs=buffered.crs)
        transformed_gdf.to_file(
            transformed_path,
            layer="buffer_abrangencia_stage",
            driver="GPKG",
        )

        state["transformed_path"] = str(transformed_path)
        state["transformed_records"] = int(len(transformed_gdf))
        self._save_state(context, branch="buffer", state=state)

        return self._result(
            context,
            status="transformed",
            branch="buffer",
            transformed_path=str(transformed_path),
            transformed_records=int(len(transformed_gdf)),
        )

    def _selected_za_uc_source_ids(self, uc_gdf: gpd.GeoDataFrame, za_gdf: gpd.GeoDataFrame) -> set[str]:
        """Select one UC per official ZA by greatest Bronze intersection area."""
        return set(self._selected_za_matches(uc_gdf, za_gdf).values())

    def _selected_za_matches(
        self, uc_gdf: gpd.GeoDataFrame, za_gdf: gpd.GeoDataFrame
    ) -> dict[Any, str]:
        """Return the deterministic winning UC for each Bronze ZA feature."""
        uc_metric = uc_gdf.to_crs(epsg=31982)
        za_metric = za_gdf.to_crs(epsg=31982)
        selected: dict[Any, str] = {}
        for za_index, za_geometry in za_metric.geometry.items():
            explicit_target = self._none_if_nan(za_metric.loc[za_index].get("_manifest_uc_identifier"))
            if explicit_target is not None:
                identifiers = uc_metric["uc_id"].astype(str).eq(str(explicit_target))
                for column in ("cd_cnuc", "wdpa_pid"):
                    if column in uc_metric:
                        identifiers |= uc_metric[column].astype(str).eq(str(explicit_target))
                targets = uc_metric.loc[identifiers, "uc_id"].astype(str).unique()
                if len(targets) == 1:
                    selected[za_index] = targets[0]
                # An explicit cadastral link must never be reassigned by intersection.
                continue
            matches = [
                (
                    float(za_geometry.intersection(uc_geometry).area),
                    float(za_geometry.distance(uc_geometry)),
                    str(uc_row["uc_id"]),
                )
                for _, uc_row in uc_metric.iterrows()
                for uc_geometry in [uc_row.geometry]
                if za_geometry is not None
                and not za_geometry.is_empty
                and uc_geometry is not None
                and not uc_geometry.is_empty
                and (
                    float(za_geometry.intersection(uc_geometry).area) > 0
                    or float(za_geometry.distance(uc_geometry)) <= 0.01
                )
            ]
            if matches:
                selected[za_index] = max(
                    matches, key=lambda match: (match[0], -match[1])
                )[2]
        return selected

    @staticmethod
    def _assert_postgres_does_not_override_bronze(
        bronze_buffer_uc_ids: set[int], postgres_official_za_uc_ids: set[int]
    ) -> None:
        """Fail when the Postgres projection contradicts the Bronze ZA source of truth."""
        conflicting_uc_ids = sorted(bronze_buffer_uc_ids & postgres_official_za_uc_ids)
        if conflicting_uc_ids:
            raise ZaBufferPipelineError(
                "Bronze/Postgres ZA conflict for id_uc="
                f"{conflicting_uc_ids}: Bronze selected these UCs for a 3 km Buffer de Abrangência, "
                "but Postgres has an active official ZA. Bronze is the source of truth; "
                "reconcile the Postgres projection instead of changing Buffer de Abrangência eligibility."
            )

    def _uc_has_official_za_in_bronze(
        self,
        uc_gdf: gpd.GeoDataFrame,
        za_gdf: gpd.GeoDataFrame,
    ) -> pd.Series:
        """Identify UCs covered by at least one official ZA using Bronze geometries only."""
        valid_za_geometries = [geometry for geometry in za_gdf.geometry if geometry is not None and not geometry.is_empty]
        if not valid_za_geometries:
            return pd.Series(False, index=uc_gdf.index, dtype=bool)

        return uc_gdf.geometry.apply(
            lambda uc_geometry: any(
                uc_geometry is not None
                and not uc_geometry.is_empty
                and uc_geometry.intersects(za_geometry)
                for za_geometry in valid_za_geometries
            )
        )

    def _assert_za_bronze_has_canonical_uc_link(self, za_gdf: gpd.GeoDataFrame) -> None:
        """Reject scheduled ZA input that cannot identify its target UC without spatial guessing."""
        for column_name in ("uc_id", "id_uc", "cd_cnuc", "cod_cnuc"):
            source_value = self._get_source_value(za_gdf, [column_name])
            if source_value is not None and source_value.notna().any():
                return

        raise ZaBufferPipelineError(
            "ZA Bronze requires a canonical UC identifier (uc_id, id_uc or cd_cnuc). "
            "Spatial intersection cannot establish an official ZA-to-UC relationship."
        )

    def load_silver_buffer(self, context: TaskExecutionContext) -> dict[str, Any]:
        return self._load_silver_branch(context, branch="buffer", layer_name="buffer_abrangencia_stage", file_name="buffer_silver.gpkg")

    def load_gold_buffer(self, context: TaskExecutionContext) -> dict[str, Any]:
        return self._load_gold_branch(context, branch="buffer", layer_name="buffer_abrangencia_stage", file_geojson="buffer_gold.geojson")

    def load_postgres_buffer(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context, branch="buffer")
        if not state.get("available", False) or not state.get("transformed_path"):
            self._write_quality_report(
                context=context,
                branch="buffer",
                stage="load_postgres_buffer",
                source_records=0,
                inserted_records=0,
                inserted_new_records=0,
                inserted_updated_records=0,
                skipped_records=0,
                result_reason="no_transformed_input",
                rejected_frames={},
            )
            return self._result(context, status="skipped", branch="buffer", reason="no_transformed_input")

        transformed = self._read_geodata(Path(state["transformed_path"]), layer="buffer_abrangencia_stage")
        source_records = int(len(transformed))
        transformed["id_uc"] = self._resolve_uc_fk(transformed)
        rejected_no_fk = transformed[transformed["id_uc"].isna()].copy()
        transformed = transformed[~transformed["id_uc"].isna()].copy()
        skipped_no_fk = int(len(rejected_no_fk))
        rejected_frames: dict[str, pd.DataFrame] = {}
        if not rejected_no_fk.empty:
            rejected_frames["NO_UC_FK_MATCH"] = rejected_no_fk

        if transformed.empty:
            skipped_records = skipped_no_fk
            self._write_quality_report(
                context=context,
                branch="buffer",
                stage="load_postgres_buffer",
                source_records=source_records,
                inserted_records=0,
                inserted_new_records=0,
                inserted_updated_records=0,
                skipped_records=skipped_records,
                result_reason="no_uc_fk_match",
                rejected_frames=rejected_frames,
            )
            return self._result(
                context,
                status="loaded_postgres",
                branch="buffer",
                inserted_records=0,
                skipped_records=skipped_no_fk,
                reason="no_uc_fk_match",
            )

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                uc_ids = transformed["id_uc"].astype(int).unique().tolist()
                cur.execute(
                    """
                    SELECT DISTINCT id_uc
                    FROM za_oficial
                    WHERE id_uc = ANY(%s) AND fl_ativa = TRUE;
                    """,
                    (uc_ids,),
                )
                uc_with_official_za = {int(row[0]) for row in cur.fetchall()}
        self._assert_postgres_does_not_override_bronze(set(uc_ids), uc_with_official_za)
        # Eligibility was already decided from the Bronze UC/ZA union in transform_buffer.
        # Postgres is only a projection-consistency check here; it must not filter rows.
        skipped_official_za = 0
        deactivated_by_official_za = 0

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE buffer_abrangencia
                    ADD COLUMN IF NOT EXISTS update_geom TEXT;
                    """
                )

                candidate_uc_ids = transformed["id_uc"].dropna().astype(int).unique().tolist()
                existing_uc_ids: set[int] = set()
                current_versions: dict[int, int] = {}
                if candidate_uc_ids:
                    cur.execute(
                        """
                        SELECT id_uc, MAX(numero_versao)
                        FROM buffer_abrangencia
                        WHERE id_uc = ANY(%s)
                        GROUP BY id_uc;
                        """,
                        (candidate_uc_ids,),
                    )
                    current_versions = {
                        int(db_row[0]): int(db_row[1] or 0)
                        for db_row in cur.fetchall()
                        if db_row[0] is not None
                    }
                    existing_uc_ids = set(current_versions)

                active_geometries: dict[int, tuple[BaseGeometry, float]] = {}
                if candidate_uc_ids:
                    cur.execute(
                        """
                        SELECT id_uc, ST_AsBinary(geom), dist_buffer_m
                        FROM buffer_abrangencia
                        WHERE id_uc = ANY(%s) AND fl_ativa = TRUE;
                        """,
                        (candidate_uc_ids,),
                    )
                    active_geometries = {
                        int(db_row[0]): (wkb.loads(bytes(db_row[1])), float(db_row[2]))
                        for db_row in cur.fetchall()
                        if db_row[1] is not None
                    }

                # Buffer de Abrangência is a deterministic derivative of the current UC and official-ZA Bronze union.
                # A new version is recorded only when the derived ring differs from the active one;
                # rerunning the scheduled DAG over unchanged sources must not grow the versioned history.
                geom_name = str(transformed.geometry.name)
                should_insert = pd.Series(
                    [
                        not self._matches_active_buffer(active_geometries.get(int(row["id_uc"])), row[geom_name])
                        for _, row in transformed.iterrows()
                    ],
                    index=transformed.index,
                    dtype=bool,
                )

                unchanged = transformed[~should_insert].copy()
                if not unchanged.empty:
                    rejected_frames["ACTIVE_BUFFER_UNCHANGED"] = unchanged

                transformed_to_insert = transformed[should_insert].copy()
                skipped_unchanged = int(len(unchanged))
                skipped_records = int(skipped_no_fk + skipped_official_za + skipped_unchanged)

                if transformed_to_insert.empty:
                    self._write_quality_report(
                        context=context,
                        branch="buffer",
                        stage="load_postgres_buffer",
                        source_records=source_records,
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        result_reason="active_buffer_unchanged",
                        rejected_frames=rejected_frames,
                    )
                    return self._result(
                        context,
                        status="loaded_postgres",
                        branch="buffer",
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        reason="active_buffer_unchanged",
                    )

                rows_existing_to_update = transformed_to_insert[
                    transformed_to_insert["id_uc"].astype(int).isin(existing_uc_ids)
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
                        branch="buffer",
                        stage="load_postgres_buffer",
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
                        branch="buffer",
                        inserted_records=0,
                        inserted_new_records=0,
                        inserted_updated_records=0,
                        skipped_records=skipped_records,
                        reason="no_valid_geometry_for_insert",
                    )

                rows_existing_to_update = transformed_to_insert[
                    transformed_to_insert["id_uc"].astype(int).isin(existing_uc_ids)
                ]
                uc_ids_to_refresh = rows_existing_to_update["id_uc"].dropna().astype(int).unique().tolist()
                inserted_updated_records = int(
                    len(rows_existing_to_update)
                )
                inserted_new_records = int(len(transformed_to_insert) - inserted_updated_records)

                manifest = state.get("manifest") or {}
                metadata = manifest.get("metadata") or {}
                rows = [
                    (
                        int(row["id_uc"]),
                        self._none_if_nan(row.get("ds_fonte")),
                        datetime.now(timezone.utc),
                        BUFFER_DISTANCE_M,
                        self._none_if_nan(row.get("update_geom")),
                        True,
                        row[geom_column].wkt if row.get(geom_column) is not None else None,
                        state["dt_bronze"],
                        state.get("dt_silver"),
                        state.get("dt_gold"),
                        context.dag_id,
                        current_versions.get(int(row["id_uc"]), 0) + 1,
                        metadata.get("reason") or "Buffer de Abrangência derivado da geometria vigente da UC",
                        manifest.get("actor") or metadata.get("actor") or "airflow",
                        manifest.get("correlation_id") or context.run_id,
                        manifest.get("import_id"),
                        context.run_id,
                    )
                    for _, row in transformed_to_insert.iterrows()
                ]

                if uc_ids_to_refresh:
                    cur.execute(
                        """
                        UPDATE buffer_abrangencia
                        SET fl_ativa = FALSE, dt_fim_vigencia = CURRENT_DATE
                        WHERE id_uc = ANY(%s) AND fl_ativa = TRUE;
                        """,
                        (uc_ids_to_refresh,),
                    )

                execute_batch(
                    cur,
                    """
                    INSERT INTO buffer_abrangencia (
                        id_uc, ds_fonte, dt_geracao, dist_buffer_m, update_geom, fl_ativa,
                        geom, dt_bronze, dt_silver, dt_gold, versao_dag,
                        numero_versao, motivo, ator, correlation_id, import_id, dag_run_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        ST_SetSRID(ST_GeomFromText(%s), 4674), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    );
                    """,
                    rows,
                    page_size=200,
                )

        self._write_quality_report(
            context=context,
            branch="buffer",
            stage="load_postgres_buffer",
            source_records=source_records,
            inserted_records=len(rows),
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
            result_reason="loaded_postgres",
            rejected_frames=rejected_frames,
        )

        return self._result(
            context,
            status="loaded_postgres",
            branch="buffer",
            inserted_records=len(rows),
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
            deactivated_by_official_za=deactivated_by_official_za,
        )

    @staticmethod
    def _matches_active_buffer(active: tuple[BaseGeometry, float] | None, candidate: Any) -> bool:
        if active is None or candidate is None or candidate.is_empty:
            return False
        active_geometry, active_distance_m = active
        if active_distance_m != BUFFER_DISTANCE_M:
            return False
        return active_geometry.normalize().equals_exact(
            candidate.normalize(), GEOMETRY_EQUALITY_TOLERANCE_DEGREES
        )

    def _apply_manifest_metadata(
        self,
        gdf: gpd.GeoDataFrame,
        state: dict[str, Any],
        *,
        branch: str,
    ) -> gpd.GeoDataFrame:
        manifest = state.get("manifest")
        if not manifest:
            return gdf
        enriched = gdf.copy()
        metadata = manifest.get("metadata", {})
        if branch == "buffer":
            if self._detect_source_column(enriched.columns, ["uc_id", "id_uc", "id", "gid"]) is None:
                enriched["uc_id"] = metadata.get("official_identifier") or manifest.get("import_id")
            if self._detect_source_column(
                enriched.columns, ["nm_uc", "nome_uc", "nome", "label"]
            ) is None:
                enriched["nm_uc"] = metadata.get("name")
            if self._detect_source_column(enriched.columns, ["cd_cnuc", "cod_cnuc"]) is None:
                enriched["cd_cnuc"] = metadata.get("cd_cnuc")
        else:
            if self._detect_source_column(enriched.columns, ["uc_id", "id_uc"]) is None:
                enriched["uc_id"] = metadata.get("uc_identifier")
            if self._detect_source_column(enriched.columns, ["ds_fonte", "obs"]) is None:
                enriched["ds_fonte"] = metadata.get("source")
        operation = manifest.get("operation")
        if operation == "create":
            enriched["update_geom"] = False
        elif operation in {"update", "replace_point", "replace_buffer_abrangencia"}:
            enriched["update_geom"] = True
            if branch == "buffer" and metadata.get("official_identifier"):
                enriched["uc_id"] = metadata["official_identifier"]
        return enriched

    # ===== Shared helpers =====
    def _extract_branch(self, context: TaskExecutionContext, branch: str) -> dict[str, Any]:
        source_root = Path(self.config.medallion_bronze_path)
        requested_domain = (context.conf or {}).get("domain")
        directed_domain = "uc" if branch == "buffer" else "za_oficial"
        if requested_domain and requested_domain != directed_domain:
            state = {
                "branch": branch,
                "available": False,
                "run_id": context.run_id,
                "logical_date": context.logical_date,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            }
            self._save_state(context, branch=branch, state=state)
            return self._result(context, status="skipped", branch=branch, reason="not_requested")

        directed = load_manifest_input(
            source_root, context.conf, expected_domain=directed_domain
        )
        if directed is not None:
            state = {
                "branch": branch,
                "available": True,
                "run_id": context.run_id,
                "logical_date": context.logical_date,
                "dt_bronze": str(directed.payload.get("created_at", context.logical_date)),
                "bronze_batch_dir": str(directed.manifest_path.parent),
                "source_path": str(directed.canonical_path),
                "shapefile_path": str(directed.canonical_path),
                "source_mode": "manifest_geojson",
                "manifest": directed.payload,
                "import_id": directed.import_id,
                "temporal_folder": self._temporal_folder_stamp(context),
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            }
            self._save_state(context, branch=branch, state=state)
            return self._result(
                context,
                status="extracted",
                branch=branch,
                bronze_batch_dir=state["bronze_batch_dir"],
                source_path=state["source_path"],
                source_mode=state["source_mode"],
                dt_bronze=state["dt_bronze"],
            )

        selections = self._list_branch_batches(source_root, branch)

        if not selections:
            state = {
                "branch": branch,
                "available": False,
                "run_id": context.run_id,
                "logical_date": context.logical_date,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            }
            self._save_state(context, branch=branch, state=state)
            return self._result(context, status="skipped", branch=branch, reason="no_bronze_input")

        run_dir = self._run_dir(context)
        run_dir.mkdir(parents=True, exist_ok=True)
        shapefile_path = self._materialize_branch_union(selections, run_dir / branch)
        source_mode = "bronze_union"
        dt_bronze = max(selection.dt_bronze for selection in selections)

        state = {
            "branch": branch,
            "available": True,
            "run_id": context.run_id,
            "logical_date": context.logical_date,
            "dt_bronze": dt_bronze,
            "bronze_batch_dir": str(source_root / branch),
            "shapefile_path": str(shapefile_path),
            "source_path": str(shapefile_path),
            "source_mode": source_mode,
            "temporal_folder": self._temporal_folder_stamp(context),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        self._save_state(context, branch=branch, state=state)

        return self._result(
            context,
            status="extracted",
            branch=branch,
            bronze_batch_dir=str(source_root / branch),
            shapefile_path=str(shapefile_path),
            source_mode=source_mode,
            dt_bronze=dt_bronze,
        )

    def _validate_branch(self, context: TaskExecutionContext, branch: str) -> dict[str, Any]:
        state = self._load_state(context, branch=branch)
        if not state.get("available", False):
            return self._result(context, status="skipped", branch=branch, reason="no_bronze_input")

        source_path = Path(state.get("source_path", state["shapefile_path"]))
        if source_path.suffix.lower() == ".shp":
            self._validate_shapefile_components(source_path)

        gdf = self._apply_manifest_metadata(
            self._read_geodata(source_path), state, branch=branch
        )
        if gdf.empty:
            raise ZaBufferPipelineError(f"{branch.upper()} shapefile is empty.")
        if gdf.crs is None:
            raise ZaBufferPipelineError(f"{branch.upper()} shapefile has no CRS.")

        validation = {
            "records": int(len(gdf)),
            "crs": str(gdf.crs),
            "geom_null": int(gdf.geometry.isna().sum()),
            "geom_empty": int(gdf.geometry.is_empty.sum()),
        }
        state["validation"] = validation
        self._save_state(context, branch=branch, state=state)

        return self._result(context, status="validated", branch=branch, **validation)

    def _load_silver_branch(self, context: TaskExecutionContext, branch: str, layer_name: str, file_name: str) -> dict[str, Any]:
        state = self._load_state(context, branch=branch)
        if not state.get("available", False) or not state.get("transformed_path"):
            return self._result(context, status="skipped", branch=branch, reason="no_transformed_input")

        transformed = self._read_geodata(Path(state["transformed_path"]), layer=layer_name)
        silver_dir = self._build_layer_output_dir(Path(self.config.medallion_silver_path), branch, state["temporal_folder"])
        silver_path = silver_dir / file_name
        transformed.to_file(silver_path, layer=layer_name, driver="GPKG")

        dt_silver = datetime.now(timezone.utc).isoformat()
        state["silver_path"] = str(silver_path)
        state["dt_silver"] = dt_silver
        self._save_state(context, branch=branch, state=state)

        (silver_dir / "metadata.json").write_text(
            json.dumps(
                {
                    "branch": branch,
                    "run_id": context.run_id,
                    "records": int(len(transformed)),
                    "dt_bronze": state.get("dt_bronze"),
                    "dt_silver": dt_silver,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        return self._result(context, status="loaded_silver", branch=branch, silver_path=str(silver_path), dt_silver=dt_silver)

    def _load_gold_branch(self, context: TaskExecutionContext, branch: str, layer_name: str, file_geojson: str) -> dict[str, Any]:
        state = self._load_state(context, branch=branch)
        if not state.get("available", False) or not state.get("transformed_path"):
            return self._result(context, status="skipped", branch=branch, reason="no_transformed_input")

        transformed = self._read_geodata(Path(state["transformed_path"]), layer=layer_name)
        gold_dir = self._build_layer_output_dir(Path(self.config.medallion_gold_path), branch, state["temporal_folder"])

        geojson_path = gold_dir / file_geojson
        transformed.to_file(geojson_path, driver="GeoJSON")

        attributes = transformed.drop(columns=[transformed.geometry.name]).copy()
        attributes.to_csv(gold_dir / file_geojson.replace(".geojson", "_attributes.csv"), index=False, encoding="utf-8")

        dt_gold = datetime.now(timezone.utc).isoformat()
        state["gold_geojson"] = str(geojson_path)
        state["dt_gold"] = dt_gold
        self._save_state(context, branch=branch, state=state)

        (gold_dir / "metadata.json").write_text(
            json.dumps(
                {
                    "branch": branch,
                    "run_id": context.run_id,
                    "records": int(len(transformed)),
                    "dt_bronze": state.get("dt_bronze"),
                    "dt_silver": state.get("dt_silver"),
                    "dt_gold": dt_gold,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        return self._result(context, status="loaded_gold", branch=branch, gold_geojson=str(geojson_path), dt_gold=dt_gold)

    def _select_branch_batch(self, source_root: Path, branch: str) -> BranchSelection | None:
        selections = self._list_branch_batches(source_root, branch)
        return selections[-1] if selections else None

    def _list_branch_batches(self, source_root: Path, branch: str) -> list[BranchSelection]:
        """Return every canonical Bronze batch for a branch in deterministic order."""
        candidates: list[tuple[datetime, int, Path]] = []

        aggregate_root = source_root / "uc_za_batches"
        if aggregate_root.is_dir() and self.config.project_db_url:
            with psycopg2.connect(self.config.project_db_url) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT new_state->>'import_id' FROM cadastral_event "
                    "WHERE event_type = 'UC_CREATED_WITH_OFFICIAL_ZA'"
                )
                committed = {row[0] for row in cur.fetchall()}
            for batch in aggregate_root.iterdir():
                if batch.name.removeprefix("batch_id=") not in committed:
                    continue
                member = batch / branch
                if (member / "manifest.json").is_file():
                    payload = json.loads((member / "manifest.json").read_text(encoding="utf-8"))
                    candidates.append((datetime.fromisoformat(payload["created_at"].replace("Z", "+00:00")), 2, member))

        branch_root = source_root / branch
        if branch_root.exists() and branch_root.is_dir():
            for child in branch_root.iterdir():
                if not child.is_dir():
                    continue
                parsed = self._try_parse_temporal_folder(child.name)
                if parsed is not None:
                    candidates.append((parsed, 1, child))
                    continue
                manifest_path = child / "manifest.json"
                if manifest_path.is_file() and child.name.startswith("import_id="):
                    try:
                        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                        created_at = datetime.fromisoformat(
                            str(payload["created_at"]).replace("Z", "+00:00")
                        )
                    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                        self.logger.warning("Ignoring invalid directed Bronze manifest: %s", manifest_path)
                    else:
                        candidates.append((created_at, 2, child))

        for child in source_root.iterdir():
            if not child.is_dir():
                continue
            parsed = self._try_parse_temporal_folder(child.name)
            if parsed is not None:
                legacy_branch_dir = child / branch
                if legacy_branch_dir.exists() and legacy_branch_dir.is_dir():
                    candidates.append((parsed, 0, legacy_branch_dir))

        candidates.sort(key=lambda item: (item[0], item[1]))
        return [
            BranchSelection(batch_dir=batch_dir, dt_bronze=batch_dt.date().isoformat())
            for batch_dt, _, batch_dir in candidates
        ]

    def _materialize_branch_union(self, selections: list[BranchSelection], target_dir: Path) -> Path:
        """Materialize a normalized union of all canonical Bronze batches for one branch."""
        frames: list[gpd.GeoDataFrame] = []
        for selection in selections:
            manifest_path = selection.batch_dir / "manifest.json"
            if manifest_path.is_file():
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                canonical_key = str((payload.get("bronze") or {}).get("canonical_key") or "")
                bronze_root = Path(self.config.medallion_bronze_path).resolve()
                source_path = (bronze_root / canonical_key).resolve()
                try:
                    source_path.relative_to(bronze_root)
                except ValueError as exc:
                    raise ZaBufferPipelineError("Directed canonical path escapes the Bronze root.") from exc
                if not canonical_key or not source_path.is_file():
                    raise ZaBufferPipelineError(f"Directed canonical file is unavailable: {manifest_path}")
            else:
                source_path, _ = self._locate_shapefile(
                    selection.batch_dir, target_dir / selection.batch_dir.name
                )
            frame = self._ensure_epsg_4674(self._read_geodata(source_path))
            if manifest_path.is_file():
                frame = self._apply_manifest_metadata(
                    frame, {"manifest": payload},
                    branch="buffer" if payload.get("domain") == "uc" else "za",
                )
                if payload.get("domain") == "za_oficial":
                    frame["_manifest_uc_identifier"] = payload.get("metadata", {}).get("uc_identifier")
            frame = frame.drop(columns=["fid"], errors="ignore")
            frame["_bronze_dt"] = pd.to_datetime(selection.dt_bronze)
            frames.append(frame)
        if not frames:
            raise ZaBufferPipelineError("No canonical Bronze batches are available for the branch union.")
        target_dir.mkdir(parents=True, exist_ok=True)
        union = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs="EPSG:4674")
        union["_bronze_union_index"] = union.index.astype(int)
        union_path = target_dir / "bronze_union.gpkg"
        union.to_file(union_path, layer="bronze_union", driver="GPKG")
        return union_path

    def _run_dir(self, context: TaskExecutionContext) -> Path:
        safe_run_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", context.run_id)
        return Path(self.config.medallion_tmp_path) / self.domain_name / safe_run_id

    def _quality_stage_dir(self, context: TaskExecutionContext, branch: str, stage: str) -> Path:
        safe_run_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", context.run_id)
        quality_root = Path(self.config.medallion_tmp_path).resolve().parent / QUALITY_FOLDER_NAME
        target = (
            quality_root
            / "rejections"
            / self.domain_name
            / f"run_id={safe_run_id}"
            / f"branch={branch}"
            / f"stage={stage}"
        )
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _write_quality_report(
        self,
        context: TaskExecutionContext,
        branch: str,
        stage: str,
        source_records: int,
        inserted_records: int,
        inserted_new_records: int,
        inserted_updated_records: int,
        skipped_records: int,
        result_reason: str,
        rejected_frames: dict[str, pd.DataFrame],
    ) -> None:
        target = self._quality_stage_dir(context, branch=branch, stage=stage)
        generated_at = datetime.now(timezone.utc).isoformat()

        rejection_counts = {
            reason: int(len(frame))
            for reason, frame in rejected_frames.items()
            if frame is not None and not frame.empty
        }
        rejection_reason_details = self._build_rejection_reason_details(rejection_counts)

        summary = {
            "domain": self.domain_name,
            "branch": branch,
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

    def _state_path(self, context: TaskExecutionContext, branch: str) -> Path:
        return self._run_dir(context) / f"state_{branch}.json"

    def _save_state(self, context: TaskExecutionContext, branch: str, state: dict[str, Any]) -> None:
        path = self._state_path(context, branch)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _load_state(self, context: TaskExecutionContext, branch: str) -> dict[str, Any]:
        path = self._state_path(context, branch)
        if not path.exists():
            raise ZaBufferPipelineError(f"Missing state for branch '{branch}': {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _temporal_folder_stamp(self, context: TaskExecutionContext) -> str:
        logical = datetime.fromisoformat(context.logical_date.replace("Z", "+00:00"))
        return logical.astimezone(timezone.utc).strftime(TEMPORAL_FOLDER_FORMAT)

    def _build_layer_output_dir(self, layer_root: Path, branch: str, temporal_folder: str) -> Path:
        target = layer_root / branch / temporal_folder
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _try_parse_temporal_folder(self, folder_name: str) -> datetime | None:
        for fmt in (TEMPORAL_FOLDER_FORMAT, LEGACY_TEMPORAL_FOLDER_FORMAT):
            try:
                return datetime.strptime(folder_name, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def _locate_shapefile(self, batch_dir: Path, run_dir: Path) -> tuple[Path, str]:
        """Locate a supported Bronze vector dataset while preserving its original format."""
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
            raise ZaBufferPipelineError(f"No shapefile, GeoJSON or zip found in {batch_dir}")

        extraction_dir = run_dir / "unzipped"
        extraction_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_files[0]) as zf:
            zf.extractall(extraction_dir)

        shapefile = self._find_shapefile_in_directory(extraction_dir)
        if shapefile is None:
            raise ZaBufferPipelineError(f"No .shp after extracting {zip_files[0]}")

        self._validate_shapefile_components(shapefile)
        return shapefile, "zip"

    def _find_shapefile_in_directory(self, directory: Path) -> Path | None:
        shapefiles = list(directory.rglob("*.shp"))
        if not shapefiles:
            return None
        return sorted(shapefiles, key=lambda p: p.stat().st_mtime, reverse=True)[0]

    def _validate_shapefile_components(self, shp_path: Path) -> None:
        missing = [ext for ext in REQUIRED_SHAPEFILE_EXTENSIONS if not shp_path.with_suffix(ext).exists()]
        if missing:
            raise ZaBufferPipelineError(f"Missing sidecar files for {shp_path.name}: {missing}")

    def _read_geodata(self, path: Path, layer: str | None = None) -> gpd.GeoDataFrame:
        open_kwargs: dict[str, Any] = {}
        if layer is not None:
            open_kwargs["layer"] = layer

        with fiona.open(str(path), **open_kwargs) as src:
            features = list(src)
            if not features:
                return gpd.GeoDataFrame(geometry=[], crs=src.crs)
            return gpd.GeoDataFrame.from_features(features, crs=src.crs)

    def _ensure_epsg_4674(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        source_epsg = gdf.crs.to_epsg() if gdf.crs else None
        if source_epsg != 4674:
            return gdf.to_crs(epsg=4674)
        return gdf

    def _repair_geometries(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        repaired = gdf.copy()
        repaired.geometry = repaired.geometry.apply(self._make_valid_geometry)
        return repaired

    def _make_valid_geometry(self, geom):
        if geom is None:
            return None
        if geom.is_valid:
            return geom
        try:
            return geom.make_valid()
        except Exception:
            return geom.buffer(0)

    def _normalize_polygon_to_multipolygon(self, geom):
        if geom is None:
            return None
        if isinstance(geom, Polygon):
            return MultiPolygon([geom])
        return geom

    def _resolve_uc_fk(
        self,
        transformed: gpd.GeoDataFrame,
        *,
        allow_spatial: bool = True,
        require_authoritative_source_for_spatial: bool = False,
    ) -> pd.Series:
        if not self.config.project_db_url:
            raise ZaBufferPipelineError("PROJECT_DB_URL is required for PostgreSQL loads.")

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_uc, uc_id, cd_cnuc, nm_uc FROM uc WHERE situacao = 'ATIVA'")
                rows = cur.fetchall()

        by_uc_id: dict[str, int] = {}
        by_cd_cnuc: dict[str, int] = {}
        by_name: dict[str, int] = {}

        for id_uc, uc_id, cd_cnuc, nm_uc in rows:
            if uc_id is not None:
                by_uc_id[str(uc_id).strip()] = int(id_uc)
            if cd_cnuc is not None:
                by_cd_cnuc[str(cd_cnuc).strip()] = int(id_uc)
            if nm_uc is not None:
                by_name[self._normalize_name(str(nm_uc))] = int(id_uc)

        resolved: list[int | None] = []
        unresolved_for_spatial: list[tuple[int, str]] = []
        geom_column = str(transformed.geometry.name)
        for _, row in transformed.iterrows():
            uc_id_src = self._none_if_nan(row.get("uc_id_source"))
            cd_cnuc_src = self._none_if_nan(row.get("cd_cnuc_source"))
            nm_src = self._none_if_nan(row.get("nm_uc_source"))

            id_uc = None
            if uc_id_src is not None:
                id_uc = by_uc_id.get(str(uc_id_src).strip())
            if id_uc is None and cd_cnuc_src is not None:
                id_uc = by_cd_cnuc.get(str(cd_cnuc_src).strip())
            if id_uc is None and nm_src is not None:
                id_uc = by_name.get(self._normalize_name(str(nm_src)))

            resolved.append(id_uc)

            authoritative_source = self._none_if_nan(row.get("ds_fonte"))
            spatial_allowed_for_row = allow_spatial and (
                not require_authoritative_source_for_spatial or authoritative_source is not None
            )
            if id_uc is None and spatial_allowed_for_row:
                geom = row.get(geom_column)
                if geom is not None and not pd.isna(geom) and not geom.is_empty:
                    unresolved_for_spatial.append((len(resolved) - 1, geom.wkt))

        if unresolved_for_spatial:
            spatial_matches = self._resolve_uc_fk_spatial(unresolved_for_spatial)
            for idx, id_uc in spatial_matches.items():
                resolved[idx] = id_uc

        return pd.Series(resolved, index=transformed.index)

    def _resolve_uc_fk_spatial(self, unresolved_for_spatial: list[tuple[int, str]]) -> dict[int, int]:
        if not unresolved_for_spatial:
            return {}

        matches: dict[int, int] = {}
        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                for idx, geom_wkt in unresolved_for_spatial:
                    cur.execute(
                        """
                        WITH candidate AS (
                            SELECT ST_SetSRID(ST_GeomFromText(%s), 4674) AS geom
                        )
                        SELECT uc.id_uc
                        FROM uc
                        CROSS JOIN candidate
                        WHERE uc.situacao = 'ATIVA' AND ST_Intersects(uc.geom, candidate.geom)
                        ORDER BY ST_Area(ST_Intersection(ST_Multi(uc.geom), ST_Multi(candidate.geom))) DESC
                        LIMIT 1;
                        """,
                        (geom_wkt,),
                    )
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        matches[idx] = int(row[0])

        return matches

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

    def _to_int_series(self, value: pd.Series | None) -> pd.Series | None:
        if value is None:
            return None
        return pd.to_numeric(value, errors="coerce").astype("Int64")

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

    def _normalize_name(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    def _none_if_nan(self, value: Any) -> Any:
        if value is None:
            return None
        if pd.isna(value):
            return None
        return value
