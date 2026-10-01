"""Pipeline service for MapBiomas Alerta clipping with UC/ZA/Buffer de Abrangência rules.

Flow:
extract -> validate -> (transform_uc || transform_zone) -> transform_merge -> load_postgres -> load_silver -> load_gold

Operational decisions for this domain:
- Filter source rows as early as possible by ESTADO='SANTA CATARINA'.
- Process source and transformed layers in chunks to reduce RAM usage.
- Spatial routing per UC:
  - if UC has active ZA, intersect only against ZA
  - otherwise, intersect only against Buffer de Abrangência
- Emit quality artifacts with reason codes and reason descriptions.
"""

from __future__ import annotations

import json
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import fiona
import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from shapely import wkb
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import DomainPipelineService, TaskExecutionContext
from scripts_python.mapbiomas_alerta_acquisition import MapbiomasAlertaAcquisitionService

LOGGER = logging.getLogger("pipeline.mapbiomas_alerta")

TEMPORAL_FOLDER_FORMAT = "%Y-%m-%d-%H-%M-%S"
LEGACY_TEMPORAL_FOLDER_FORMAT = "%Y-%m-%d-%S-%M-%H"
REQUIRED_SHAPEFILE_EXTENSIONS = (".shp", ".shx", ".dbf")
QUALITY_FOLDER_NAME = "quality"

SOURCE_FIELDS = ("FONTE", "BIOMA", "ESTADO", "MUNICIPIO", "AREAHA", "ANODETEC", "VPRESSAO")
ALERT_CODE_SOURCE_FIELDS = ("CODEALERTA", "codealerta", "CODE_ALERTA", "COD_ALERTA")
MAPBIOMAS_ALERTA_LAYER = "mapbiomas_alerta_stage"
MAPBIOMAS_ALERTA_LAYER_UC = "mapbiomas_alerta_transform_uc"
MAPBIOMAS_ALERTA_LAYER_ZONE = "mapbiomas_alerta_transform_zone"
TARGET_STATES_NORMALIZED = {
    "SC", "SANTACATARINA",
    "PR", "PARANA",
    "RS", "RIOGRANDEDOSUL",
}
CHUNK_SIZE_SOURCE = 3000
CHUNK_SIZE_POSTGRES = 2000
REJECTION_SAMPLE_LIMIT = 2000
TIPO_CRUZAMENTO_UC = "UC"
TIPO_CRUZAMENTO_ZA = "ZA"
TIPO_CRUZAMENTO_BUFFER_ABRANGENCIA = "BUFFER_ABRANGENCIA"

RESULT_REASON_DESCRIPTIONS = {
    "loaded_postgres": "Carga no Postgres concluida com sucesso.",
    "no_transformed_input": "Nao ha dataset transformado disponivel para carga.",
    "no_eligible_rows_after_spatial_rules": "Nenhum registro elegivel apos regras espaciais UC/ZA/Buffer de Abrangência.",
    "no_valid_geometry_for_insert": "Registros elegiveis foram rejeitados por geometria nula ou vazia.",
}

REJECTION_REASON_DESCRIPTIONS = {
    "NO_UC_INTERSECTION": "Registro nao intersecta nenhuma UC.",
    "NO_ZONE_INTERSECTION": "Registro nao intersecta zona de entorno esperada (ZA ou Buffer de Abrangência).",
    "NO_ACTIVE_ZONE_FOR_UC": "UC sem zona ativa correspondente ao ramo aplicado.",
    "INVALID_OR_EMPTY_GEOMETRY": "Registro com geometria nula ou vazia apos transformacao.",
    "DUPLICATE_ALERT_ZONE_KEY": "Duplicado no lote por chave de alerta/uc/entorno.",
    "MISSING_ALERT_CODE": "Registro sem CODEALERTA valido na origem.",
    "DUPLICATE_ALERT_CODE_IN_BATCH": "Duplicado no lote por CODEALERTA.",
    "DUPLICATE_ALERT_CODE_IN_DB": "CODEALERTA ja existe na tabela de destino.",
}


@dataclass(frozen=True)
class BronzeSelection:
    """Selected bronze batch directory and inferred bronze date."""

    batch_dir: Path
    dt_bronze: str


class MapbiomasAlertaPipelineError(RuntimeError):
    """Raised for operational errors in the MapBiomas Alerta pipeline."""


class InputValidationError(MapbiomasAlertaPipelineError):
    """Raised when source payload does not satisfy expected schema/contract."""


class MapbiomasAlertaPipelineService(DomainPipelineService):
    """MapBiomas Alerta implementation with chunked geospatial processing."""

    def __init__(self, config: PipelineConfig | None = None) -> None:
        super().__init__(domain_name="mapbiomas_alerta", config=config)
        self.logger = LOGGER

    def acquire_snapshot(self, context: TaskExecutionContext) -> bool:
        return MapbiomasAlertaAcquisitionService(self.config).acquire(context)

    def extract(self, context: TaskExecutionContext) -> dict[str, Any]:
        source_root = Path(self.config.medallion_bronze_path)
        if not source_root.exists():
            raise MapbiomasAlertaPipelineError(
                f"Bronze root does not exist for {self.domain_name}: {source_root}"
            )

        bronze = self._select_bronze_batch(source_root)
        run_dir = self._run_dir(context)
        run_dir.mkdir(parents=True, exist_ok=True)

        shapefile_path, source_mode = self._locate_shapefile(bronze.batch_dir, run_dir)

        state = {
            "domain": self.domain_name,
            "run_id": context.run_id,
            "dag_id": context.dag_id,
            "logical_date": context.logical_date,
            "temporal_folder": self._temporal_folder_stamp(context),
            "bronze_batch_dir": str(bronze.batch_dir),
            "bronze_mode": source_mode,
            "shapefile_path": str(shapefile_path),
            "dt_bronze": bronze.dt_bronze,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        self._save_state(context, state)

        self.logger.info(
            "MapBiomas Alerta extract completed. mode=%s batch=%s shapefile=%s dt_bronze=%s",
            source_mode,
            bronze.batch_dir,
            shapefile_path,
            bronze.dt_bronze,
        )
        return self._result(
            context,
            status="extracted",
            bronze_batch_dir=str(bronze.batch_dir),
            shapefile_path=str(shapefile_path),
            dt_bronze=bronze.dt_bronze,
            source_mode=source_mode,
            temporal_folder=state["temporal_folder"],
        )

    def validate(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        shapefile_path = Path(state["shapefile_path"])
        self._validate_shapefile_components(shapefile_path)

        with fiona.open(str(shapefile_path)) as src:
            columns = list((src.schema or {}).get("properties", {}).keys())
            missing = [field for field in SOURCE_FIELDS if self._detect_source_column(columns, [field]) is None]
            if missing:
                raise InputValidationError(
                    f"Missing required source fields in {shapefile_path.name}: {missing}"
                )

            alert_code_field = self._detect_source_column(columns, list(ALERT_CODE_SOURCE_FIELDS))
            if alert_code_field is None:
                raise InputValidationError(
                    "Field CODEALERTA is required for deterministic de-duplication in Postgres load."
                )

            if src.crs is None:
                raise InputValidationError("Source shapefile has no CRS metadata.")

        state["validated_at_utc"] = datetime.now(timezone.utc).isoformat()
        state["source_alert_code_field"] = alert_code_field
        self._save_state(context, state)

        return self._result(
            context,
            status="validated",
            validated_source_fields=list(SOURCE_FIELDS),
            shapefile_path=str(shapefile_path),
        )

    def transform_uc(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        shapefile_path = Path(state["shapefile_path"])

        uc_gdf, _, _ = self._load_reference_layers()
        if uc_gdf.empty:
            raise MapbiomasAlertaPipelineError("UC table is empty. Execute DAG_UCS before this DAG.")

        transformed_uc_path = self._run_dir(context) / "mapbiomas_alerta_transform_uc.gpkg"
        if transformed_uc_path.exists():
            transformed_uc_path.unlink()

        source_records_sc = 0
        transformed_records = 0
        invalid_geometry_records = 0
        alert_offset = 0
        wrote_layer = False

        for source_chunk in self._iter_sc_chunks(shapefile_path, chunk_size=CHUNK_SIZE_SOURCE):
            if source_chunk.empty:
                continue

            source_records_sc += int(len(source_chunk))
            source_chunk = self._ensure_epsg_4674(source_chunk)
            source_chunk = self._repair_geometries(source_chunk)

            mapped = self._map_to_target_schema(
                source_chunk,
                dt_bronze=state["dt_bronze"],
                dag_version=context.dag_id,
                row_offset=alert_offset,
            )
            alert_offset += int(len(mapped))

            valid_uc_chunk, invalid_count = self._extract_uc_intersections_chunk(mapped, uc_gdf)
            invalid_geometry_records += int(invalid_count)

            if valid_uc_chunk.empty:
                continue

            wrote_layer = self._append_gpkg_layer(
                chunk=valid_uc_chunk,
                path=transformed_uc_path,
                layer=MAPBIOMAS_ALERTA_LAYER_UC,
                wrote_layer=wrote_layer,
            )
            transformed_records += int(len(valid_uc_chunk))

        self._save_branch_state(
            context,
            branch="transform_uc",
            state={
                "source_records_sc": int(source_records_sc),
                "transformed_records": int(transformed_records),
                "invalid_geometry_records": int(invalid_geometry_records),
                "transformed_uc_path": str(transformed_uc_path) if wrote_layer else None,
            },
        )

        return self._result(
            context,
            status="transformed_uc",
            source_records_sc=int(source_records_sc),
            transformed_records=int(transformed_records),
            transformed_uc_path=str(transformed_uc_path) if wrote_layer else None,
        )

    def transform_zone(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        shapefile_path = Path(state["shapefile_path"])

        _, za_gdf, buffer_gdf = self._load_reference_layers()
        uc_with_active_za = set(za_gdf["id_uc"].astype(int).tolist()) if not za_gdf.empty else set()

        transformed_zone_path = self._run_dir(context) / "mapbiomas_alerta_transform_zone.gpkg"
        if transformed_zone_path.exists():
            transformed_zone_path.unlink()

        source_records_sc = 0
        transformed_records = 0
        invalid_geometry_records = 0
        alert_offset = 0
        wrote_layer = False

        for source_chunk in self._iter_sc_chunks(shapefile_path, chunk_size=CHUNK_SIZE_SOURCE):
            if source_chunk.empty:
                continue

            source_records_sc += int(len(source_chunk))
            source_chunk = self._ensure_epsg_4674(source_chunk)
            source_chunk = self._repair_geometries(source_chunk)

            mapped = self._map_to_target_schema(
                source_chunk,
                dt_bronze=state["dt_bronze"],
                dag_version=context.dag_id,
                row_offset=alert_offset,
            )
            alert_offset += int(len(mapped))

            valid_zone_chunk, invalid_count = self._extract_zone_intersections_chunk(
                mapped=mapped,
                za_gdf=za_gdf,
                buffer_gdf=buffer_gdf,
                uc_with_active_za=uc_with_active_za,
            )
            invalid_geometry_records += int(invalid_count)

            if valid_zone_chunk.empty:
                continue

            wrote_layer = self._append_gpkg_layer(
                chunk=valid_zone_chunk,
                path=transformed_zone_path,
                layer=MAPBIOMAS_ALERTA_LAYER_ZONE,
                wrote_layer=wrote_layer,
            )
            transformed_records += int(len(valid_zone_chunk))

        self._save_branch_state(
            context,
            branch="transform_zone",
            state={
                "source_records_sc": int(source_records_sc),
                "transformed_records": int(transformed_records),
                "invalid_geometry_records": int(invalid_geometry_records),
                "transformed_zone_path": str(transformed_zone_path) if wrote_layer else None,
            },
        )

        return self._result(
            context,
            status="transformed_zone",
            source_records_sc=int(source_records_sc),
            transformed_records=int(transformed_records),
            transformed_zone_path=str(transformed_zone_path) if wrote_layer else None,
        )

    def transform_merge(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        uc_state = self._load_branch_state(context, branch="transform_uc")
        zone_state = self._load_branch_state(context, branch="transform_zone")

        transformed_uc_path_raw = uc_state.get("transformed_uc_path")
        transformed_zone_path_raw = zone_state.get("transformed_zone_path")
        transformed_uc_path = Path(transformed_uc_path_raw) if transformed_uc_path_raw else None
        transformed_zone_path = Path(transformed_zone_path_raw) if transformed_zone_path_raw else None

        transformed_path = self._run_dir(context) / "mapbiomas_alerta_transformed.gpkg"
        if transformed_path.exists():
            transformed_path.unlink()

        wrote_layer = False
        transformed_records = 0
        matched_alert_rows: set[int] = set()
        zone_keys: set[tuple[int, int]] = set()

        if transformed_zone_path is not None and transformed_zone_path.exists():
            for zone_chunk in self._iter_layer_chunks(
                transformed_zone_path,
                layer=MAPBIOMAS_ALERTA_LAYER_ZONE,
                chunk_size=CHUNK_SIZE_SOURCE,
            ):
                if zone_chunk.empty:
                    continue

                zone_chunk["alert_row_id"] = pd.to_numeric(zone_chunk["alert_row_id"], errors="coerce").astype("Int64")
                zone_chunk["id_uc"] = pd.to_numeric(zone_chunk["id_uc"], errors="coerce").astype("Int64")

                for alert_id, uc_id in (
                    zone_chunk[["alert_row_id", "id_uc"]]
                    .dropna()
                    .astype({"alert_row_id": "int64", "id_uc": "int64"})
                    .itertuples(index=False, name=None)
                ):
                    zone_keys.add((alert_id, uc_id))

                matched_alert_rows.update(
                    int(value)
                    for value in zone_chunk["alert_row_id"].dropna().astype(int).tolist()
                )

                wrote_layer = self._append_gpkg_layer(
                    chunk=zone_chunk,
                    path=transformed_path,
                    layer=MAPBIOMAS_ALERTA_LAYER,
                    wrote_layer=wrote_layer,
                )
                transformed_records += int(len(zone_chunk))

        if transformed_uc_path is not None and transformed_uc_path.exists():
            for uc_chunk in self._iter_layer_chunks(
                transformed_uc_path,
                layer=MAPBIOMAS_ALERTA_LAYER_UC,
                chunk_size=CHUNK_SIZE_SOURCE,
            ):
                if uc_chunk.empty:
                    continue

                uc_chunk["alert_row_id"] = pd.to_numeric(uc_chunk["alert_row_id"], errors="coerce").astype("Int64")
                uc_chunk["id_uc"] = pd.to_numeric(uc_chunk["id_uc"], errors="coerce").astype("Int64")

                if zone_keys:
                    remove_mask = [
                        (
                            not pd.isna(alert_id)
                            and not pd.isna(uc_id)
                            and (int(alert_id), int(uc_id)) in zone_keys
                        )
                        for alert_id, uc_id in zip(uc_chunk["alert_row_id"], uc_chunk["id_uc"])
                    ]
                    uc_chunk = uc_chunk[[not flag for flag in remove_mask]].copy()

                if uc_chunk.empty:
                    continue

                matched_alert_rows.update(
                    int(value)
                    for value in uc_chunk["alert_row_id"].dropna().astype(int).tolist()
                )

                wrote_layer = self._append_gpkg_layer(
                    chunk=uc_chunk,
                    path=transformed_path,
                    layer=MAPBIOMAS_ALERTA_LAYER,
                    wrote_layer=wrote_layer,
                )
                transformed_records += int(len(uc_chunk))

        source_records_sc = int(
            max(
                int(uc_state.get("source_records_sc", 0)),
                int(zone_state.get("source_records_sc", 0)),
            )
        )
        invalid_geometry_records = int(
            max(
                int(uc_state.get("invalid_geometry_records", 0)),
                int(zone_state.get("invalid_geometry_records", 0)),
            )
        )

        rejection_counts: dict[str, int] = {}
        unmatched_records = max(source_records_sc - len(matched_alert_rows), 0)
        if unmatched_records > 0:
            rejection_counts["NO_UC_INTERSECTION"] = int(unmatched_records)
        if invalid_geometry_records > 0:
            rejection_counts["INVALID_OR_EMPTY_GEOMETRY"] = int(invalid_geometry_records)

        state["source_records_sc"] = int(source_records_sc)
        state["transformed_records"] = int(transformed_records)
        state["transform_rejection_counts"] = rejection_counts
        state["transformed_path"] = str(transformed_path) if wrote_layer else None
        self._save_state(context, state)

        self.logger.info(
            "MapBiomas Alerta transform merge completed. source_records_sc=%s transformed_records=%s rejections=%s",
            source_records_sc,
            transformed_records,
            rejection_counts,
        )
        return self._result(
            context,
            status="transformed",
            source_records_sc=int(source_records_sc),
            transformed_records=int(transformed_records),
            transformed_path=str(transformed_path) if wrote_layer else None,
            rejection_counts=rejection_counts,
        )

    def transform(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        shapefile_path = Path(state["shapefile_path"])

        uc_gdf, za_gdf, buffer_gdf = self._load_reference_layers()
        if uc_gdf.empty:
            raise MapbiomasAlertaPipelineError("UC table is empty. Execute DAG_UCS before this DAG.")

        uc_with_active_za = set(za_gdf["id_uc"].astype(int).tolist()) if not za_gdf.empty else set()

        transformed_path = self._run_dir(context) / "mapbiomas_alerta_transformed.gpkg"
        if transformed_path.exists():
            transformed_path.unlink()

        source_records_sc = 0
        transformed_records = 0
        alert_offset = 0
        wrote_layer = False

        rejection_counts: dict[str, int] = {}
        rejection_samples: dict[str, pd.DataFrame] = {}

        for source_chunk in self._iter_sc_chunks(shapefile_path, chunk_size=CHUNK_SIZE_SOURCE):
            if source_chunk.empty:
                continue

            source_records_sc += int(len(source_chunk))
            source_chunk = self._ensure_epsg_4674(source_chunk)
            source_chunk = self._repair_geometries(source_chunk)

            mapped = self._map_to_target_schema(
                source_chunk,
                dt_bronze=state["dt_bronze"],
                dag_version=context.dag_id,
                row_offset=alert_offset,
            )
            alert_offset += int(len(mapped))

            valid_chunk, rejected_chunk = self._apply_spatial_rules(
                mapped=mapped,
                uc_gdf=uc_gdf,
                za_gdf=za_gdf,
                buffer_gdf=buffer_gdf,
                uc_with_active_za=uc_with_active_za,
            )

            self._merge_rejection_samples_and_counts(
                rejection_samples,
                rejection_counts,
                rejected_chunk,
            )

            if valid_chunk.empty:
                continue

            mode = "w" if not wrote_layer else "a"
            valid_chunk.to_file(
                transformed_path,
                layer=MAPBIOMAS_ALERTA_LAYER,
                driver="GPKG",
                mode=mode,
            )
            wrote_layer = True
            transformed_records += int(len(valid_chunk))

        state["source_records_sc"] = int(source_records_sc)
        state["transformed_records"] = int(transformed_records)
        state["transform_rejection_counts"] = rejection_counts
        state["transformed_path"] = str(transformed_path) if wrote_layer else None
        self._save_state(context, state)

        self.logger.info(
            "MapBiomas Alerta transform completed. source_records_sc=%s transformed_records=%s rejections=%s",
            source_records_sc,
            transformed_records,
            rejection_counts,
        )
        return self._result(
            context,
            status="transformed",
            source_records_sc=int(source_records_sc),
            transformed_records=int(transformed_records),
            transformed_path=str(transformed_path) if wrote_layer else None,
            rejection_counts=rejection_counts,
        )

    def load_postgres(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        transformed_path_raw = state.get("transformed_path")
        transformed_path = Path(transformed_path_raw) if transformed_path_raw else None

        source_records = int(state.get("source_records_sc", 0))
        transformed_records = int(state.get("transformed_records", 0))
        transform_rejection_counts = {
            str(k): int(v)
            for k, v in (state.get("transform_rejection_counts") or {}).items()
        }
        dt_silver = state.get("dt_silver")
        dt_gold = state.get("dt_gold")

        if transformed_path is None or not transformed_path.exists() or transformed_records == 0:
            skipped_records = int(source_records)
            result_reason = "no_transformed_input" if source_records == 0 else "no_eligible_rows_after_spatial_rules"
            self._write_quality_report(
                context=context,
                stage="load_postgres",
                source_records=source_records,
                inserted_records=0,
                inserted_new_records=0,
                inserted_updated_records=0,
                skipped_records=skipped_records,
                result_reason=result_reason,
                rejected_frames={},
                rejection_counts_override=transform_rejection_counts,
            )
            return self._result(
                context,
                status="loaded_postgres",
                inserted_records=0,
                skipped_records=skipped_records,
                reason=result_reason,
            )

        if not self.config.project_db_url:
            raise MapbiomasAlertaPipelineError("PROJECT_DB_URL is required for load_postgres.")

        inserted_records = 0
        inserted_new_records = 0
        inserted_updated_records = 0
        skipped_invalid_geom = 0

        rejected_frames: dict[str, pd.DataFrame] = {}

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                for chunk in self._iter_layer_chunks(
                    transformed_path,
                    layer=MAPBIOMAS_ALERTA_LAYER,
                    chunk_size=CHUNK_SIZE_POSTGRES,
                ):
                    if chunk.empty:
                        continue

                    geometry_column = str(chunk.geometry.name)
                    before = int(len(chunk))
                    invalid_mask = chunk[geometry_column].isna() | gpd.GeoSeries(
                        chunk[geometry_column], crs=chunk.crs
                    ).is_empty
                    invalid_frame = chunk[invalid_mask].copy()
                    chunk = chunk[~invalid_mask].copy()
                    skipped_invalid_geom += int(before - len(chunk))

                    if not invalid_frame.empty:
                        rejected_frames["INVALID_OR_EMPTY_GEOMETRY"] = pd.concat(
                            [rejected_frames.get("INVALID_OR_EMPTY_GEOMETRY", pd.DataFrame()), invalid_frame],
                            axis=0,
                            ignore_index=True,
                        ).head(REJECTION_SAMPLE_LIMIT)

                    if chunk.empty:
                        continue

                    if "id_alerta_original" not in chunk.columns:
                        raise MapbiomasAlertaPipelineError(
                            "Transformed dataset is missing 'id_alerta_original' required for duplicate control."
                        )

                    chunk["id_alerta_original"] = pd.to_numeric(chunk["id_alerta_original"], errors="coerce").astype("Int64")

                    missing_code_mask = chunk["id_alerta_original"].isna()
                    missing_code_rows = chunk[missing_code_mask].copy()
                    chunk = chunk[~missing_code_mask].copy()

                    if not missing_code_rows.empty:
                        rejected_frames["MISSING_ALERT_CODE"] = pd.concat(
                            [rejected_frames.get("MISSING_ALERT_CODE", pd.DataFrame()), missing_code_rows],
                            axis=0,
                            ignore_index=True,
                        ).head(REJECTION_SAMPLE_LIMIT)

                    if chunk.empty:
                        continue

                    duplicate_mask = chunk.duplicated(
                        subset=[
                            "id_alerta_original",
                            "id_uc",
                            "id_za_oficial",
                            "id_buffer_abrangencia",
                            "tipo_cruzamento",
                        ],
                        keep="first",
                    )
                    duplicate_rows = chunk[duplicate_mask].copy()
                    chunk = chunk[~duplicate_mask].copy()

                    if not duplicate_rows.empty:
                        rejected_frames["DUPLICATE_ALERT_CODE_IN_BATCH"] = pd.concat(
                            [rejected_frames.get("DUPLICATE_ALERT_CODE_IN_BATCH", pd.DataFrame()), duplicate_rows],
                            axis=0,
                            ignore_index=True,
                        ).head(REJECTION_SAMPLE_LIMIT)

                    if chunk.empty:
                        continue

                    candidate_codes = [
                        int(value)
                        for value in chunk["id_alerta_original"].dropna().astype(int).tolist()
                    ]
                    existing_keys: set[tuple[int, int, int, int, str]] = set()
                    if candidate_codes:
                        cur.execute(
                            """
                            SELECT id_alerta_original, id_uc,
                                   COALESCE(id_za_oficial, 0), COALESCE(id_buffer_abrangencia, 0),
                                   COALESCE(tipo_cruzamento, '')
                            FROM mapbiomas_alerta_clip
                            WHERE id_alerta_original = ANY(%s);
                            """,
                            (candidate_codes,),
                        )
                        existing_keys = {
                            (int(db_row[0]), int(db_row[1]), int(db_row[2]), int(db_row[3]), str(db_row[4]))
                            for db_row in cur.fetchall()
                        }

                    if existing_keys:
                        existing_mask = chunk.apply(
                            lambda item: (
                                int(item["id_alerta_original"]),
                                int(item["id_uc"]),
                                int(self._none_if_nan(item.get("id_za_oficial")) or 0),
                                int(self._none_if_nan(item.get("id_buffer_abrangencia")) or 0),
                                str(self._none_if_nan(item.get("tipo_cruzamento")) or ""),
                            ) in existing_keys,
                            axis=1,
                        )
                        existing_rows = chunk[existing_mask].copy()
                        chunk = chunk[~existing_mask].copy()

                        if not existing_rows.empty:
                            rejected_frames["DUPLICATE_ALERT_CODE_IN_DB"] = pd.concat(
                                [rejected_frames.get("DUPLICATE_ALERT_CODE_IN_DB", pd.DataFrame()), existing_rows],
                                axis=0,
                                ignore_index=True,
                            ).head(REJECTION_SAMPLE_LIMIT)

                    if chunk.empty:
                        continue

                    # Serialize against extinction, including snapshots produced before it.
                    cur.execute(
                        "SELECT id_uc FROM uc WHERE id_uc = ANY(%s) AND situacao = 'ATIVA' ORDER BY id_uc FOR SHARE",
                        (chunk["id_uc"].astype(int).unique().tolist(),),
                    )
                    active_ids = {row[0] for row in cur.fetchall()}
                    chunk = chunk[chunk["id_uc"].isin(active_ids)].copy()
                    chunk["dt_silver"] = dt_silver
                    chunk["dt_gold"] = dt_gold
                    rows = [self._row_to_db_tuple(row, geometry_column) for _, row in chunk.iterrows()]

                    execute_batch(
                        cur,
                        """
                        INSERT INTO mapbiomas_alerta_clip (
                            id_uc,
                            id_za_oficial,
                            id_buffer_abrangencia,
                            tipo_cruzamento,
                            id_alerta_original,
                            dt_deteccao,
                            dt_imagem_anterior,
                            dt_imagem_posterior,
                            area_ha,
                            ds_bioma,
                            ds_fonte_deteccao,
                            geom,
                            dt_bronze,
                            dt_silver,
                            dt_gold,
                            versao_dag
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            ST_SetSRID(ST_GeomFromText(%s), 4674),
                            %s, %s, %s, %s
                        );
                        """,
                        rows,
                        page_size=1000,
                    )
                    inserted_records += int(len(rows))
                    inserted_new_records += int(len(rows))

        base_counts = dict(transform_rejection_counts)
        for reason, frame in rejected_frames.items():
            if frame is None or frame.empty:
                continue
            base_counts[reason] = int(base_counts.get(reason, 0)) + int(len(frame))

        skipped_records = max(int(source_records - inserted_records), 0)

        if inserted_records == 0 and skipped_invalid_geom > 0 and skipped_records > 0:
            result_reason = "no_valid_geometry_for_insert"
        elif inserted_records == 0:
            result_reason = "no_eligible_rows_after_spatial_rules"
        else:
            result_reason = "loaded_postgres"

        self._write_quality_report(
            context=context,
            stage="load_postgres",
            source_records=source_records,
            inserted_records=inserted_records,
            inserted_new_records=inserted_new_records,
            inserted_updated_records=inserted_updated_records,
            skipped_records=skipped_records,
            result_reason=result_reason,
            rejected_frames=rejected_frames,
            rejection_counts_override=base_counts,
        )

        return self._result(
            context,
            status="loaded_postgres",
            inserted_records=int(inserted_records),
            inserted_new_records=int(inserted_new_records),
            inserted_updated_records=0,
            skipped_records=int(skipped_records),
            reason=result_reason,
        )

    def load_silver(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        transformed_path_raw = state.get("transformed_path")
        transformed_path = Path(transformed_path_raw) if transformed_path_raw else None

        if transformed_path is None or not transformed_path.exists():
            return self._result(context, status="skipped", reason="no_transformed_input")

        transformed = self._read_geodata(transformed_path, layer=MAPBIOMAS_ALERTA_LAYER)

        silver_dir = self._build_layer_output_dir(
            layer_root=Path(self.config.medallion_silver_path),
            temporal_folder=state["temporal_folder"],
        )

        silver_path = silver_dir / "mapbiomas_alerta_silver.gpkg"
        transformed.to_file(silver_path, layer=MAPBIOMAS_ALERTA_LAYER, driver="GPKG")

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

        return self._result(
            context,
            status="loaded_silver",
            silver_path=str(silver_path),
            dt_silver=dt_silver,
            records=int(len(transformed)),
        )

    def load_gold(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        transformed_path_raw = state.get("transformed_path")
        transformed_path = Path(transformed_path_raw) if transformed_path_raw else None

        if transformed_path is None or not transformed_path.exists():
            return self._result(context, status="skipped", reason="no_transformed_input")

        transformed = self._read_geodata(transformed_path, layer=MAPBIOMAS_ALERTA_LAYER)

        gold_dir = self._build_layer_output_dir(
            layer_root=Path(self.config.medallion_gold_path),
            temporal_folder=state["temporal_folder"],
        )

        gold_geojson = gold_dir / "mapbiomas_alerta_gold.geojson"
        transformed.to_file(gold_geojson, driver="GeoJSON")

        attributes = transformed.drop(columns=[transformed.geometry.name]).copy()
        attributes.to_csv(gold_dir / "mapbiomas_alerta_gold_attributes.csv", index=False, encoding="utf-8")

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

        return self._result(
            context,
            status="loaded_gold",
            gold_geojson=str(gold_geojson),
            dt_gold=dt_gold,
            records=int(len(transformed)),
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
            / "branch=mapbiomas_alerta"
            / f"stage={stage}"
        )
        target.mkdir(parents=True, exist_ok=True)
        return target

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
        rejection_counts_override: dict[str, int] | None = None,
    ) -> None:
        target = self._quality_stage_dir(context, stage=stage)
        generated_at = datetime.now(timezone.utc).isoformat()

        rejection_counts = {
            reason: int(len(frame))
            for reason, frame in rejected_frames.items()
            if frame is not None and not frame.empty
        }
        if rejection_counts_override:
            for reason_code, count in rejection_counts_override.items():
                rejection_counts[reason_code] = int(count)

        rejection_reason_details = self._build_rejection_reason_details(rejection_counts)

        summary = {
            "domain": self.domain_name,
            "branch": "mapbiomas_alerta",
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

        geo_rejected = gpd.GeoDataFrame(
            rejected_combined,
            geometry=geometry_column,
            crs=getattr(rejected_combined, "crs", None),
        )
        geo_rejected = geo_rejected[~geo_rejected[geometry_column].isna()].copy()
        if geo_rejected.empty:
            return

        geo_rejected = geo_rejected[
            ~gpd.GeoSeries(geo_rejected[geometry_column], crs=geo_rejected.crs).is_empty
        ].copy()
        if geo_rejected.empty:
            return

        geo_rejected.to_file(target / "rejected_rows.geojson", driver="GeoJSON")

    def _combine_rejections(
        self,
        rejected_frames: dict[str, pd.DataFrame],
    ) -> pd.DataFrame | gpd.GeoDataFrame | None:
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

    def _branch_state_path(self, context: TaskExecutionContext, branch: str) -> Path:
        safe_branch = re.sub(r"[^a-zA-Z0-9_.-]", "_", branch)
        return self._run_dir(context) / f"state_{safe_branch}.json"

    def _save_state(self, context: TaskExecutionContext, state: dict[str, Any]) -> None:
        path = self._state_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _save_branch_state(self, context: TaskExecutionContext, branch: str, state: dict[str, Any]) -> None:
        path = self._branch_state_path(context, branch=branch)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _load_state(self, context: TaskExecutionContext) -> dict[str, Any]:
        path = self._state_path(context)
        if not path.exists():
            raise MapbiomasAlertaPipelineError(
                f"Missing pipeline state for stage '{context.stage}': {path}"
            )
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_branch_state(self, context: TaskExecutionContext, branch: str) -> dict[str, Any]:
        path = self._branch_state_path(context, branch=branch)
        if not path.exists():
            raise MapbiomasAlertaPipelineError(
                f"Missing branch state for stage '{context.stage}' branch '{branch}': {path}"
            )
        return json.loads(path.read_text(encoding="utf-8"))

    def _select_bronze_batch(self, source_root: Path) -> BronzeSelection:
        dated_candidates: list[tuple[datetime, int, Path]] = []

        domain_root = source_root / self.domain_name

        if domain_root.exists() and domain_root.is_dir():
            for child in domain_root.iterdir():
                if not child.is_dir():
                    continue
                parsed = self._try_parse_temporal_folder(child.name)
                if parsed is not None:
                    dated_candidates.append((parsed, 1, child))

        for child in source_root.iterdir():
            if not child.is_dir():
                continue
            parsed = self._try_parse_temporal_folder(child.name)
            if parsed is not None:
                domain_dir = child / self.domain_name
                if domain_dir.exists() and domain_dir.is_dir():
                    dated_candidates.append((parsed, 0, domain_dir))

        if dated_candidates:
            dated_candidates.sort(key=lambda item: (item[0], item[1]))
            selected_dt, _, selected_dir = dated_candidates[-1]
            return BronzeSelection(batch_dir=selected_dir, dt_bronze=selected_dt.date().isoformat())

        if domain_root.exists() and domain_root.is_dir():
            return BronzeSelection(
                batch_dir=domain_root,
                dt_bronze=datetime.now(timezone.utc).date().isoformat(),
            )

        raise MapbiomasAlertaPipelineError(
            f"No bronze batch found for domain '{self.domain_name}' at {source_root}"
        )

    def _try_parse_temporal_folder(self, folder_name: str) -> datetime | None:
        for fmt in (TEMPORAL_FOLDER_FORMAT, LEGACY_TEMPORAL_FOLDER_FORMAT):
            try:
                return datetime.strptime(folder_name, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def _temporal_folder_stamp(self, context: TaskExecutionContext) -> str:
        logical = datetime.fromisoformat(context.logical_date.replace("Z", "+00:00"))
        return logical.astimezone(timezone.utc).strftime(TEMPORAL_FOLDER_FORMAT)

    def _build_layer_output_dir(self, layer_root: Path, temporal_folder: str) -> Path:
        target = layer_root / self.domain_name / temporal_folder
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _locate_shapefile(self, batch_dir: Path, run_dir: Path) -> tuple[Path, str]:
        shapefile = self._find_shapefile_in_directory(batch_dir)
        if shapefile is not None:
            self._validate_shapefile_components(shapefile)
            return shapefile, "shapefile"

        zip_files = sorted(batch_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not zip_files:
            raise MapbiomasAlertaPipelineError(f"No shapefile or zip found in {batch_dir}")

        extraction_dir = run_dir / "unzipped"
        extraction_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_files[0]) as zf:
            zf.extractall(extraction_dir)

        shapefile = self._find_shapefile_in_directory(extraction_dir)
        if shapefile is None:
            raise MapbiomasAlertaPipelineError(f"No .shp after extracting {zip_files[0]}")

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
            raise MapbiomasAlertaPipelineError(f"Missing sidecar files for {shp_path.name}: {missing}")

    def _read_geodata(self, path: Path, layer: str | None = None) -> gpd.GeoDataFrame:
        open_kwargs: dict[str, Any] = {}
        if layer is not None:
            open_kwargs["layer"] = layer

        with fiona.open(str(path), **open_kwargs) as src:
            features = list(src)
            if not features:
                return gpd.GeoDataFrame(geometry=[], crs=src.crs)
            return gpd.GeoDataFrame.from_features(features, crs=src.crs)

    def _iter_layer_chunks(self, path: Path, layer: str, chunk_size: int) -> Iterator[gpd.GeoDataFrame]:
        with fiona.open(str(path), layer=layer) as src:
            buffer: list[dict[str, Any]] = []
            for feature in src:
                buffer.append(feature)
                if len(buffer) >= chunk_size:
                    yield gpd.GeoDataFrame.from_features(buffer, crs=src.crs)
                    buffer = []
            if buffer:
                yield gpd.GeoDataFrame.from_features(buffer, crs=src.crs)

    def _iter_sc_chunks(self, path: Path, chunk_size: int) -> Iterator[gpd.GeoDataFrame]:
        with fiona.open(str(path)) as src:
            state_field = self._detect_source_column(
                list((src.schema or {}).get("properties", {}).keys()),
                ["ESTADO"],
            )
            if state_field is None:
                raise InputValidationError("Field ESTADO is required for early state filtering (SC/PR/RS).")

            buffer: list[dict[str, Any]] = []
            for feature in src:
                props = (feature or {}).get("properties") or {}
                if not self._is_target_state(props.get(state_field)):
                    continue

                buffer.append(feature)
                if len(buffer) >= chunk_size:
                    yield gpd.GeoDataFrame.from_features(buffer, crs=src.crs)
                    buffer = []

            if buffer:
                yield gpd.GeoDataFrame.from_features(buffer, crs=src.crs)

    def _load_reference_layers(self) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
        if not self.config.project_db_url:
            raise MapbiomasAlertaPipelineError("PROJECT_DB_URL is required for spatial reference lookup.")

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_uc, ST_AsBinary(geom) AS geom FROM uc WHERE situacao = 'ATIVA'")
                uc_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT id_za_oficial, id_uc, ST_AsBinary(geom) AS geom
                    FROM za_oficial
                    WHERE fl_ativa = TRUE;
                    """
                )
                za_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT id_buffer_abrangencia, id_uc, ST_AsBinary(geom) AS geom
                    FROM buffer_abrangencia
                    WHERE fl_ativa = TRUE;
                    """
                )
                buffer_rows = cur.fetchall()

        uc_gdf = self._rows_to_gdf(uc_rows, ["id_uc", "geometry"])
        za_gdf = self._rows_to_gdf(za_rows, ["id_za_oficial", "id_uc", "geometry"])
        buffer_gdf = self._rows_to_gdf(buffer_rows, ["id_buffer_abrangencia", "id_uc", "geometry"])
        za_gdf = self._exclusive_zone_geometries(za_gdf, uc_gdf)
        buffer_gdf = self._exclusive_zone_geometries(buffer_gdf, uc_gdf)

        return uc_gdf, za_gdf, buffer_gdf

    def _rows_to_gdf(self, rows: list[tuple[Any, ...]], columns: list[str]) -> gpd.GeoDataFrame:
        frame = pd.DataFrame(rows, columns=columns)
        if frame.empty:
            return gpd.GeoDataFrame(frame, geometry=[], crs="EPSG:4674")

        geometry_column = columns[-1]
        frame[geometry_column] = frame[geometry_column].apply(
            lambda value: wkb.loads(bytes(value)) if value is not None else None
        )
        gdf = gpd.GeoDataFrame(frame, geometry=geometry_column, crs="EPSG:4674")
        gdf = gdf[~gdf[geometry_column].isna()].copy()
        if gdf.empty:
            return gdf
        gdf = gdf[~gpd.GeoSeries(gdf[geometry_column], crs=gdf.crs).is_empty].copy()
        return gdf

    def _extract_uc_intersections_chunk(
        self,
        mapped: gpd.GeoDataFrame,
        uc_gdf: gpd.GeoDataFrame,
    ) -> tuple[gpd.GeoDataFrame, int]:
        geometry_column = str(mapped.geometry.name)
        invalid_mask = mapped[geometry_column].isna() | gpd.GeoSeries(mapped[geometry_column], crs=mapped.crs).is_empty
        invalid_count = int(invalid_mask.sum())

        mapped_valid = mapped[~invalid_mask].copy()
        if mapped_valid.empty:
            return gpd.GeoDataFrame(mapped_valid, geometry=geometry_column, crs=mapped.crs), invalid_count

        uc_join = gpd.sjoin(
            mapped_valid,
            uc_gdf[["id_uc", uc_gdf.geometry.name]],
            how="inner",
            predicate="intersects",
        )
        if uc_join.empty:
            return gpd.GeoDataFrame(mapped_valid.iloc[0:0].copy(), geometry=geometry_column, crs=mapped.crs), invalid_count

        uc_id_column = "id_uc"
        if uc_id_column not in uc_join.columns:
            if "id_uc_right" in uc_join.columns:
                uc_id_column = "id_uc_right"
            elif "id_uc_left" in uc_join.columns:
                uc_id_column = "id_uc_left"
            else:
                raise MapbiomasAlertaPipelineError("Spatial join output is missing UC identifier column.")

        uc_join["id_uc"] = pd.to_numeric(uc_join[uc_id_column], errors="coerce").astype("Int64")
        uc_join = uc_join[~uc_join["id_uc"].isna()].copy()
        if uc_join.empty:
            return gpd.GeoDataFrame(mapped_valid.iloc[0:0].copy(), geometry=geometry_column, crs=mapped.crs), invalid_count

        uc_join["id_uc"] = uc_join["id_uc"].astype(int)
        return self._intersect_with_uc_layer(uc_join, uc_gdf), invalid_count

    def _extract_zone_intersections_chunk(
        self,
        mapped: gpd.GeoDataFrame,
        za_gdf: gpd.GeoDataFrame,
        buffer_gdf: gpd.GeoDataFrame,
        uc_with_active_za: set[int],
    ) -> tuple[gpd.GeoDataFrame, int]:
        geometry_column = str(mapped.geometry.name)
        invalid_mask = mapped[geometry_column].isna() | gpd.GeoSeries(mapped[geometry_column], crs=mapped.crs).is_empty
        invalid_count = int(invalid_mask.sum())

        mapped_valid = mapped[~invalid_mask].copy()
        if mapped_valid.empty:
            return gpd.GeoDataFrame(mapped_valid, geometry=geometry_column, crs=mapped.crs), invalid_count

        valid_za = self._intersect_unmatched_with_zone_layer(
            mapped_valid,
            za_gdf,
            zone_id_column="id_za_oficial",
            branch="za",
        )

        valid_buffer = self._intersect_unmatched_with_zone_layer(
            mapped_valid,
            buffer_gdf,
            zone_id_column="id_buffer_abrangencia",
            branch="buffer",
        )
        if not valid_buffer.empty:
            valid_buffer = valid_buffer[~valid_buffer["id_uc"].isin(uc_with_active_za)].copy()

        if valid_za.empty and valid_buffer.empty:
            return gpd.GeoDataFrame(mapped_valid.iloc[0:0].copy(), geometry=geometry_column, crs=mapped.crs), invalid_count

        zone_parts: list[gpd.GeoDataFrame] = []
        if not valid_za.empty:
            zone_parts.append(valid_za)
        if not valid_buffer.empty:
            zone_parts.append(valid_buffer)

        combined = pd.concat(zone_parts, axis=0, ignore_index=True)
        return gpd.GeoDataFrame(combined, geometry=geometry_column, crs=mapped.crs), invalid_count

    def _append_gpkg_layer(
        self,
        chunk: gpd.GeoDataFrame,
        path: Path,
        layer: str,
        wrote_layer: bool,
    ) -> bool:
        if chunk.empty:
            return wrote_layer

        path.parent.mkdir(parents=True, exist_ok=True)
        chunk_to_write = chunk.copy()
        for column_name in (
            "dt_deteccao", "dt_imagem_anterior", "dt_imagem_posterior",
            "dt_bronze", "dt_silver", "dt_gold",
        ):
            if column_name in chunk_to_write.columns:
                chunk_to_write[column_name] = chunk_to_write[column_name].apply(
                    self._serialize_temporal_value_for_gpkg
                )

        mode = "w" if not wrote_layer else "a"
        chunk_to_write.to_file(path, layer=layer, driver="GPKG", mode=mode)
        return True

    @staticmethod
    def _serialize_temporal_value_for_gpkg(value: Any) -> str | None:
        """Convert temporal scalars to Fiona-compatible ISO strings."""
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, str):
            normalized = value.strip()
            return normalized or None
        if isinstance(value, (pd.Timestamp, datetime, date)):
            return value.isoformat()
        isoformat = getattr(value, "isoformat", None)
        if callable(isoformat):
            serialized = isoformat()
            return serialized if isinstance(serialized, str) else str(serialized)
        return str(value)

    def _apply_spatial_rules(
        self,
        mapped: gpd.GeoDataFrame,
        uc_gdf: gpd.GeoDataFrame,
        za_gdf: gpd.GeoDataFrame,
        buffer_gdf: gpd.GeoDataFrame,
        uc_with_active_za: set[int],
    ) -> tuple[gpd.GeoDataFrame, dict[str, pd.DataFrame]]:
        rejected: dict[str, pd.DataFrame] = {}

        geometry_column = str(mapped.geometry.name)
        invalid_mask = mapped[geometry_column].isna() | gpd.GeoSeries(mapped[geometry_column], crs=mapped.crs).is_empty
        if invalid_mask.any():
            rejected["INVALID_OR_EMPTY_GEOMETRY"] = mapped[invalid_mask].copy()
        mapped_valid = mapped[~invalid_mask].copy()

        if mapped_valid.empty:
            return gpd.GeoDataFrame(mapped_valid, geometry=geometry_column, crs=mapped.crs), rejected

        uc_join = gpd.sjoin(
            mapped_valid,
            uc_gdf[["id_uc", uc_gdf.geometry.name]],
            how="inner",
            predicate="intersects",
        )
        if not uc_join.empty:
            uc_id_column = "id_uc"
            if uc_id_column not in uc_join.columns:
                if "id_uc_right" in uc_join.columns:
                    uc_id_column = "id_uc_right"
                elif "id_uc_left" in uc_join.columns:
                    uc_id_column = "id_uc_left"
                else:
                    raise MapbiomasAlertaPipelineError(
                        "Spatial join output is missing UC identifier column."
                    )

            uc_join["id_uc"] = pd.to_numeric(uc_join[uc_id_column], errors="coerce").astype("Int64")
            uc_join = uc_join[~uc_join["id_uc"].isna()].copy()
            uc_join["id_uc"] = uc_join["id_uc"].astype(int)

        matched_by_uc = set(uc_join["alert_row_id"].tolist()) if not uc_join.empty else set()
        unmatched_by_uc = mapped_valid[~mapped_valid["alert_row_id"].isin(matched_by_uc)].copy()

        valid_parts: list[gpd.GeoDataFrame] = []

        if not uc_join.empty:
            with_za = uc_join[uc_join["id_uc"].isin(uc_with_active_za)].copy()
            with_buffer = uc_join[~uc_join["id_uc"].isin(uc_with_active_za)].copy()

            valid_za, _ = self._intersect_with_zone_layer(
                joined=with_za,
                zone_gdf=za_gdf,
                zone_id_column="id_za_oficial",
                branch="za",
            )
            valid_buffer, _ = self._intersect_with_zone_layer(
                joined=with_buffer,
                zone_gdf=buffer_gdf,
                zone_id_column="id_buffer_abrangencia",
                branch="buffer",
            )

            if not valid_za.empty:
                valid_parts.append(valid_za)
            if not valid_buffer.empty:
                valid_parts.append(valid_buffer)

            valid_uc = self._intersect_with_uc_layer(uc_join, uc_gdf)
            if not valid_uc.empty:
                valid_parts.append(valid_uc)

        zone_only_za = self._intersect_unmatched_with_zone_layer(
            unmatched_by_uc,
            za_gdf,
            zone_id_column="id_za_oficial",
            branch="za",
        )

        zone_only_buffer = self._intersect_unmatched_with_zone_layer(
            unmatched_by_uc,
            buffer_gdf,
            zone_id_column="id_buffer_abrangencia",
            branch="buffer",
        )
        if not zone_only_buffer.empty:
            zone_only_buffer = zone_only_buffer[~zone_only_buffer["id_uc"].isin(uc_with_active_za)].copy()

        if not zone_only_za.empty:
            valid_parts.append(zone_only_za)
        if not zone_only_buffer.empty:
            valid_parts.append(zone_only_buffer)

        matched_by_zone_only: set[int] = set()
        if not zone_only_za.empty:
            matched_by_zone_only.update(zone_only_za["alert_row_id"].astype(int).tolist())
        if not zone_only_buffer.empty:
            matched_by_zone_only.update(zone_only_buffer["alert_row_id"].astype(int).tolist())

        still_unmatched = unmatched_by_uc[~unmatched_by_uc["alert_row_id"].isin(matched_by_zone_only)].copy()
        if not still_unmatched.empty:
            rejected["NO_UC_INTERSECTION"] = still_unmatched

        if not valid_parts:
            return gpd.GeoDataFrame(mapped_valid.iloc[0:0].copy(), geometry=geometry_column, crs=mapped.crs), rejected

        combined = pd.concat(valid_parts, axis=0, ignore_index=True)
        return gpd.GeoDataFrame(combined, geometry=geometry_column, crs=mapped.crs), rejected

    @staticmethod
    def _exclusive_zone_geometries(zone_gdf: gpd.GeoDataFrame, uc_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Return zone geometries with their owning UC removed for thematic analysis."""
        if zone_gdf.empty:
            return zone_gdf
        geometry_column = str(zone_gdf.geometry.name)
        uc_geometry_column = str(uc_gdf.geometry.name)
        lookup = uc_gdf[["id_uc", uc_geometry_column]].rename(columns={uc_geometry_column: "_uc_geometry"})
        result = zone_gdf.merge(lookup, on="id_uc", how="left")
        result[geometry_column] = gpd.GeoSeries(result[geometry_column], crs=zone_gdf.crs).difference(
            gpd.GeoSeries(result["_uc_geometry"], crs=zone_gdf.crs)
        )
        result = result.drop(columns=["_uc_geometry"])
        return gpd.GeoDataFrame(result, geometry=geometry_column, crs=zone_gdf.crs)

    def _intersect_with_uc_layer(
        self,
        joined: gpd.GeoDataFrame,
        uc_gdf: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        if joined.empty:
            geometry_column = str(joined.geometry.name) if isinstance(joined, gpd.GeoDataFrame) else "geometry"
            return gpd.GeoDataFrame(joined.copy(), geometry=geometry_column, crs=getattr(joined, "crs", None))

        geometry_column = str(joined.geometry.name)
        uc_geom_column = str(uc_gdf.geometry.name)

        uc_view = uc_gdf[["id_uc", uc_geom_column]].rename(columns={uc_geom_column: "uc_geometry"}).copy()
        merged = joined.merge(uc_view, on="id_uc", how="left")
        merged = merged[~merged["uc_geometry"].isna()].copy()
        if merged.empty:
            return gpd.GeoDataFrame(joined.iloc[0:0].copy(), geometry=geometry_column, crs=joined.crs)

        alert_geometry = gpd.GeoSeries(merged[geometry_column], crs=joined.crs)
        uc_geometry = gpd.GeoSeries(merged["uc_geometry"], crs=joined.crs)
        merged[geometry_column] = alert_geometry.intersection(uc_geometry).apply(self._normalize_polygon_geometry)

        valid = merged[
            ~(merged[geometry_column].isna() | gpd.GeoSeries(merged[geometry_column], crs=joined.crs).is_empty)
        ].copy()
        if valid.empty:
            return gpd.GeoDataFrame(joined.iloc[0:0].copy(), geometry=geometry_column, crs=joined.crs)

        valid["id_za_oficial"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
        valid["id_buffer_abrangencia"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
        valid["tipo_cruzamento"] = TIPO_CRUZAMENTO_UC

        valid["area_ha"] = (
            gpd.GeoSeries(valid[geometry_column], crs=joined.crs)
            .to_crs(epsg=32722)
            .area / 10_000
        )

        keep_columns = [
            "alert_row_id",
            "id_uc",
            "id_za_oficial",
            "id_buffer_abrangencia",
            "tipo_cruzamento",
            "id_alerta_original",
            "dt_deteccao",
            "dt_imagem_anterior",
            "dt_imagem_posterior",
            "area_ha",
            "ds_bioma",
            "ds_fonte_deteccao",
            "nm_estado_origem",
            "nm_municipio_origem",
            "ano_origem",
            "vpressao_origem",
            "dt_bronze",
            "dt_silver",
            "dt_gold",
            "versao_dag",
            geometry_column,
        ]
        valid = valid[[col for col in keep_columns if col in valid.columns]].copy()
        return gpd.GeoDataFrame(valid, geometry=geometry_column, crs=joined.crs)

    def _intersect_unmatched_with_zone_layer(
        self,
        unmatched: gpd.GeoDataFrame,
        zone_gdf: gpd.GeoDataFrame,
        zone_id_column: str,
        branch: str,
    ) -> gpd.GeoDataFrame:
        if unmatched.empty or zone_gdf.empty:
            geometry_column = str(unmatched.geometry.name) if isinstance(unmatched, gpd.GeoDataFrame) else "geometry"
            return gpd.GeoDataFrame(unmatched.iloc[0:0].copy(), geometry=geometry_column, crs=getattr(unmatched, "crs", None))

        geometry_column = str(unmatched.geometry.name)
        zone_geom_column = str(zone_gdf.geometry.name)

        zone_view = zone_gdf[["id_uc", zone_id_column, zone_geom_column]].rename(
            columns={zone_geom_column: "zone_geometry"}
        )
        zone_view = gpd.GeoDataFrame(zone_view, geometry="zone_geometry", crs=zone_gdf.crs)

        joined = gpd.sjoin(unmatched, zone_view, how="inner", predicate="intersects")
        if joined.empty:
            return gpd.GeoDataFrame(unmatched.iloc[0:0].copy(), geometry=geometry_column, crs=unmatched.crs)

        if "id_uc" not in joined.columns:
            uc_column = next((name for name in ["id_uc_right", "id_uc_left"] if name in joined.columns), "")
            if not uc_column:
                return gpd.GeoDataFrame(unmatched.iloc[0:0].copy(), geometry=geometry_column, crs=unmatched.crs)
            joined["id_uc"] = joined[uc_column]

        if zone_id_column not in joined.columns:
            zone_column = next(
                (
                    name
                    for name in [f"{zone_id_column}_right", f"{zone_id_column}_left"]
                    if name in joined.columns
                ),
                "",
            )
            if not zone_column:
                return gpd.GeoDataFrame(unmatched.iloc[0:0].copy(), geometry=geometry_column, crs=unmatched.crs)
            joined[zone_id_column] = joined[zone_column]

        joined["id_uc"] = pd.to_numeric(joined["id_uc"], errors="coerce").astype("Int64")
        joined = joined[~joined["id_uc"].isna()].copy()
        if joined.empty:
            return gpd.GeoDataFrame(unmatched.iloc[0:0].copy(), geometry=geometry_column, crs=unmatched.crs)

        joined["id_uc"] = joined["id_uc"].astype(int)
        joined = joined.merge(
            zone_view[["zone_geometry"]],
            left_on="index_right",
            right_index=True,
            how="left",
        )
        joined = joined[~joined["zone_geometry"].isna()].copy()
        if joined.empty:
            return gpd.GeoDataFrame(unmatched.iloc[0:0].copy(), geometry=geometry_column, crs=unmatched.crs)

        alert_geometry = gpd.GeoSeries(joined[geometry_column], crs=unmatched.crs)
        zone_geometry = gpd.GeoSeries(joined["zone_geometry"], crs=unmatched.crs)
        joined[geometry_column] = alert_geometry.intersection(zone_geometry).apply(self._normalize_polygon_geometry)

        valid = joined[
            ~(joined[geometry_column].isna() | gpd.GeoSeries(joined[geometry_column], crs=unmatched.crs).is_empty)
        ].copy()
        if valid.empty:
            return gpd.GeoDataFrame(unmatched.iloc[0:0].copy(), geometry=geometry_column, crs=unmatched.crs)

        if branch == "za":
            valid["id_za_oficial"] = pd.to_numeric(valid[zone_id_column], errors="coerce").astype("Int64")
            valid["id_buffer_abrangencia"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
            valid["tipo_cruzamento"] = TIPO_CRUZAMENTO_ZA
        else:
            valid["id_buffer_abrangencia"] = pd.to_numeric(valid[zone_id_column], errors="coerce").astype("Int64")
            valid["id_za_oficial"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
            valid["tipo_cruzamento"] = TIPO_CRUZAMENTO_BUFFER_ABRANGENCIA

        valid["area_ha"] = (
            gpd.GeoSeries(valid[geometry_column], crs=unmatched.crs)
            .to_crs(epsg=32722)
            .area / 10_000
        )

        keep_columns = [
            "alert_row_id",
            "id_uc",
            "id_za_oficial",
            "id_buffer_abrangencia",
            "tipo_cruzamento",
            "id_alerta_original",
            "dt_deteccao",
            "dt_imagem_anterior",
            "dt_imagem_posterior",
            "area_ha",
            "ds_bioma",
            "ds_fonte_deteccao",
            "nm_estado_origem",
            "nm_municipio_origem",
            "ano_origem",
            "vpressao_origem",
            "dt_bronze",
            "dt_silver",
            "dt_gold",
            "versao_dag",
            geometry_column,
        ]
        valid = valid[[col for col in keep_columns if col in valid.columns]].copy()
        return gpd.GeoDataFrame(valid, geometry=geometry_column, crs=unmatched.crs)

    def _intersect_with_zone_layer(
        self,
        joined: gpd.GeoDataFrame,
        zone_gdf: gpd.GeoDataFrame,
        zone_id_column: str,
        branch: str,
    ) -> tuple[gpd.GeoDataFrame, dict[str, pd.DataFrame]]:
        rejected: dict[str, pd.DataFrame] = {}
        if joined.empty:
            geometry_column = str(joined.geometry.name) if isinstance(joined, gpd.GeoDataFrame) else "geometry"
            empty = gpd.GeoDataFrame(joined.copy(), geometry=geometry_column, crs=getattr(joined, "crs", None))
            return empty, rejected

        geometry_column = str(joined.geometry.name)
        zone_geom_column = str(zone_gdf.geometry.name) if not zone_gdf.empty else "geometry"
        zone_id_lookup_column = f"zone_{zone_id_column}"

        zone_view = (
            zone_gdf[["id_uc", zone_id_column, zone_geom_column]]
            .rename(columns={zone_id_column: zone_id_lookup_column, zone_geom_column: "zone_geometry"})
            .copy()
            if not zone_gdf.empty
            else pd.DataFrame(columns=["id_uc", zone_id_lookup_column, "zone_geometry"])
        )

        merged = joined.merge(zone_view, on="id_uc", how="left")

        zone_merge_column = zone_id_lookup_column
        if zone_merge_column not in merged.columns:
            candidates = [
                zone_id_lookup_column,
                f"{zone_id_lookup_column}_x",
                f"{zone_id_lookup_column}_y",
                zone_id_column,
                f"{zone_id_column}_x",
                f"{zone_id_column}_y",
            ]
            zone_merge_column = next((name for name in candidates if name in merged.columns), "")

        if not zone_merge_column:
            rejected["NO_ACTIVE_ZONE_FOR_UC"] = merged.copy()
            return gpd.GeoDataFrame(joined.iloc[0:0].copy(), geometry=geometry_column, crs=joined.crs), rejected

        no_zone = merged[merged[zone_merge_column].isna()].copy()
        if not no_zone.empty:
            rejected["NO_ACTIVE_ZONE_FOR_UC"] = no_zone

        candidates = merged[~merged[zone_merge_column].isna()].copy()
        if candidates.empty:
            return gpd.GeoDataFrame(joined.iloc[0:0].copy(), geometry=geometry_column, crs=joined.crs), rejected

        alert_geometry = gpd.GeoSeries(candidates[geometry_column], crs=joined.crs)
        zone_geometry = gpd.GeoSeries(candidates["zone_geometry"], crs=joined.crs)

        intersected = alert_geometry.intersection(zone_geometry)
        candidates[geometry_column] = intersected.apply(self._normalize_polygon_geometry)

        invalid_after_intersection = candidates[
            candidates[geometry_column].isna() | gpd.GeoSeries(candidates[geometry_column], crs=joined.crs).is_empty
        ].copy()
        if not invalid_after_intersection.empty:
            rejected["NO_ZONE_INTERSECTION"] = invalid_after_intersection

        valid = candidates[
            ~(candidates[geometry_column].isna() | gpd.GeoSeries(candidates[geometry_column], crs=joined.crs).is_empty)
        ].copy()

        if valid.empty:
            return gpd.GeoDataFrame(joined.iloc[0:0].copy(), geometry=geometry_column, crs=joined.crs), rejected

        if branch == "za":
            valid["id_za_oficial"] = pd.to_numeric(valid[zone_merge_column], errors="coerce").astype("Int64")
            valid["id_buffer_abrangencia"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
            valid["tipo_cruzamento"] = TIPO_CRUZAMENTO_ZA
        else:
            valid["id_buffer_abrangencia"] = pd.to_numeric(valid[zone_merge_column], errors="coerce").astype("Int64")
            valid["id_za_oficial"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
            valid["tipo_cruzamento"] = TIPO_CRUZAMENTO_BUFFER_ABRANGENCIA

        valid["area_ha"] = (
            gpd.GeoSeries(valid[geometry_column], crs=joined.crs)
            .to_crs(epsg=32722)
            .area / 10_000
        )

        keep_columns = [
            "alert_row_id",
            "id_uc",
            "id_za_oficial",
            "id_buffer_abrangencia",
            "tipo_cruzamento",
            "id_alerta_original",
            "dt_deteccao",
            "dt_imagem_anterior",
            "dt_imagem_posterior",
            "area_ha",
            "ds_bioma",
            "ds_fonte_deteccao",
            "nm_estado_origem",
            "nm_municipio_origem",
            "ano_origem",
            "vpressao_origem",
            "dt_bronze",
            "dt_silver",
            "dt_gold",
            "versao_dag",
            geometry_column,
        ]
        valid = valid[[col for col in keep_columns if col in valid.columns]].copy()

        return gpd.GeoDataFrame(valid, geometry=geometry_column, crs=joined.crs), rejected

    def _map_to_target_schema(
        self,
        gdf: gpd.GeoDataFrame,
        dt_bronze: str,
        dag_version: str,
        row_offset: int,
    ) -> gpd.GeoDataFrame:
        mapped = pd.DataFrame(index=gdf.index)

        mapped["ds_fonte_deteccao"] = self._get_source_value(gdf, ["FONTE", "fonte"])
        mapped["ds_bioma"] = self._get_source_value(gdf, ["BIOMA", "bioma"])
        mapped["nm_estado_origem"] = self._get_source_value(gdf, ["ESTADO", "estado"])
        mapped["nm_municipio_origem"] = self._get_source_value(gdf, ["MUNICIPIO", "municipio"])
        mapped["area_ha"] = self._to_numeric_series(self._get_source_value(gdf, ["AREAHA", "areaha"]))

        ano_series = self._parse_year_series(self._get_source_value(gdf, ["ANODETEC", "anodetec"]))
        mapped["ano_origem"] = ano_series

        dt_deteccao_source = self._get_source_value(
            gdf,
            [
                "DT_DETECCAO",
                "dt_deteccao",
                "DATA_DETEC",
                "data_detec",
                "DT_DETEC",
                "DATADETEC",
                "datadetec",
            ],
        )
        dt_deteccao = self._parse_date_series(dt_deteccao_source)
        if dt_deteccao is None:
            dt_deteccao = ano_series.apply(
                lambda value: date(int(value), 1, 1) if value is not None and not pd.isna(value) else None
            )
        mapped["dt_deteccao"] = dt_deteccao

        mapped["dt_imagem_anterior"] = self._parse_date_series(
            self._get_source_value(gdf, ["DT_IMAGEM_ANTERIOR", "dt_imagem_anterior", "dt_img_ant", "DTIMGANT"])
        )
        mapped["dt_imagem_posterior"] = self._parse_date_series(
            self._get_source_value(gdf, ["DT_IMAGEM_POSTERIOR", "dt_imagem_posterior", "dt_img_pos", "DTIMGDEP"])
        )

        id_alerta_source = self._get_source_value(
            gdf,
            [
                *ALERT_CODE_SOURCE_FIELDS,
                "id_alerta_original",
                "id_alerta",
                "id",
                "fid",
                "objectid",
                "gid",
            ],
        )
        if id_alerta_source is not None:
            ids = pd.to_numeric(id_alerta_source, errors="coerce").astype("Int64")
        else:
            ids = pd.Series([pd.NA] * len(gdf), dtype="Int64")

        mapped["id_alerta_original"] = ids

        vpressao = self._get_source_value(gdf, ["VPRESSAO", "vpressao"])
        mapped["vpressao_origem"] = vpressao if vpressao is not None else None

        mapped["alert_row_id"] = pd.Series(
            range(row_offset, row_offset + len(gdf)),
            index=gdf.index,
            dtype="Int64",
        )

        mapped["dt_bronze"] = pd.to_datetime(dt_bronze, errors="coerce").date()
        mapped["dt_silver"] = None
        mapped["dt_gold"] = None
        mapped["versao_dag"] = dag_version

        mapped["geometry"] = gdf.geometry.apply(self._normalize_polygon_geometry)
        mapped_gdf = gpd.GeoDataFrame(mapped, geometry="geometry", crs=gdf.crs)
        return mapped_gdf

    def _merge_rejection_samples_and_counts(
        self,
        sample_store: dict[str, pd.DataFrame],
        count_store: dict[str, int],
        rejected_frames: dict[str, pd.DataFrame],
    ) -> None:
        for reason_code, frame in rejected_frames.items():
            if frame is None or frame.empty:
                continue

            count_store[reason_code] = int(count_store.get(reason_code, 0)) + int(len(frame))

            existing = sample_store.get(reason_code)
            existing_count = int(len(existing)) if existing is not None else 0
            remaining = max(REJECTION_SAMPLE_LIMIT - existing_count, 0)
            if remaining == 0:
                continue

            sampled = frame.head(remaining).copy()
            if existing is None:
                sample_store[reason_code] = sampled
            else:
                sample_store[reason_code] = pd.concat([existing, sampled], axis=0, ignore_index=True)

    def _row_to_db_tuple(self, row: pd.Series, geometry_column: str) -> tuple[Any, ...]:
        geometry_value = row.get(geometry_column)
        geometry_wkt = geometry_value.wkt if geometry_value is not None and not pd.isna(geometry_value) else None

        return (
            int(row["id_uc"]),
            self._none_if_nan(row.get("id_za_oficial")),
            self._none_if_nan(row.get("id_buffer_abrangencia")),
            self._none_if_nan(row.get("tipo_cruzamento")),
            int(row["id_alerta_original"]),
            self._none_if_nan(row.get("dt_deteccao")),
            self._none_if_nan(row.get("dt_imagem_anterior")),
            self._none_if_nan(row.get("dt_imagem_posterior")),
            self._none_if_nan(row.get("area_ha")),
            self._none_if_nan(row.get("ds_bioma")),
            self._none_if_nan(row.get("ds_fonte_deteccao")),
            geometry_wkt,
            self._none_if_nan(row.get("dt_bronze")),
            self._none_if_nan(row.get("dt_silver")),
            self._none_if_nan(row.get("dt_gold")),
            self._none_if_nan(row.get("versao_dag")),
        )

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

    def _normalize_polygon_geometry(self, geom):
        if geom is None:
            return None

        fixed = self._make_valid_geometry(geom)
        if fixed is None or fixed.is_empty:
            return None

        if isinstance(fixed, Polygon):
            return fixed

        if isinstance(fixed, MultiPolygon):
            polygons = [poly for poly in fixed.geoms if isinstance(poly, Polygon) and not poly.is_empty]
            if not polygons:
                return None
            return max(polygons, key=lambda poly: poly.area)

        if isinstance(fixed, GeometryCollection):
            polygons = [
                element
                for element in fixed.geoms
                if isinstance(element, Polygon) and not element.is_empty
            ]
            if not polygons:
                return None
            return max(polygons, key=lambda poly: poly.area)

        return None

    def _get_source_value(self, gdf: gpd.GeoDataFrame, candidates: list[str]) -> pd.Series | None:
        source_col = self._detect_source_column(gdf.columns, candidates)
        if source_col is None:
            return None
        return gdf[source_col]

    def _detect_source_column(self, columns, candidates: list[str]) -> str | None:
        normalized = {self._normalize_name(str(col)): col for col in columns}
        for candidate in candidates:
            found = normalized.get(self._normalize_name(candidate))
            if found is not None:
                return found
        return None

    def _normalize_name(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    def _to_numeric_series(self, value: pd.Series | None) -> pd.Series:
        if value is None:
            return pd.Series(dtype="float64")
        return pd.to_numeric(value, errors="coerce")

    def _parse_year_series(self, value: pd.Series | None) -> pd.Series:
        if value is None:
            return pd.Series(dtype="Int64")

        numeric = pd.to_numeric(value, errors="coerce")
        year = numeric.where((numeric >= 1900) & (numeric <= 2200), other=pd.NA)
        return year.round(0).astype("Int64")

    def _parse_date_series(self, value: pd.Series | None) -> pd.Series | None:
        if value is None:
            return None

        # The API publishes ISO dates; dayfirst=True misreads 2026-09-01 as January 9.
        parsed = pd.to_datetime(value, format="%Y-%m-%d", errors="coerce")
        missing = parsed.isna() & value.notna()
        if missing.any():
            parsed.loc[missing] = pd.to_datetime(value.loc[missing], errors="coerce", dayfirst=True)
        return parsed.dt.date

    def _is_target_state(self, value: Any) -> bool:
        clean = self._none_if_nan(value)
        if clean is None:
            return False

        normalized = self._normalize_name(str(clean)).upper()
        return normalized in TARGET_STATES_NORMALIZED

    def _none_if_nan(self, value: Any) -> Any:
        if value is None:
            return None
        if pd.isna(value):
            return None
        return value
