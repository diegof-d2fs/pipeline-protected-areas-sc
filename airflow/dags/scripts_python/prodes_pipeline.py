"""Pipeline service for PRODES clipping with UC/ZA/Buffer de Abrangência rules."""

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
from scripts_python.object_storage import fetch_bronze_prefix
from scripts_python.domain_pipeline import DomainPipelineService, TaskExecutionContext

LOGGER = logging.getLogger("pipeline.prodes")

TEMPORAL_FOLDER_FORMAT = "%Y-%m-%d-%H-%M-%S"
LEGACY_TEMPORAL_FOLDER_FORMAT = "%Y-%m-%d-%S-%M-%H"
REQUIRED_SHAPEFILE_EXTENSIONS = (".shp", ".shx", ".dbf")
QUALITY_FOLDER_NAME = "quality"

SOURCE_FIELDS = ("main_class", "year", "area_km")
PRODES_ID_SOURCE_FIELDS = ("uuid", "UUID", "id", "ID")
PRODES_LAYER = "prodes_stage"
PRODES_LAYER_UC = "prodes_transform_uc"
PRODES_LAYER_ZONE = "prodes_transform_zone"
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
    "MISSING_PRODES_ID": "Registro sem identificador uuid valido na origem.",
    "DUPLICATE_PRODES_ID_IN_BATCH": "Duplicado no lote por uuid.",
    "DUPLICATE_PRODES_ID_IN_DB": "uuid ja existe na tabela de destino.",
}


@dataclass(frozen=True)
class BronzeSelection:
    """Selected bronze batch directory and inferred bronze date."""

    batch_dir: Path
    dt_bronze: str


class ProdesPipelineError(RuntimeError):
    """Raised for operational errors in the PRODES pipeline."""


class InputValidationError(ProdesPipelineError):
    """Raised when source payload does not satisfy expected schema/contract."""


class ProdesPipelineService(DomainPipelineService):
    """PRODES implementation with UC/ZA/Buffer de Abrangência clipping and duplicate-safe load."""

    def __init__(self, config: PipelineConfig | None = None) -> None:
        super().__init__(domain_name="prodes", config=config)
        self.logger = LOGGER

    def extract(self, context: TaskExecutionContext) -> dict[str, Any]:
        source_root = Path(self.config.medallion_bronze_path)
        if not source_root.exists():
            raise ProdesPipelineError(
                f"Bronze root does not exist for {self.domain_name}: {source_root}"
            )

        # The newest batch may exist only in the S3 Bronze (uploaded while the node runs).
        fetch_bronze_prefix(self.config, f"{self.domain_name}/")
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

            prodes_id_field = self._detect_source_column(columns, list(PRODES_ID_SOURCE_FIELDS))
            if prodes_id_field is None:
                raise InputValidationError(
                    "Field uuid is required for deterministic de-duplication in Postgres load."
                )

            state_field = self._detect_source_column(columns, ["state", "STATE"])
            if state_field is None:
                raise InputValidationError(
                    "Field state is required for early state filtering (SC/PR/RS) and memory-safe processing."
                )

            if src.crs is None:
                raise InputValidationError("Source shapefile has no CRS metadata.")

        state["validated_at_utc"] = datetime.now(timezone.utc).isoformat()
        state["source_prodes_id_field"] = prodes_id_field
        state["source_state_field"] = state_field
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
            raise ProdesPipelineError("UC table is empty. Execute DAG_UCS before this DAG.")

        transformed_uc_path = self._run_dir(context) / "prodes_transform_uc.gpkg"
        if transformed_uc_path.exists():
            transformed_uc_path.unlink()

        source_records = 0
        transformed_records = 0
        invalid_geometry_records = 0
        row_offset = 0
        wrote_layer = False

        for source_chunk in self._iter_sc_chunks(shapefile_path, chunk_size=CHUNK_SIZE_SOURCE):
            if source_chunk.empty:
                continue

            source_records += int(len(source_chunk))
            source_chunk = self._ensure_epsg_4674(source_chunk)
            source_chunk = self._repair_geometries(source_chunk)

            mapped = self._map_to_target_schema(
                source_chunk,
                dt_bronze=state["dt_bronze"],
                dag_version=context.dag_id,
                row_offset=row_offset,
            )
            row_offset += int(len(mapped))

            valid_uc_chunk, invalid_count = self._extract_uc_intersections_chunk(mapped, uc_gdf)
            invalid_geometry_records += int(invalid_count)

            if valid_uc_chunk.empty:
                continue

            wrote_layer = self._append_gpkg_layer(
                chunk=valid_uc_chunk,
                path=transformed_uc_path,
                layer=PRODES_LAYER_UC,
                wrote_layer=wrote_layer,
            )
            transformed_records += int(len(valid_uc_chunk))

        self._save_branch_state(
            context,
            branch="transform_uc",
            state={
                "source_records": int(source_records),
                "transformed_records": int(transformed_records),
                "invalid_geometry_records": int(invalid_geometry_records),
                "transformed_uc_path": str(transformed_uc_path) if wrote_layer else None,
            },
        )

        return self._result(
            context,
            status="transformed_uc",
            source_records=int(source_records),
            transformed_records=int(transformed_records),
            transformed_uc_path=str(transformed_uc_path) if wrote_layer else None,
        )

    def transform_zone(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        shapefile_path = Path(state["shapefile_path"])

        uc_gdf, za_gdf, buffer_gdf = self._load_reference_layers()
        if uc_gdf.empty:
            raise ProdesPipelineError("UC table is empty. Execute DAG_UCS before this DAG.")

        uc_with_active_za = set(za_gdf["id_uc"].astype(int).tolist()) if not za_gdf.empty else set()

        transformed_zone_path = self._run_dir(context) / "prodes_transform_zone.gpkg"
        if transformed_zone_path.exists():
            transformed_zone_path.unlink()

        source_records = 0
        transformed_records = 0
        invalid_geometry_records = 0
        row_offset = 0
        wrote_layer = False

        for source_chunk in self._iter_sc_chunks(shapefile_path, chunk_size=CHUNK_SIZE_SOURCE):
            if source_chunk.empty:
                continue

            source_records += int(len(source_chunk))
            source_chunk = self._ensure_epsg_4674(source_chunk)
            source_chunk = self._repair_geometries(source_chunk)

            mapped = self._map_to_target_schema(
                source_chunk,
                dt_bronze=state["dt_bronze"],
                dag_version=context.dag_id,
                row_offset=row_offset,
            )
            row_offset += int(len(mapped))

            valid_zone_chunk, invalid_count = self._extract_zone_intersections_chunk(
                mapped=mapped,
                uc_gdf=uc_gdf,
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
                layer=PRODES_LAYER_ZONE,
                wrote_layer=wrote_layer,
            )
            transformed_records += int(len(valid_zone_chunk))

        self._save_branch_state(
            context,
            branch="transform_zone",
            state={
                "source_records": int(source_records),
                "transformed_records": int(transformed_records),
                "invalid_geometry_records": int(invalid_geometry_records),
                "transformed_zone_path": str(transformed_zone_path) if wrote_layer else None,
            },
        )

        return self._result(
            context,
            status="transformed_zone",
            source_records=int(source_records),
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

        transformed_path = self._run_dir(context) / "prodes_transformed.gpkg"
        if transformed_path.exists():
            transformed_path.unlink()

        wrote_layer = False
        transformed_records = 0
        matched_source_rows: set[int] = set()
        zone_keys: set[tuple[int, int]] = set()

        if transformed_zone_path is not None and transformed_zone_path.exists():
            for zone_chunk in self._iter_layer_chunks(
                transformed_zone_path,
                layer=PRODES_LAYER_ZONE,
                chunk_size=CHUNK_SIZE_SOURCE,
            ):
                if zone_chunk.empty:
                    continue

                zone_chunk["source_row_id"] = pd.to_numeric(zone_chunk["source_row_id"], errors="coerce").astype("Int64")
                zone_chunk["id_uc"] = pd.to_numeric(zone_chunk["id_uc"], errors="coerce").astype("Int64")

                for source_row_id, uc_id in (
                    zone_chunk[["source_row_id", "id_uc"]]
                    .dropna()
                    .astype({"source_row_id": "int64", "id_uc": "int64"})
                    .itertuples(index=False, name=None)
                ):
                    zone_keys.add((source_row_id, uc_id))

                matched_source_rows.update(
                    int(value)
                    for value in zone_chunk["source_row_id"].dropna().astype(int).tolist()
                )

                wrote_layer = self._append_gpkg_layer(
                    chunk=zone_chunk,
                    path=transformed_path,
                    layer=PRODES_LAYER,
                    wrote_layer=wrote_layer,
                )
                transformed_records += int(len(zone_chunk))

        if transformed_uc_path is not None and transformed_uc_path.exists():
            for uc_chunk in self._iter_layer_chunks(
                transformed_uc_path,
                layer=PRODES_LAYER_UC,
                chunk_size=CHUNK_SIZE_SOURCE,
            ):
                if uc_chunk.empty:
                    continue

                uc_chunk["source_row_id"] = pd.to_numeric(uc_chunk["source_row_id"], errors="coerce").astype("Int64")
                uc_chunk["id_uc"] = pd.to_numeric(uc_chunk["id_uc"], errors="coerce").astype("Int64")

                if zone_keys:
                    remove_mask = [
                        (
                            not pd.isna(source_row_id)
                            and not pd.isna(uc_id)
                            and (int(source_row_id), int(uc_id)) in zone_keys
                        )
                        for source_row_id, uc_id in zip(uc_chunk["source_row_id"], uc_chunk["id_uc"])
                    ]
                    uc_chunk = uc_chunk[[not flag for flag in remove_mask]].copy()

                if uc_chunk.empty:
                    continue

                matched_source_rows.update(
                    int(value)
                    for value in uc_chunk["source_row_id"].dropna().astype(int).tolist()
                )

                wrote_layer = self._append_gpkg_layer(
                    chunk=uc_chunk,
                    path=transformed_path,
                    layer=PRODES_LAYER,
                    wrote_layer=wrote_layer,
                )
                transformed_records += int(len(uc_chunk))

        source_records = int(
            max(
                int(uc_state.get("source_records", 0)),
                int(zone_state.get("source_records", 0)),
            )
        )
        invalid_geometry_records = int(
            max(
                int(uc_state.get("invalid_geometry_records", 0)),
                int(zone_state.get("invalid_geometry_records", 0)),
            )
        )

        rejection_counts: dict[str, int] = {}
        unmatched_records = max(source_records - len(matched_source_rows), 0)
        if unmatched_records > 0:
            rejection_counts["NO_UC_INTERSECTION"] = int(unmatched_records)
        if invalid_geometry_records > 0:
            rejection_counts["INVALID_OR_EMPTY_GEOMETRY"] = int(invalid_geometry_records)

        state["source_records"] = int(source_records)
        state["transformed_records"] = int(transformed_records)
        state["transform_rejection_counts"] = rejection_counts
        state["transformed_path"] = str(transformed_path) if wrote_layer else None
        self._save_state(context, state)

        return self._result(
            context,
            status="transformed",
            source_records=int(source_records),
            transformed_records=int(transformed_records),
            transformed_path=str(transformed_path) if wrote_layer else None,
            rejection_counts=rejection_counts,
        )

    def transform(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        shapefile_path = Path(state["shapefile_path"])

        uc_gdf, za_gdf, buffer_gdf = self._load_reference_layers()
        if uc_gdf.empty:
            raise ProdesPipelineError("UC table is empty. Execute DAG_UCS before this DAG.")

        uc_with_active_za = set(za_gdf["id_uc"].astype(int).tolist()) if not za_gdf.empty else set()

        transformed_path = self._run_dir(context) / "prodes_transformed.gpkg"
        if transformed_path.exists():
            transformed_path.unlink()

        source_records = 0
        transformed_records = 0
        row_offset = 0
        wrote_layer = False
        rejection_counts: dict[str, int] = {}

        for source_chunk in self._iter_sc_chunks(shapefile_path, chunk_size=CHUNK_SIZE_SOURCE):
            if source_chunk.empty:
                continue

            source_records += int(len(source_chunk))
            source_chunk = self._ensure_epsg_4674(source_chunk)
            source_chunk = self._repair_geometries(source_chunk)

            mapped = self._map_to_target_schema(
                source_chunk,
                dt_bronze=state["dt_bronze"],
                dag_version=context.dag_id,
                row_offset=row_offset,
            )
            row_offset += int(len(mapped))

            transformed_chunk, rejected_chunk = self._apply_spatial_rules(
                mapped=mapped,
                uc_gdf=uc_gdf,
                za_gdf=za_gdf,
                buffer_gdf=buffer_gdf,
                uc_with_active_za=uc_with_active_za,
            )

            for reason, frame in rejected_chunk.items():
                if frame is None or frame.empty:
                    continue
                rejection_counts[reason] = int(rejection_counts.get(reason, 0)) + int(len(frame))

            if transformed_chunk.empty:
                continue

            mode = "w" if not wrote_layer else "a"
            transformed_chunk.to_file(transformed_path, layer=PRODES_LAYER, driver="GPKG", mode=mode)
            wrote_layer = True
            transformed_records += int(len(transformed_chunk))

        state["source_records"] = int(source_records)
        state["transformed_records"] = int(transformed_records)
        state["transform_rejection_counts"] = rejection_counts
        state["transformed_path"] = str(transformed_path) if wrote_layer else None
        self._save_state(context, state)

        return self._result(
            context,
            status="transformed",
            source_records=int(source_records),
            transformed_records=int(transformed_records),
            transformed_path=str(transformed_path) if wrote_layer else None,
            rejection_counts=rejection_counts,
        )

    def load_postgres(self, context: TaskExecutionContext) -> dict[str, Any]:
        state = self._load_state(context)
        transformed_path_raw = state.get("transformed_path")
        transformed_path = Path(transformed_path_raw) if transformed_path_raw else None

        source_records = int(state.get("source_records", 0))
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
            raise ProdesPipelineError("PROJECT_DB_URL is required for load_postgres.")

        inserted_records = 0
        inserted_new_records = 0
        inserted_updated_records = 0
        skipped_invalid_geom = 0

        rejected_frames: dict[str, pd.DataFrame] = {}

        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                for chunk in self._iter_layer_chunks(
                    transformed_path,
                    layer=PRODES_LAYER,
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

                    if "id_prodes_original" not in chunk.columns:
                        raise ProdesPipelineError(
                            "Transformed dataset is missing 'id_prodes_original' required for duplicate control."
                        )

                    chunk["id_prodes_original"] = chunk["id_prodes_original"].astype("string")
                    chunk["id_prodes_original"] = chunk["id_prodes_original"].str.strip()

                    missing_id_mask = chunk["id_prodes_original"].isna() | (chunk["id_prodes_original"] == "")
                    missing_id_rows = chunk[missing_id_mask].copy()
                    chunk = chunk[~missing_id_mask].copy()

                    if not missing_id_rows.empty:
                        rejected_frames["MISSING_PRODES_ID"] = pd.concat(
                            [rejected_frames.get("MISSING_PRODES_ID", pd.DataFrame()), missing_id_rows],
                            axis=0,
                            ignore_index=True,
                        ).head(REJECTION_SAMPLE_LIMIT)

                    if chunk.empty:
                        continue

                    duplicate_mask = chunk.duplicated(
                        subset=[
                            "id_prodes_original",
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
                        rejected_frames["DUPLICATE_PRODES_ID_IN_BATCH"] = pd.concat(
                            [rejected_frames.get("DUPLICATE_PRODES_ID_IN_BATCH", pd.DataFrame()), duplicate_rows],
                            axis=0,
                            ignore_index=True,
                        ).head(REJECTION_SAMPLE_LIMIT)

                    if chunk.empty:
                        continue

                    candidate_ids = [
                        str(value)
                        for value in chunk["id_prodes_original"].dropna().astype(str).tolist()
                    ]
                    existing_keys: set[tuple[str, int, int, int, str]] = set()
                    if candidate_ids:
                        cur.execute(
                            """
                            SELECT id_prodes_original, id_uc,
                                   COALESCE(id_za_oficial, 0), COALESCE(id_buffer_abrangencia, 0),
                                   COALESCE(tipo_cruzamento, '')
                            FROM prodes_clip
                            WHERE id_prodes_original = ANY(%s);
                            """,
                            (candidate_ids,),
                        )
                        existing_keys = {
                            (str(db_row[0]), int(db_row[1]), int(db_row[2]), int(db_row[3]), str(db_row[4]))
                            for db_row in cur.fetchall()
                            if db_row[0] is not None
                        }

                    if existing_keys:
                        existing_mask = chunk.apply(
                            lambda item: (
                                str(item["id_prodes_original"]),
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
                            rejected_frames["DUPLICATE_PRODES_ID_IN_DB"] = pd.concat(
                                [rejected_frames.get("DUPLICATE_PRODES_ID_IN_DB", pd.DataFrame()), existing_rows],
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
                        INSERT INTO prodes_clip (
                            id_uc,
                            id_za_oficial,
                            id_buffer_abrangencia,
                            tipo_cruzamento,
                            id_prodes_original,
                            nr_ano,
                            ds_class_name,
                            area_km2,
                            area_intersecao_km2,
                            geom,
                            dt_bronze,
                            dt_silver,
                            dt_gold,
                            versao_dag
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            ST_Multi(ST_SetSRID(ST_GeomFromText(%s), 4674)),
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

        transformed = self._read_geodata(transformed_path, layer=PRODES_LAYER)

        silver_dir = self._build_layer_output_dir(
            layer_root=Path(self.config.medallion_silver_path),
            temporal_folder=state["temporal_folder"],
        )

        silver_path = silver_dir / "prodes_silver.gpkg"
        transformed.to_file(silver_path, layer=PRODES_LAYER, driver="GPKG")

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

        transformed = self._read_geodata(transformed_path, layer=PRODES_LAYER)

        gold_dir = self._build_layer_output_dir(
            layer_root=Path(self.config.medallion_gold_path),
            temporal_folder=state["temporal_folder"],
        )

        gold_geojson = gold_dir / "prodes_gold.geojson"
        transformed.to_file(gold_geojson, driver="GeoJSON")

        attributes = transformed.drop(columns=[transformed.geometry.name]).copy()
        attributes.to_csv(gold_dir / "prodes_gold_attributes.csv", index=False, encoding="utf-8")

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
            / "branch=prodes"
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
            "branch": "prodes",
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
            raise ProdesPipelineError(
                f"Missing pipeline state for stage '{context.stage}': {path}"
            )
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_branch_state(self, context: TaskExecutionContext, branch: str) -> dict[str, Any]:
        path = self._branch_state_path(context, branch=branch)
        if not path.exists():
            raise ProdesPipelineError(
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

        raise ProdesPipelineError(
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

        zip_candidates = sorted(batch_dir.glob("*.zip"))
        if not zip_candidates:
            raise InputValidationError(f"No shapefile or zip package found in {batch_dir}")

        package_path = zip_candidates[-1]
        extract_dir = run_dir / "source_zip"
        if extract_dir.exists():
            for child in extract_dir.iterdir():
                if child.is_file():
                    child.unlink()
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(package_path, "r") as archive:
            archive.extractall(extract_dir)

        shapefile = self._find_shapefile_in_directory(extract_dir)
        if shapefile is None:
            raise InputValidationError(f"Zip package has no shapefile: {package_path}")

        self._validate_shapefile_components(shapefile)
        return shapefile, "zip"

    def _find_shapefile_in_directory(self, directory: Path) -> Path | None:
        candidates = sorted(directory.glob("*.shp"))
        return candidates[0] if candidates else None

    def _validate_shapefile_components(self, shp_path: Path) -> None:
        missing = [ext for ext in REQUIRED_SHAPEFILE_EXTENSIONS if not shp_path.with_suffix(ext).exists()]
        if missing:
            raise InputValidationError(
                f"Missing shapefile components for {shp_path.name}: {missing}"
            )

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
                ["state", "STATE"],
            )
            if state_field is None:
                raise InputValidationError("Field state is required for early state filtering.")

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
            raise ProdesPipelineError("PROJECT_DB_URL is required for spatial reference lookup.")

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
                raise ProdesPipelineError("Spatial join output is missing UC identifier column.")

        uc_join["id_uc"] = pd.to_numeric(uc_join[uc_id_column], errors="coerce").astype("Int64")
        uc_join = uc_join[~uc_join["id_uc"].isna()].copy()
        if uc_join.empty:
            return gpd.GeoDataFrame(mapped_valid.iloc[0:0].copy(), geometry=geometry_column, crs=mapped.crs), invalid_count

        uc_join["id_uc"] = uc_join["id_uc"].astype(int)
        return self._intersect_with_uc_layer(uc_join, uc_gdf), invalid_count

    def _extract_zone_intersections_chunk(
        self,
        mapped: gpd.GeoDataFrame,
        uc_gdf: gpd.GeoDataFrame,
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

        # Sjoin direto contra geometria da ZA — sem exigir toque na UC primeiro.
        # Poligonos PRODES 100% na faixa de entorno (sem tocar a UC) passam aqui.
        valid_za, _ = self._sjoin_and_clip_zone(
            source=mapped_valid,
            zone_gdf=za_gdf,
            zone_id_column="id_za_oficial",
            tipo=TIPO_CRUZAMENTO_ZA,
        )

        # Sjoin direto contra geometria do Buffer de Abrangência, excluindo UCs que ja possuem ZA oficial ativa.
        buffer_eligible = (
            buffer_gdf[~buffer_gdf["id_uc"].isin(uc_with_active_za)].copy()
            if not buffer_gdf.empty
            else buffer_gdf
        )
        valid_buffer, _ = self._sjoin_and_clip_zone(
            source=mapped_valid,
            zone_gdf=buffer_eligible,
            zone_id_column="id_buffer_abrangencia",
            tipo=TIPO_CRUZAMENTO_BUFFER_ABRANGENCIA,
        )

        zone_parts: list[gpd.GeoDataFrame] = []
        if not valid_za.empty:
            zone_parts.append(valid_za)
        if not valid_buffer.empty:
            zone_parts.append(valid_buffer)

        if not zone_parts:
            return gpd.GeoDataFrame(mapped_valid.iloc[0:0].copy(), geometry=geometry_column, crs=mapped.crs), invalid_count

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
        for column_name in ("dt_bronze", "dt_silver", "dt_gold"):
            if column_name not in chunk_to_write.columns:
                continue

            self._log_temporal_column_profile(
                chunk_to_write,
                column_name,
                layer,
            )
            chunk_to_write[column_name] = chunk_to_write[column_name].apply(
                self._serialize_temporal_value_for_gpkg
            )

        mode = "w" if not wrote_layer else "a"
        chunk_to_write.to_file(path, layer=layer, driver="GPKG", mode=mode)
        return True

    def _log_temporal_column_profile(
        self,
        frame: gpd.GeoDataFrame,
        column_name: str,
        layer: str,
    ) -> None:
        series = frame[column_name]
        non_null = series[series.notna()]
        type_distribution: dict[str, int] = {}
        if not non_null.empty:
            raw_distribution = non_null.map(lambda value: type(value).__name__).value_counts().to_dict()
            type_distribution = {str(key): int(count) for key, count in raw_distribution.items()}

        self.logger.info(
            (
                "Preparing temporal column '%s' for GPKG write "
                "(layer=%s, dtype=%s, non_null=%s, type_distribution=%s)."
            ),
            column_name,
            layer,
            str(series.dtype),
            int(non_null.shape[0]),
            type_distribution,
        )

    def _serialize_temporal_value_for_gpkg(self, value: Any) -> str | None:
        if value is None:
            return None

        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass

        if isinstance(value, str):
            normalized = value.strip()
            return normalized if normalized else None

        if isinstance(value, pd.Timestamp):
            return value.isoformat()

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        isoformat = getattr(value, "isoformat", None)
        if callable(isoformat):
            try:
                serialized = isoformat()
                return serialized if isinstance(serialized, str) else str(serialized)
            except Exception:  # defensive fallback for custom objects
                pass

        coerced = pd.to_datetime(value, errors="coerce")
        if not pd.isna(coerced):
            return coerced.isoformat()

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

        if uc_join.empty:
            rejected["NO_UC_INTERSECTION"] = mapped_valid.copy()
            return gpd.GeoDataFrame(mapped_valid.iloc[0:0].copy(), geometry=geometry_column, crs=mapped.crs), rejected

        uc_id_column = "id_uc"
        if uc_id_column not in uc_join.columns:
            if "id_uc_right" in uc_join.columns:
                uc_id_column = "id_uc_right"
            elif "id_uc_left" in uc_join.columns:
                uc_id_column = "id_uc_left"
            else:
                raise ProdesPipelineError("Spatial join output is missing UC identifier column.")

        uc_join["id_uc"] = pd.to_numeric(uc_join[uc_id_column], errors="coerce").astype("Int64")
        uc_join = uc_join[~uc_join["id_uc"].isna()].copy()
        uc_join["id_uc"] = uc_join["id_uc"].astype(int)

        matched_by_uc = set(uc_join["source_row_id"].tolist()) if not uc_join.empty else set()
        unmatched_by_uc = mapped_valid[~mapped_valid["source_row_id"].isin(matched_by_uc)].copy()
        if not unmatched_by_uc.empty:
            rejected["NO_UC_INTERSECTION"] = unmatched_by_uc

        valid_parts: list[gpd.GeoDataFrame] = []

        with_za = uc_join[uc_join["id_uc"].isin(uc_with_active_za)].copy()
        with_buffer = uc_join[~uc_join["id_uc"].isin(uc_with_active_za)].copy()

        valid_za, rejected_za = self._intersect_with_zone_layer(
            joined=with_za,
            zone_gdf=za_gdf,
            zone_id_column="id_za_oficial",
            tipo=TIPO_CRUZAMENTO_ZA,
        )
        valid_buffer, rejected_buffer = self._intersect_with_zone_layer(
            joined=with_buffer,
            zone_gdf=buffer_gdf,
            zone_id_column="id_buffer_abrangencia",
            tipo=TIPO_CRUZAMENTO_BUFFER_ABRANGENCIA,
        )

        for reason, frame in {**rejected_za, **rejected_buffer}.items():
            if frame is None or frame.empty:
                continue
            rejected[reason] = pd.concat(
                [rejected.get(reason, pd.DataFrame()), frame],
                axis=0,
                ignore_index=True,
            ).head(REJECTION_SAMPLE_LIMIT)

        if not valid_za.empty:
            valid_parts.append(valid_za)
        if not valid_buffer.empty:
            valid_parts.append(valid_buffer)

        valid_uc = self._intersect_with_uc_layer(uc_join, uc_gdf)
        if not valid_uc.empty:
            valid_parts.append(valid_uc)

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
        merged[geometry_column] = alert_geometry.intersection(uc_geometry).apply(self._normalize_polygon_to_multipolygon)

        valid = merged[
            ~(merged[geometry_column].isna() | gpd.GeoSeries(merged[geometry_column], crs=joined.crs).is_empty)
        ].copy()
        if valid.empty:
            return gpd.GeoDataFrame(joined.iloc[0:0].copy(), geometry=geometry_column, crs=joined.crs)

        valid["id_za_oficial"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
        valid["id_buffer_abrangencia"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
        valid["tipo_cruzamento"] = TIPO_CRUZAMENTO_UC

        valid["area_intersecao_km2"] = (
            gpd.GeoSeries(valid[geometry_column], crs=joined.crs)
            .to_crs(epsg=32722)
            .area / 1_000_000
        )

        return self._project_prodes_columns(valid, geometry_column)

    def _intersect_with_zone_layer(
        self,
        joined: gpd.GeoDataFrame,
        zone_gdf: gpd.GeoDataFrame,
        zone_id_column: str,
        tipo: str,
    ) -> tuple[gpd.GeoDataFrame, dict[str, pd.DataFrame]]:
        rejected: dict[str, pd.DataFrame] = {}
        if joined.empty:
            geometry_column = str(joined.geometry.name) if isinstance(joined, gpd.GeoDataFrame) else "geometry"
            empty = gpd.GeoDataFrame(joined.copy(), geometry=geometry_column, crs=getattr(joined, "crs", None))
            return empty, rejected

        geometry_column = str(joined.geometry.name)
        zone_geom_column = str(zone_gdf.geometry.name) if not zone_gdf.empty else "geometry"

        zone_view = (
            zone_gdf[["id_uc", zone_id_column, zone_geom_column]]
            .rename(columns={zone_geom_column: "zone_geometry"})
            .copy()
            if not zone_gdf.empty
            else pd.DataFrame(columns=["id_uc", zone_id_column, "zone_geometry"])
        )

        merged = joined.merge(zone_view, on="id_uc", how="left")

        no_zone = merged[merged[zone_id_column].isna()].copy() if zone_id_column in merged.columns else merged.copy()
        if not no_zone.empty:
            rejected["NO_ACTIVE_ZONE_FOR_UC"] = no_zone

        if zone_id_column not in merged.columns:
            return gpd.GeoDataFrame(joined.iloc[0:0].copy(), geometry=geometry_column, crs=joined.crs), rejected

        candidates = merged[~merged[zone_id_column].isna()].copy()
        if candidates.empty:
            return gpd.GeoDataFrame(joined.iloc[0:0].copy(), geometry=geometry_column, crs=joined.crs), rejected

        alert_geometry = gpd.GeoSeries(candidates[geometry_column], crs=joined.crs)
        zone_geometry = gpd.GeoSeries(candidates["zone_geometry"], crs=joined.crs)

        intersected = alert_geometry.intersection(zone_geometry)
        candidates[geometry_column] = intersected.apply(self._normalize_polygon_to_multipolygon)

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

        if tipo == TIPO_CRUZAMENTO_ZA:
            valid["id_za_oficial"] = pd.to_numeric(valid[zone_id_column], errors="coerce").astype("Int64")
            valid["id_buffer_abrangencia"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
        else:
            valid["id_buffer_abrangencia"] = pd.to_numeric(valid[zone_id_column], errors="coerce").astype("Int64")
            valid["id_za_oficial"] = pd.Series([pd.NA] * len(valid), dtype="Int64")

        valid["tipo_cruzamento"] = tipo
        valid["area_intersecao_km2"] = (
            gpd.GeoSeries(valid[geometry_column], crs=joined.crs)
            .to_crs(epsg=32722)
            .area / 1_000_000
        )

        return self._project_prodes_columns(valid, geometry_column), rejected

    def _sjoin_and_clip_zone(
        self,
        source: gpd.GeoDataFrame,
        zone_gdf: gpd.GeoDataFrame,
        zone_id_column: str,
        tipo: str,
    ) -> tuple[gpd.GeoDataFrame, dict[str, pd.DataFrame]]:
        """Sjoin direto contra a geometria da zona (sem pre-filtro por UC).

        Ao contrario de _intersect_with_zone_layer, nao exige que o poligono
        fonte ja tenha passado por um sjoin com a UC. O id_uc e obtido
        diretamente dos atributos da propria tabela de zona (ZA ou Buffer de Abrangência).
        """
        rejected: dict[str, pd.DataFrame] = {}
        geometry_column = str(source.geometry.name)

        if zone_gdf.empty:
            return gpd.GeoDataFrame(source.iloc[0:0].copy(), geometry=geometry_column, crs=source.crs), rejected

        zone_geom_column = str(zone_gdf.geometry.name)

        # Spatial join: descobre quais poligonos PRODES intersectam a zona.
        # source nao tem id_uc nem zone_id_column, portanto nao ha conflito de nomes.
        zone_join = gpd.sjoin(
            source,
            zone_gdf[["id_uc", zone_id_column, zone_geom_column]],
            how="inner",
            predicate="intersects",
        )

        if zone_join.empty:
            return gpd.GeoDataFrame(source.iloc[0:0].copy(), geometry=geometry_column, crs=source.crs), rejected

        # Resolve possiveis sufixos adicionados pelo sjoin.
        uc_col = "id_uc" if "id_uc" in zone_join.columns else (
            "id_uc_right" if "id_uc_right" in zone_join.columns else None
        )
        zone_col = zone_id_column if zone_id_column in zone_join.columns else (
            f"{zone_id_column}_right" if f"{zone_id_column}_right" in zone_join.columns else None
        )
        if uc_col is None or zone_col is None:
            return gpd.GeoDataFrame(source.iloc[0:0].copy(), geometry=geometry_column, crs=source.crs), rejected

        zone_join["id_uc"] = pd.to_numeric(zone_join[uc_col], errors="coerce").astype("Int64")
        zone_join[zone_id_column] = pd.to_numeric(zone_join[zone_col], errors="coerce").astype("Int64")
        zone_join = zone_join[
            ~zone_join["id_uc"].isna() & ~zone_join[zone_id_column].isna()
        ].copy()

        if zone_join.empty:
            return gpd.GeoDataFrame(source.iloc[0:0].copy(), geometry=geometry_column, crs=source.crs), rejected

        zone_join["id_uc"] = zone_join["id_uc"].astype(int)

        # Recupera a geometria real da zona para fazer o clip (sjoin nao a repassa).
        zone_geom_lookup = (
            zone_gdf[[zone_id_column, zone_geom_column]]
            .copy()
            .rename(columns={zone_geom_column: "_zone_geometry"})
        )
        zone_geom_lookup[zone_id_column] = pd.to_numeric(
            zone_geom_lookup[zone_id_column], errors="coerce"
        ).astype("Int64")

        merged = zone_join.merge(zone_geom_lookup, on=zone_id_column, how="left")
        merged = merged[~merged["_zone_geometry"].isna()].copy()

        if merged.empty:
            return gpd.GeoDataFrame(source.iloc[0:0].copy(), geometry=geometry_column, crs=source.crs), rejected

        # Clip: intersecta geometria PRODES com geometria da zona.
        alert_geom = gpd.GeoSeries(merged[geometry_column], crs=source.crs)
        zone_geom = gpd.GeoSeries(merged["_zone_geometry"], crs=source.crs)
        merged[geometry_column] = alert_geom.intersection(zone_geom).apply(self._normalize_polygon_to_multipolygon)

        valid = merged[
            ~(merged[geometry_column].isna() | gpd.GeoSeries(merged[geometry_column], crs=source.crs).is_empty)
        ].copy()

        if valid.empty:
            return gpd.GeoDataFrame(source.iloc[0:0].copy(), geometry=geometry_column, crs=source.crs), rejected

        if tipo == TIPO_CRUZAMENTO_ZA:
            valid["id_za_oficial"] = pd.to_numeric(valid[zone_id_column], errors="coerce").astype("Int64")
            valid["id_buffer_abrangencia"] = pd.Series([pd.NA] * len(valid), dtype="Int64")
        else:
            valid["id_buffer_abrangencia"] = pd.to_numeric(valid[zone_id_column], errors="coerce").astype("Int64")
            valid["id_za_oficial"] = pd.Series([pd.NA] * len(valid), dtype="Int64")

        valid["tipo_cruzamento"] = tipo
        valid["area_intersecao_km2"] = (
            gpd.GeoSeries(valid[geometry_column], crs=source.crs)
            .to_crs(epsg=32722)
            .area / 1_000_000
        )

        # _project_prodes_columns descarta colunas extras (ex.: _zone_geometry, index_right).
        return self._project_prodes_columns(
            gpd.GeoDataFrame(valid, geometry=geometry_column, crs=source.crs),
            geometry_column,
        ), rejected

    def _project_prodes_columns(self, frame: gpd.GeoDataFrame, geometry_column: str) -> gpd.GeoDataFrame:
        keep_columns = [
            "source_row_id",
            "id_uc",
            "id_za_oficial",
            "id_buffer_abrangencia",
            "tipo_cruzamento",
            "id_prodes_original",
            "nr_ano",
            "ds_class_name",
            "area_km2",
            "area_intersecao_km2",
            "dt_bronze",
            "dt_silver",
            "dt_gold",
            "versao_dag",
            geometry_column,
        ]
        projected = frame[[col for col in keep_columns if col in frame.columns]].copy()
        return gpd.GeoDataFrame(projected, geometry=geometry_column, crs=frame.crs)

    def _map_to_target_schema(
        self,
        gdf: gpd.GeoDataFrame,
        dt_bronze: str,
        dag_version: str,
        row_offset: int,
    ) -> gpd.GeoDataFrame:
        mapped = pd.DataFrame(index=gdf.index)

        source_id = self._get_source_value(gdf, list(PRODES_ID_SOURCE_FIELDS))
        if source_id is not None:
            mapped["id_prodes_original"] = source_id.astype("string").str.strip()
        else:
            mapped["id_prodes_original"] = pd.Series([pd.NA] * len(gdf), dtype="string")

        mapped["nr_ano"] = pd.to_numeric(
            self._get_source_value(gdf, ["year", "YEAR", "ano", "ANO"]),
            errors="coerce",
        ).astype("Int64")
        mapped["ds_class_name"] = self._get_source_value(gdf, ["main_class", "MAIN_CLASS", "class_name"])
        mapped["area_km2"] = pd.to_numeric(
            self._get_source_value(gdf, ["area_km", "AREA_KM", "area_km2", "AREA_KM2"]),
            errors="coerce",
        )
        mapped["area_intersecao_km2"] = mapped["area_km2"]

        mapped["source_row_id"] = pd.Series(
            range(row_offset, row_offset + len(gdf)),
            index=gdf.index,
            dtype="Int64",
        )
        mapped["dt_bronze"] = pd.to_datetime(dt_bronze, errors="coerce").date()
        mapped["dt_silver"] = None
        mapped["dt_gold"] = None
        mapped["versao_dag"] = dag_version

        mapped["geometry"] = gdf.geometry.apply(self._normalize_polygon_to_multipolygon)
        return gpd.GeoDataFrame(mapped, geometry="geometry", crs=gdf.crs)

    def _row_to_db_tuple(self, row: pd.Series, geometry_column: str) -> tuple[Any, ...]:
        geometry_value = row.get(geometry_column)
        geometry_wkt = geometry_value.wkt if geometry_value is not None and not pd.isna(geometry_value) else None

        return (
            int(row["id_uc"]),
            self._none_if_nan(row.get("id_za_oficial")),
            self._none_if_nan(row.get("id_buffer_abrangencia")),
            self._none_if_nan(row.get("tipo_cruzamento")),
            self._none_if_nan(row.get("id_prodes_original")),
            self._none_if_nan(row.get("nr_ano")),
            self._none_if_nan(row.get("ds_class_name")),
            self._none_if_nan(row.get("area_km2")),
            self._none_if_nan(row.get("area_intersecao_km2")),
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

    def _normalize_polygon_to_multipolygon(self, geom):
        if geom is None:
            return None

        fixed = self._make_valid_geometry(geom)
        if fixed is None or fixed.is_empty:
            return None

        if isinstance(fixed, Polygon):
            return MultiPolygon([fixed])

        if isinstance(fixed, MultiPolygon):
            polygons = [poly for poly in fixed.geoms if isinstance(poly, Polygon) and not poly.is_empty]
            if not polygons:
                return None
            return MultiPolygon(polygons)

        if isinstance(fixed, GeometryCollection):
            polygons = [
                element
                for element in fixed.geoms
                if isinstance(element, Polygon) and not element.is_empty
            ]
            if not polygons:
                return None
            return MultiPolygon(polygons)

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

    def _none_if_nan(self, value: Any) -> Any:
        if value is None:
            return None
        if pd.isna(value):
            return None
        return value

    def _is_target_state(self, value: Any) -> bool:
        clean = self._none_if_nan(value)
        if clean is None:
            return False

        normalized = self._normalize_name(str(clean)).upper()
        return normalized in TARGET_STATES_NORMALIZED
