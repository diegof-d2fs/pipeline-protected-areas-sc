"""Domain service for incremental and historical NASA FIRMS ingestion."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from shapely import prepare, wkb
from shapely.prepared import prep
from shapely.validation import make_valid

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.firms_client import FirmsAreaClient, FirmsAreaResponse

LOGGER = logging.getLogger(__name__)


class FirmsPipelineError(RuntimeError):
    """Report a FIRMS pipeline contract or publication failure."""


# Maior intervalo aceito pela FIRMS Area API numa única consulta.
MAX_WINDOW_DAYS = 5

@dataclass(frozen=True)
class FirmsWindow:
    """Identify one bounded product interval processed as an atomic unit."""

    source_product: str
    start_date: date
    end_date: date
    mode: str

    def __post_init__(self) -> None:
        days = (self.end_date - self.start_date).days + 1
        if not 1 <= days <= MAX_WINDOW_DAYS:
            raise ValueError("A FIRMS window must contain between one and five days.")
        if self.mode not in {"incremental", "backfill"}:
            raise ValueError("FIRMS mode must be incremental or backfill.")

    @property
    def day_range(self) -> int:
        """Return the inclusive number of days requested from the Area API."""
        return (self.end_date - self.start_date).days + 1

    def to_conf(self) -> dict[str, str]:
        """Serialize the window into an Airflow mapping-safe payload."""
        return {
            "source_product": self.source_product,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "mode": self.mode,
        }


class FirmsPipelineService:
    """Ingest, normalize, relate and publish FIRMS detections with replay safety."""

    PIPELINE_VERSION = "1.0.0"
    PUBLICATION_RULE_VERSION = "1"

    def __init__(
        self,
        config: PipelineConfig | None = None,
        client_factory: Callable[[], FirmsAreaClient] | None = None,
    ) -> None:
        self.config = config or PipelineConfig.from_env()
        self._client_factory = client_factory
        self._state_boundary_cache: tuple[tuple[str, str], gpd.GeoDataFrame] | None = None
        self._reference_cache: tuple[tuple, tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]] | None = None

    def resolve_incremental_windows(self, context: TaskExecutionContext) -> list[dict[str, str]]:
        """Cover the whole schedule period, plus overlap, for every configured NRT product.

        The period ends at `end_date` (the last complete day before the run) and is split into
        windows of at most five days, the Area API limit; detections replayed by the overlap are
        deduplicated downstream.
        """
        conf = context.conf or {}
        end = self._parse_date(conf.get("end_date") or context.logical_date[:10], "end_date")
        overlap = int(conf.get("overlap_days", self.config.firms_overlap_days))
        if not 0 <= overlap <= 4:
            raise FirmsPipelineError("FIRMS overlap_days must be between zero and four.")
        period = int(conf.get("period_days", self.config.firms_incremental_period_days))
        if not 1 <= period <= 31:
            raise FirmsPipelineError("FIRMS period_days must be between one and 31.")
        start = end - timedelta(days=period - 1 + overlap)
        spans: list[tuple[date, date]] = []
        cursor = start
        while cursor <= end:
            span_end = min(cursor + timedelta(days=MAX_WINDOW_DAYS - 1), end)
            spans.append((cursor, span_end))
            cursor = span_end + timedelta(days=1)
        products = conf.get("products") or self.config.firms_daily_products
        if isinstance(products, str):
            products = [item.strip() for item in products.split(",") if item.strip()]
        windows = [
            FirmsWindow(str(product), span_start, span_end, "incremental")
            for product in products
            for span_start, span_end in spans
        ]
        if not windows:
            raise FirmsPipelineError("At least one FIRMS incremental product is required.")
        return [{"window_conf": window.to_conf()} for window in windows]

    def plan_backfill_windows(self, context: TaskExecutionContext) -> list[dict[str, str]]:
        """Seed resumable historical windows and return a bounded pending page."""
        if not self.config.project_db_url:
            raise FirmsPipelineError("PROJECT_DB_URL is required for FIRMS backfill planning.")
        conf = context.conf or {}
        start = self._parse_date(conf.get("start_date", "2015-01-01"), "start_date")
        end = self._parse_date(
            conf.get("end_date", (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()),
            "end_date",
        )
        if end < start:
            raise FirmsPipelineError("FIRMS backfill end_date must not precede start_date.")
        products = conf.get("products") or self.config.firms_backfill_products
        if isinstance(products, str):
            products = [item.strip() for item in products.split(",") if item.strip()]
        page_size = int(conf.get("batch_size", self.config.firms_backfill_batch_size))
        if not 1 <= page_size <= 100:
            raise FirmsPipelineError("FIRMS backfill batch_size must be between one and 100.")
        availability = conf.get("availability") or self._client().fetch_availability()
        product_ranges = {}
        for product in products:
            if product not in availability:
                raise FirmsPipelineError(f"FIRMS availability is missing {product}.")
            first = self._parse_date(availability[product].get("min_date"), "min_date")
            last = self._parse_date(availability[product].get("max_date"), "max_date")
            if last < first:
                raise FirmsPipelineError(f"FIRMS availability dates are invalid for {product}.")
            product_ranges[str(product)] = (first, last)
        rows: list[tuple[str, date, date]] = []
        for product in products:
            cursor = start
            while cursor <= end:
                window_end = min(cursor + timedelta(days=4), end)
                first, last = product_ranges[str(product)]
                if window_end >= first and cursor <= last:
                    rows.append((str(product), cursor, window_end))
                cursor = window_end + timedelta(days=1)
        with psycopg2.connect(self.config.project_db_url) as connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    INSERT INTO firms_backfill_window (source_product, start_date, end_date)
                    VALUES %s
                    ON CONFLICT (source_product, start_date, end_date) DO NOTHING
                    """,
                    rows,
                    page_size=500,
                )
                for product, (first, last) in product_ranges.items():
                    cursor.execute(
                        """
                        UPDATE firms_backfill_window
                        SET processing_state='PENDING', last_error=NULL, updated_at=CURRENT_TIMESTAMP
                        WHERE source_product=%s AND start_date >= %s AND end_date <= %s
                          AND processing_state='SKIPPED_UNAVAILABLE'
                          AND end_date >= %s AND start_date <= %s
                        """,
                        (product, start, end, first, last),
                    )
                    cursor.execute(
                        """
                        UPDATE firms_backfill_window
                        SET processing_state='SKIPPED_UNAVAILABLE',
                            last_error=%s, updated_at=CURRENT_TIMESTAMP
                        WHERE source_product=%s AND start_date >= %s AND end_date <= %s
                          AND processing_state IN ('PENDING', 'FAILED')
                          AND (end_date < %s OR start_date > %s)
                        """,
                        (f"Outside NASA availability {first.isoformat()}..{last.isoformat()}",
                         product, start, end, first, last),
                    )
                selection_year = None
                if conf.get("yearly_batches", False):
                    cursor.execute(
                        """
                        SELECT MIN(EXTRACT(YEAR FROM start_date))
                        FROM firms_backfill_window
                        WHERE processing_state IN ('PENDING', 'FAILED')
                          AND start_date >= %s AND end_date <= %s
                          AND source_product = ANY(%s)
                        """,
                        (start, end, list(products)),
                    )
                    selection_year = cursor.fetchone()[0]
                cursor.execute(
                    """
                    SELECT source_product, start_date, end_date
                    FROM firms_backfill_window
                    WHERE processing_state IN ('PENDING', 'FAILED')
                      AND start_date >= %s AND end_date <= %s
                      AND source_product = ANY(%s)
                      AND (%s IS NULL OR EXTRACT(YEAR FROM start_date) = %s)
                    ORDER BY start_date, source_product
                    LIMIT %s
                    """,
                    (start, end, list(products), selection_year, selection_year, page_size),
                )
                pending = cursor.fetchall()
        return [
            {
                "window_conf": FirmsWindow(product, row_start, row_end, "backfill").to_conf()
            }
            for product, row_start, row_end in pending
        ]

    def process_window(
        self,
        context: TaskExecutionContext,
        *,
        suppress_errors: bool = False,
    ) -> dict[str, Any]:
        """Execute a complete Medallion and PostGIS transaction for one source window.

        When ``suppress_errors`` is true, failures become sanitized result payloads so a
        coordinator can apply multi-source success semantics. Backfill state is advanced
        transactionally around processing and remains resumable after failure.
        """
        window = self._window(context)
        if window.mode == "backfill":
            self._set_backfill_running(window, context.run_id)
        bronze: dict[str, Any] | None = None
        try:
            response, bronze = self._acquire(window, context)
            detections, metrics = self._normalize(response.content, window, context, bronze)
            silver = self._publish_silver(detections, window, context, bronze)
            relations = self._build_relations(detections)
            gold = self._publish_gold(relations, window, context, bronze)
            inserted = self._load_postgres(relations, window, context, bronze, silver, gold)
            result = {
                "status": "SUCCESS",
                "source_product": window.source_product,
                "start_date": window.start_date.isoformat(),
                "end_date": window.end_date.isoformat(),
                "mode": window.mode,
                "source_records": metrics["source_records"],
                "silver_records": len(detections),
                "gold_relations": len(relations),
                "inserted_relations": inserted,
                "bronze_manifest_key": bronze["manifest_key"],
                "source_checksum": bronze["checksum_sha256"],
            }
            self._write_quality(context, window, metrics, result)
            if window.mode == "backfill":
                self._set_backfill_published(window, bronze, context.run_id)
            return result
        except Exception as exc:
            error = self._sanitize_error(exc)
            if window.mode == "backfill":
                self._set_backfill_failed(window, context.run_id, error, bronze)
            result = {
                "status": "FAILED",
                "source_product": window.source_product,
                "start_date": window.start_date.isoformat(),
                "end_date": window.end_date.isoformat(),
                "mode": window.mode,
                "error": error,
            }
            self._write_quality(context, window, {}, result)
            if suppress_errors:
                return result
            raise FirmsPipelineError(error) from exc

    def summarize_incremental(
        self,
        context: TaskExecutionContext,
        results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Apply multi-source availability semantics to mapped window results."""
        succeeded = [result for result in results if result.get("status") == "SUCCESS"]
        failed = [result for result in results if result.get("status") != "SUCCESS"]
        if not succeeded:
            self._write_run_quality(
                context,
                {
                    "status": "FAILED",
                    "successful_sources": [],
                    "failed_sources": sorted({str(item.get("source_product")) for item in failed}),
                    "source_records": 0,
                    "silver_records": 0,
                    "gold_relations": 0,
                    "inserted_relations": 0,
                },
            )
            raise FirmsPipelineError("All configured FIRMS incremental sources failed.")
        summary = {
            "status": "DEGRADED" if failed else "SUCCESS",
            "successful_sources": sorted({item["source_product"] for item in succeeded}),
            "failed_sources": sorted({str(item.get("source_product")) for item in failed}),
            "source_records": sum(int(item.get("source_records", 0)) for item in succeeded),
            "silver_records": sum(int(item.get("silver_records", 0)) for item in succeeded),
            "gold_relations": sum(int(item.get("gold_relations", 0)) for item in succeeded),
            "inserted_relations": sum(int(item.get("inserted_relations", 0)) for item in succeeded),
            "run_id": context.run_id,
        }
        self._write_run_quality(context, summary)
        if failed:
            LOGGER.warning(
                "FIRMS incremental run completed with unavailable windows: %s",
                ", ".join(summary["failed_sources"]),
            )
        return summary

    def summarize_backfill(
        self,
        context: TaskExecutionContext,
        results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Summarize one bounded backfill page and fail visibly when a window failed."""
        failures = [result for result in results if result.get("status") != "SUCCESS"]
        summary = {
            "status": "FAILED" if failures else "SUCCESS",
            "processed_windows": len(results),
            "failed_windows": len(failures),
            "inserted_relations": sum(int(item.get("inserted_relations", 0)) for item in results),
            "run_id": context.run_id,
        }
        self._write_run_quality(context, summary)
        if failures:
            raise FirmsPipelineError(
                f"{len(failures)} FIRMS backfill window(s) failed and remain resumable."
            )
        return summary

    def _acquire(
        self,
        window: FirmsWindow,
        context: TaskExecutionContext,
    ) -> tuple[FirmsAreaResponse, dict[str, Any]]:
        if window.mode == "backfill":
            replay = self._stored_backfill_response(window)
            if replay is not None:
                return replay
        target = self._partition(self.config.medallion_bronze_path, window, context)
        manifest_path = target / "manifest.json"
        response_path = target / "response.csv"
        if manifest_path.is_file() and response_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            checksum = self._sha256(response_path.read_bytes())
            if checksum != manifest.get("checksum_sha256"):
                raise FirmsPipelineError("FIRMS Bronze response checksum mismatch.")
            return (
                FirmsAreaResponse(
                    content=response_path.read_bytes(),
                    http_status=int(manifest["http_status"]),
                    content_type=str(manifest["content_type"]),
                    requested_at=str(manifest["requested_at"]),
                    received_at=str(manifest["received_at"]),
                    sanitized_endpoint=str(manifest["sanitized_endpoint"]),
                ),
                manifest,
            )
        if target.exists():
            raise FirmsPipelineError(f"Incomplete FIRMS Bronze partition: {target}")
        response = self._client().fetch_csv(
            source_product=window.source_product,
            bbox=self.config.firms_request_bbox,
            start_date=window.start_date.isoformat(),
            day_range=window.day_range,
        )
        checksum = self._sha256(response.content)
        manifest = {
            "schema_version": "1.0",
            "domain": "firms",
            "run_id": context.run_id,
            "source_product": window.source_product,
            "source_processing": self._source_processing(window.source_product),
            "request_bbox": self.config.firms_request_bbox,
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "day_range": window.day_range,
            "requested_at": response.requested_at,
            "received_at": response.received_at,
            "http_status": response.http_status,
            "content_type": response.content_type,
            "object_key": self._relative(response_path),
            "manifest_key": self._relative(manifest_path),
            "checksum_sha256": checksum,
            "bytes": len(response.content),
            "record_count": self._csv_record_count(response.content),
            "sanitized_endpoint": response.sanitized_endpoint,
            "pipeline_version": self.PIPELINE_VERSION,
        }
        self._publish_directory(
            target,
            {"response.csv": response.content, "manifest.json": self._json_bytes(manifest)},
        )
        return response, manifest

    def _normalize(
        self,
        content: bytes,
        window: FirmsWindow,
        context: TaskExecutionContext,
        bronze: dict[str, Any],
    ) -> tuple[gpd.GeoDataFrame, dict[str, int]]:
        try:
            source = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
        except Exception as exc:
            raise FirmsPipelineError("FIRMS CSV could not be parsed.") from exc
        required = {"latitude", "longitude", "acq_date", "acq_time", "confidence"}
        missing = sorted(required - set(source.columns))
        if missing:
            raise FirmsPipelineError("FIRMS CSV is missing columns: " + ", ".join(missing))
        source_records = len(source)
        if source.empty:
            return self._empty_detections(), {
                "source_records": 0,
                "invalid_records": 0,
                "outside_sc_records": 0,
                "confidence_filtered_records": 0,
            }
        latitude = pd.to_numeric(source["latitude"], errors="coerce")
        longitude = pd.to_numeric(source["longitude"], errors="coerce")
        times = source["acq_time"].str.replace(r"\.0$", "", regex=True).str.zfill(4)
        acquired = pd.to_datetime(
            source["acq_date"].str.strip() + times,
            format="%Y-%m-%d%H%M",
            errors="coerce",
            utc=True,
        )
        valid = (
            latitude.between(-90, 90)
            & longitude.between(-180, 180)
            & acquired.notna()
        )
        invalid_records = int((~valid).sum())
        frame = source.loc[valid].copy()
        frame["latitude"] = latitude.loc[valid].astype(float)
        frame["longitude"] = longitude.loc[valid].astype(float)
        frame["acquired_at_utc"] = acquired.loc[valid]
        frame = gpd.GeoDataFrame(
            frame,
            geometry=gpd.points_from_xy(frame["longitude"], frame["latitude"]),
            crs="EPSG:4326",
        ).to_crs("EPSG:4674")
        boundary = self._load_state_boundary().to_crs(frame.crs).geometry.unary_union
        inside = frame.geometry.apply(prep(boundary).covers)
        outside_sc_records = int((~inside).sum())
        frame = frame.loc[inside].copy()
        viirs = "VIIRS" in window.source_product.upper()
        if viirs:
            confidence = frame["confidence"].str.strip().str.lower()
            accepted_confidence = confidence.isin({"l", "n", "h"})
            frame["confidence_raw"] = confidence
            frame["confidence_scheme"] = "VIIRS_CATEGORY"
            frame["confidence_score"] = None
            frame["confidence_class"] = confidence.map(
                {"l": "LOW", "n": "NOMINAL", "h": "HIGH"}
            )
            frame["publish_gold"] = confidence.eq("h")
        else:
            confidence_score = pd.to_numeric(frame["confidence"], errors="coerce")
            accepted_confidence = confidence_score.ge(35) & confidence_score.le(100)
            frame["confidence_raw"] = frame["confidence"].str.strip()
            frame["confidence_scheme"] = "MODIS_PERCENT"
            frame["confidence_score"] = confidence_score
            frame["confidence_class"] = confidence_score.apply(
                lambda value: "HIGH" if pd.notna(value) and value > 70 else "NOMINAL"
            )
            frame["publish_gold"] = confidence_score.gt(70)
        confidence_filtered_records = int((~accepted_confidence).sum())
        frame = frame.loc[accepted_confidence].copy()
        if frame.empty:
            return self._empty_detections(), {
                "source_records": source_records,
                "invalid_records": invalid_records,
                "outside_sc_records": outside_sc_records,
                "confidence_filtered_records": confidence_filtered_records,
            }
        frame["source_product"] = window.source_product
        frame["source_version"] = frame.get("version", "").astype(str) if "version" in frame else ""
        frame["source_processing"] = self._source_processing(window.source_product)
        frame["satellite"] = frame.get("satellite", "").astype(str) if "satellite" in frame else ""
        frame["instrument"] = frame.get("instrument", "").astype(str) if "instrument" in frame else ""
        frame["frp_mw"] = self._numeric(frame, "frp")
        frame["brightness_kelvin"] = self._numeric(
            frame, "bright_ti4" if "bright_ti4" in frame else "brightness"
        )
        frame["brightness_secondary_kelvin"] = self._numeric(
            frame, "bright_ti5" if "bright_ti5" in frame else "bright_t31"
        )
        frame["scan_km"] = self._numeric(frame, "scan")
        frame["track_km"] = self._numeric(frame, "track")
        frame["daynight"] = self._string(frame, "daynight").str.upper().where(
            self._string(frame, "daynight").str.upper().isin({"D", "N"}), None
        )
        frame["detection_type"] = self._string(frame, "type").replace("", None)
        frame["source_checksum"] = bronze["checksum_sha256"]
        frame["run_id"] = context.run_id
        frame["publication_rule_version"] = self.PUBLICATION_RULE_VERSION
        frame["detection_id"] = frame.apply(self._detection_id, axis=1)
        columns = list(self._empty_detections().columns)
        return frame[columns].reset_index(drop=True), {
            "source_records": source_records,
            "invalid_records": invalid_records,
            "outside_sc_records": outside_sc_records,
            "confidence_filtered_records": confidence_filtered_records,
        }

    def recross_published(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Relate every published FIRMS detection to the currently active areas.

        Runs after a cadastral change (new or replaced UC, official ZA or Buffer de Abrangência)
        without calling the FIRMS API: detections already normalized in the Silver are related
        with the same rule as the regular load, and relations that already exist are ignored by
        the unique key, so only the changed areas receive new rows. The relations created by
        this run get their own Gold partition.
        """
        silver_root = Path(self.config.medallion_silver_path)
        partitions = sorted(
            manifest.parent
            for manifest in (silver_root / "firms").glob(
                "product=*/start_date=*/end_date=*/run_id=*/manifest.json"
            )
        )
        reference = self._load_reference_layers() if partitions else None
        relations_total = 0
        inserted_total = 0
        for partition in partitions:
            detections = gpd.read_parquet(partition / "detections.parquet")
            relations = self._build_relations(detections, reference)
            if relations.empty:
                continue
            relative = partition.relative_to(silver_root)
            bronze = json.loads(
                (Path(self.config.medallion_bronze_path) / relative / "manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            silver = json.loads((partition / "manifest.json").read_text(encoding="utf-8"))
            gold = self._publish_recross_gold(relations, relative, context, silver)
            relations_total += len(relations)
            inserted_total += self._load_postgres(relations, None, context, bronze, silver, gold)
        summary = {
            "status": "SUCCESS",
            "mode": "recross",
            "silver_partitions": len(partitions),
            "gold_relations": relations_total,
            "inserted_relations": inserted_total,
            "run_id": context.run_id,
        }
        self._write_run_quality(context, summary)
        return summary

    def _publish_recross_gold(
        self,
        relations: gpd.GeoDataFrame,
        relative: Path,
        context: TaskExecutionContext,
        silver: dict[str, Any],
    ) -> dict[str, Any]:
        target = (
            Path(self.config.medallion_gold_path)
            / "firms"
            / "recross"
            / f"run_id={self._safe_segment(context.run_id)}"
            / Path(*relative.parts[1:])
        )
        manifest = {
            "schema_version": "1.0",
            "domain": "firms",
            "stage": "gold",
            "mode": "recross",
            "run_id": context.run_id,
            "source_product": silver.get("source_product"),
            "relation_count": len(relations),
            "source_checksum": silver.get("source_checksum"),
            "object_key": self._relative(target / "relations.geojson"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if not (target / "manifest.json").is_file():
            serializable = relations.copy()
            if "acquired_at_utc" in serializable:
                serializable["acquired_at_utc"] = serializable["acquired_at_utc"].apply(
                    lambda value: value.isoformat() if pd.notna(value) else None
                )
            self._publish_directory(
                target,
                {
                    "relations.geojson": serializable.to_json(drop_id=True).encode("utf-8"),
                    "manifest.json": self._json_bytes(manifest),
                },
            )
        return manifest

    def _build_relations(
        self,
        detections: gpd.GeoDataFrame,
        reference: tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame] | None = None,
    ) -> gpd.GeoDataFrame:
        eligible = detections[detections["publish_gold"]].copy()
        if eligible.empty:
            return self._empty_relations()
        ucs, official_zones, buffer_abrangencia = reference or self._load_reference_layers()
        records: list[dict[str, Any]] = []
        for detection in eligible.itertuples(index=False):
            candidates: dict[int, tuple[str, int | None, int | None]] = {}
            point = detection.geometry
            for uc in ucs.itertuples(index=False):
                if uc.geometry.covers(point):
                    candidates[int(uc.id_uc)] = ("UC", None, None)
            for zone_type, zones, id_column in (
                ("ZA", official_zones, "id_za_oficial"),
                ("BUFFER_ABRANGENCIA", buffer_abrangencia, "id_buffer_abrangencia"),
            ):
                for zone in zones.itertuples(index=False):
                    uc_id = int(zone.id_uc)
                    if uc_id not in candidates and zone.geometry.covers(point):
                        candidates[uc_id] = (
                            zone_type,
                            int(getattr(zone, id_column)) if zone_type == "ZA" else None,
                            int(getattr(zone, id_column)) if zone_type == "BUFFER_ABRANGENCIA" else None,
                        )
            base = detection._asdict()
            for uc_id, (kind, za_id, buffer_id) in candidates.items():
                records.append(
                    {
                        **base,
                        "id_uc": uc_id,
                        "id_za_oficial": za_id,
                        "id_buffer_abrangencia": buffer_id,
                        "tipo_cruzamento": kind,
                    }
                )
        if not records:
            return self._empty_relations()
        return gpd.GeoDataFrame(records, geometry="geometry", crs=eligible.crs)

    def _load_reference_layers(
        self,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
        if not self.config.project_db_url:
            raise FirmsPipelineError("PROJECT_DB_URL is required for FIRMS spatial relations.")
        with psycopg2.connect(self.config.project_db_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute(
                    "SELECT id_uc, ST_AsBinary(geom) FROM uc WHERE situacao = 'ATIVA'"
                )
                uc_rows = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT z.id_za_oficial, z.id_uc, ST_AsBinary(z.geom)
                    FROM za_oficial z JOIN uc u ON u.id_uc = z.id_uc
                    WHERE z.fl_ativa = TRUE AND u.situacao = 'ATIVA'
                    """
                )
                official_rows = cursor.fetchall()

                cursor.execute(
                    """
                    SELECT z.id_buffer_abrangencia, z.id_uc, ST_AsBinary(z.geom)
                    FROM buffer_abrangencia z JOIN uc u ON u.id_uc = z.id_uc
                    WHERE z.fl_ativa = TRUE AND u.situacao = 'ATIVA'
                    """
                )
                buffer_rows = cursor.fetchall()
        # Read a new consistent DB snapshot for every window. Cache only its conversion
        # and spatial preparation, identified by IDs and SHA-256 of every geometry.
        fingerprint = tuple(
            tuple(sorted(
                tuple(row[:-1]) + (hashlib.sha256(bytes(row[-1])).hexdigest() if row[-1] is not None else None,)
                for row in rows
            ))
            for rows in (uc_rows, official_rows, buffer_rows)
        )
        if self._reference_cache is not None and self._reference_cache[0] == fingerprint:
            return self._reference_cache[1]
        ucs = self._rows_to_gdf(uc_rows, ["id_uc", "geometry"])
        official = self._rows_to_gdf(official_rows, ["id_za_oficial", "id_uc", "geometry"])
        buffer = self._rows_to_gdf(buffer_rows, ["id_buffer_abrangencia", "id_uc", "geometry"])
        self._validate_reference_layers(ucs, official, buffer)
        uc_geometries = {int(row.id_uc): make_valid(row.geometry) for row in ucs.itertuples()}
        for zones in (official, buffer):
            for index, row in zones.iterrows():
                zones.at[index, "geometry"] = make_valid(row.geometry).difference(
                    uc_geometries[int(row.id_uc)]
                )
            zones.drop(zones[zones.geometry.is_empty].index, inplace=True)
        for layer in (ucs, official, buffer):
            for geometry in layer.geometry:
                prepare(geometry)
        reference = (ucs, official, buffer)
        self._reference_cache = (fingerprint, reference)
        return reference

    @staticmethod
    def _validate_reference_layers(
        ucs: gpd.GeoDataFrame,
        official: gpd.GeoDataFrame,
        buffer: gpd.GeoDataFrame,
    ) -> None:
        """Require one active zone per active UC in the consistent database snapshot."""
        zone_counts: dict[int, int] = {int(value): 0 for value in ucs.get("id_uc", [])}
        for zones in (official, buffer):
            for value in zones.get("id_uc", []):
                uc_id = int(value)
                if uc_id in zone_counts:
                    zone_counts[uc_id] += 1
        invalid = sorted(uc_id for uc_id, count in zone_counts.items() if count != 1)
        if invalid:
            sample = ", ".join(str(value) for value in invalid[:10])
            raise FirmsPipelineError(
                "FIRMS requires exactly one active ZA or Buffer de Abrangência per active UC; "
                f"invalid UC ids: {sample}."
            )

    def _publish_silver(
        self,
        detections: gpd.GeoDataFrame,
        window: FirmsWindow,
        context: TaskExecutionContext,
        bronze: dict[str, Any],
    ) -> dict[str, Any]:
        target = self._partition(self.config.medallion_silver_path, window, context)
        manifest = {
            "schema_version": "1.0",
            "domain": "firms",
            "stage": "silver",
            "run_id": context.run_id,
            "source_product": window.source_product,
            "record_count": len(detections),
            "source_checksum": bronze["checksum_sha256"],
            "object_key": self._relative(target / "detections.parquet"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if not (target / "manifest.json").is_file():
            buffer = io.BytesIO()
            detections.to_parquet(buffer, index=False)
            self._publish_directory(
                target,
                {"detections.parquet": buffer.getvalue(), "manifest.json": self._json_bytes(manifest)},
            )
        return manifest

    def _publish_gold(
        self,
        relations: gpd.GeoDataFrame,
        window: FirmsWindow,
        context: TaskExecutionContext,
        bronze: dict[str, Any],
    ) -> dict[str, Any]:
        target = self._partition(self.config.medallion_gold_path, window, context)
        manifest = {
            "schema_version": "1.0",
            "domain": "firms",
            "stage": "gold",
            "run_id": context.run_id,
            "source_product": window.source_product,
            "relation_count": len(relations),
            "source_checksum": bronze["checksum_sha256"],
            "object_key": self._relative(target / "relations.geojson"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if not (target / "manifest.json").is_file():
            serializable = relations.copy()
            if "acquired_at_utc" in serializable:
                serializable["acquired_at_utc"] = serializable["acquired_at_utc"].apply(
                    lambda value: value.isoformat() if pd.notna(value) else None
                )
            geojson = serializable.to_json(drop_id=True).encode("utf-8")
            self._publish_directory(
                target,
                {"relations.geojson": geojson, "manifest.json": self._json_bytes(manifest)},
            )
        return manifest

    def _load_postgres(
        self,
        relations: gpd.GeoDataFrame,
        window: FirmsWindow | None,
        context: TaskExecutionContext,
        bronze: dict[str, Any],
        silver: dict[str, Any],
        gold: dict[str, Any],
    ) -> int:
        if relations.empty:
            return 0
        now = datetime.now(timezone.utc)
        values = []
        for row in relations.itertuples(index=False):
            values.append(
                (
                    row.detection_id, row.source_product, row.source_version or None,
                    row.source_processing, int(row.id_uc),
                    self._optional_int(row.id_za_oficial), self._optional_int(row.id_buffer_abrangencia),
                    row.tipo_cruzamento, row.acquired_at_utc.to_pydatetime(), row.satellite or None,
                    row.instrument or None, row.confidence_raw, row.confidence_scheme,
                    self._optional_float(row.confidence_score), row.confidence_class,
                    row.publication_rule_version, self._optional_float(row.frp_mw),
                    self._optional_float(row.brightness_kelvin),
                    self._optional_float(row.brightness_secondary_kelvin),
                    self._optional_float(row.scan_km), self._optional_float(row.track_km),
                    row.daynight, row.detection_type, float(row.geometry.x), float(row.geometry.y),
                    row.source_checksum, context.run_id,
                    datetime.fromisoformat(bronze["received_at"]),
                    datetime.fromisoformat(silver["created_at"]),
                    datetime.fromisoformat(gold["created_at"]), now,
                )
            )
        sql = """
            INSERT INTO firms_clip (
                detection_id, source_product, source_version, source_processing,
                id_uc, id_za_oficial, id_buffer_abrangencia, tipo_cruzamento, acquired_at_utc,
                satellite, instrument, confidence_raw, confidence_scheme, confidence_score,
                confidence_class, publication_rule_version, frp_mw, brightness_kelvin,
                brightness_secondary_kelvin, scan_km, track_km, daynight, detection_type,
                geom, source_checksum, run_id, dt_bronze, dt_silver, dt_gold, dt_carga
            ) VALUES %s ON CONFLICT DO NOTHING
        """
        template = "(" + ",".join(["%s"] * 23) + ",ST_SetSRID(ST_MakePoint(%s,%s),4674)," + ",".join(["%s"] * 6) + ")"
        with psycopg2.connect(self.config.project_db_url) as connection:
            with connection.cursor() as cursor:
                execute_values(cursor, sql, values, template=template, page_size=500)
                return max(cursor.rowcount, 0)

    def _set_backfill_running(self, window: FirmsWindow, run_id: str) -> None:
        self._update_backfill(
            window,
            """
            UPDATE firms_backfill_window
            SET processing_state='RUNNING', attempt_count=attempt_count+1,
                run_id=%s, last_error=NULL, updated_at=CURRENT_TIMESTAMP
            WHERE source_product=%s AND start_date=%s AND end_date=%s
            """,
            (run_id, window.source_product, window.start_date, window.end_date),
        )

    def _set_backfill_published(
        self, window: FirmsWindow, bronze: dict[str, Any], run_id: str
    ) -> None:
        self._update_backfill(
            window,
            """
            UPDATE firms_backfill_window
            SET processing_state='PUBLISHED', manifest_key=%s, source_checksum=%s,
                run_id=%s, last_error=NULL, published_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
            WHERE source_product=%s AND start_date=%s AND end_date=%s
            """,
            (
                bronze["manifest_key"], bronze["checksum_sha256"], run_id,
                window.source_product, window.start_date, window.end_date,
            ),
        )

    def _set_backfill_failed(
        self,
        window: FirmsWindow,
        run_id: str,
        error: str,
        bronze: dict[str, Any] | None,
    ) -> None:
        self._update_backfill(
            window,
            """
            UPDATE firms_backfill_window
            SET processing_state='FAILED', run_id=%s, last_error=%s,
                manifest_key=COALESCE(%s, manifest_key),
                source_checksum=COALESCE(%s, source_checksum),
                updated_at=CURRENT_TIMESTAMP
            WHERE source_product=%s AND start_date=%s AND end_date=%s
            """,
            (
                run_id,
                error,
                bronze.get("manifest_key") if bronze else None,
                bronze.get("checksum_sha256") if bronze else None,
                window.source_product,
                window.start_date,
                window.end_date,
            ),
        )

    def _stored_backfill_response(
        self, window: FirmsWindow
    ) -> tuple[FirmsAreaResponse, dict[str, Any]] | None:
        """Reuse an immutable Bronze response retained by a failed historical window."""
        with psycopg2.connect(self.config.project_db_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT manifest_key, source_checksum
                    FROM firms_backfill_window
                    WHERE source_product=%s AND start_date=%s AND end_date=%s
                    """,
                    (window.source_product, window.start_date, window.end_date),
                )
                row = cursor.fetchone()
        if not row or not row[0]:
            return None
        medallion_root = Path(self.config.medallion_bronze_path).parent.resolve()
        manifest_path = (medallion_root / Path(str(row[0]))).resolve()
        if medallion_root not in manifest_path.parents or not manifest_path.is_file():
            raise FirmsPipelineError("Stored FIRMS backfill manifest is unavailable or unsafe.")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        response_path = (medallion_root / Path(str(manifest["object_key"]))).resolve()
        if medallion_root not in response_path.parents or not response_path.is_file():
            raise FirmsPipelineError("Stored FIRMS backfill response is unavailable or unsafe.")
        content = response_path.read_bytes()
        checksum = self._sha256(content)
        if checksum != row[1] or checksum != manifest.get("checksum_sha256"):
            raise FirmsPipelineError("Stored FIRMS backfill response checksum mismatch.")
        return (
            FirmsAreaResponse(
                content=content,
                http_status=int(manifest["http_status"]),
                content_type=str(manifest["content_type"]),
                requested_at=str(manifest["requested_at"]),
                received_at=str(manifest["received_at"]),
                sanitized_endpoint=str(manifest["sanitized_endpoint"]),
            ),
            manifest,
        )

    def _update_backfill(
        self, window: FirmsWindow, statement: str, parameters: tuple[Any, ...]
    ) -> None:
        with psycopg2.connect(self.config.project_db_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, parameters)
                if cursor.rowcount != 1:
                    raise FirmsPipelineError(
                        "FIRMS backfill control row was not found for "
                        f"{window.source_product} {window.start_date}:{window.end_date}."
                    )

    def _write_quality(
        self,
        context: TaskExecutionContext,
        window: FirmsWindow,
        metrics: dict[str, int],
        result: dict[str, Any],
    ) -> None:
        root = Path(self.config.medallion_gold_path).parent / "quality" / "firms"
        target = (
            root
            / f"run_id={self._safe_segment(context.run_id)}"
            / f"product={self._safe_segment(window.source_product)}"
            / f"start_date={window.start_date.isoformat()}"
            / f"end_date={window.end_date.isoformat()}"
        )
        target.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": "1.0",
            "dag_id": context.dag_id,
            "run_id": context.run_id,
            "domain": "firms",
            "stage": "window",
            "source_product": window.source_product,
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "status": result["status"],
            "source_detections": metrics.get("source_records", 0),
            "accepted_records": result.get("silver_records", 0),
            "spatial_relations": result.get("gold_relations", 0),
            "inserted_relations": result.get("inserted_relations", 0),
            "reason_counts": {
                "INVALID_RECORD": metrics.get("invalid_records", 0),
                "OUTSIDE_SC": metrics.get("outside_sc_records", 0),
                "CONFIDENCE_FILTERED": metrics.get("confidence_filtered_records", 0),
            },
            "error": result.get("error"),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": self.PIPELINE_VERSION,
        }
        self._write_json_atomic(target / "summary.json", payload)

    def _write_run_quality(
        self,
        context: TaskExecutionContext,
        summary: dict[str, Any],
    ) -> None:
        """Persist the aggregate outcome of a daily or backfill DAG run."""
        target = (
            Path(self.config.medallion_gold_path).parent
            / "quality"
            / "firms"
            / f"run_id={self._safe_segment(context.run_id)}"
            / "summary.json"
        )
        payload = {
            "schema_version": "1.0",
            "domain": "firms",
            "stage": "run",
            "dag_id": context.dag_id,
            "run_id": context.run_id,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": self.PIPELINE_VERSION,
            **summary,
        }
        self._write_json_atomic(target, payload)

    def _client(self) -> FirmsAreaClient:
        if self._client_factory:
            return self._client_factory()
        return FirmsAreaClient(
            map_key=self.config.firms_map_key,
            base_url=self.config.firms_api_base_url,
            connect_timeout_seconds=self.config.firms_connect_timeout_seconds,
            read_timeout_seconds=self.config.firms_read_timeout_seconds,
            max_attempts=self.config.firms_max_attempts,
        )

    def _load_state_boundary(self) -> gpd.GeoDataFrame:
        """Validate canonical bytes on every read and reuse geometry within one task."""
        package = Path(
            self.config.medallion_bronze_path,
            "boundaries", "source=ibge", "year=2025", "area=sc",
        )
        source = Path(self.config.sc_boundary_source_dir) / "limites_SC.geojson"
        entry = None
        if package.exists():
            manifest_path = package / "manifest.json"
            if not manifest_path.is_file():
                raise FirmsPipelineError("Santa Catarina Bronze boundary manifest is missing.")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("domain") != "ibge_sc_boundary":
                raise FirmsPipelineError("Santa Catarina Bronze boundary domain is invalid.")
            inventory = manifest.get("files")
            if not isinstance(inventory, list):
                raise FirmsPipelineError("Santa Catarina Bronze boundary inventory is invalid.")
            entries = [item for item in inventory if isinstance(item, dict) and item.get("name") == source.name]
            if len(entries) != 1:
                raise FirmsPipelineError("Santa Catarina Bronze boundary inventory is incomplete.")
            source = package / source.name
            entry = entries[0]
        if not source.is_file():
            raise FirmsPipelineError(f"Santa Catarina boundary is unavailable: {source}")
        content = source.read_bytes()
        checksum = self._sha256(content)
        if entry is not None and (
            checksum != entry.get("checksum_sha256") or len(content) != entry.get("byte_size")
        ):
            raise FirmsPipelineError("Santa Catarina Bronze boundary checksum or size mismatch.")
        cache_key = (str(source.resolve()), checksum)
        if self._state_boundary_cache is not None and self._state_boundary_cache[0] == cache_key:
            return self._state_boundary_cache[1]
        boundary = gpd.read_file(io.BytesIO(content))
        if boundary.empty or boundary.crs is None:
            raise FirmsPipelineError("Santa Catarina boundary is empty or has no CRS.")
        self._state_boundary_cache = (cache_key, boundary)
        return boundary

    def _partition(
        self, root: str, window: FirmsWindow, context: TaskExecutionContext
    ) -> Path:
        return (
            Path(root)
            / "firms"
            / f"product={self._safe_segment(window.source_product)}"
            / f"start_date={window.start_date.isoformat()}"
            / f"end_date={window.end_date.isoformat()}"
            / f"run_id={self._safe_segment(context.run_id)}"
        )

    def _relative(self, path: Path) -> str:
        candidates = [
            Path(self.config.medallion_bronze_path).parent,
            Path(self.config.medallion_silver_path).parent,
            Path(self.config.medallion_gold_path).parent,
        ]
        resolved = path.resolve()
        for root in candidates:
            try:
                return resolved.relative_to(root.resolve()).as_posix()
            except ValueError:
                continue
        raise FirmsPipelineError(f"FIRMS artifact is outside the Medallion root: {path}")

    @staticmethod
    def _publish_directory(target: Path, files: dict[str, bytes]) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        staging = target.with_name(f".{target.name}.publishing-{uuid4()}")
        staging.mkdir(parents=True)
        try:
            for name, content in files.items():
                destination = staging / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(content)
            os.replace(staging, target)
        except Exception:
            if staging.exists():
                shutil.rmtree(staging)
            raise

    @staticmethod
    def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid4()}.tmp")
        temporary.write_bytes(FirmsPipelineService._json_bytes(payload))
        os.replace(temporary, path)

    @staticmethod
    def _json_bytes(payload: dict[str, Any]) -> bytes:
        return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )

    @staticmethod
    def _window(context: TaskExecutionContext) -> FirmsWindow:
        conf = context.conf or {}
        try:
            return FirmsWindow(
                source_product=str(conf["source_product"]),
                start_date=date.fromisoformat(str(conf["start_date"])),
                end_date=date.fromisoformat(str(conf["end_date"])),
                mode=str(conf["mode"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise FirmsPipelineError("Invalid or incomplete FIRMS window configuration.") from exc

    @staticmethod
    def _parse_date(value: Any, field: str) -> date:
        try:
            return date.fromisoformat(str(value))
        except ValueError as exc:
            raise FirmsPipelineError(f"FIRMS {field} must use YYYY-MM-DD.") from exc

    @staticmethod
    def _source_processing(product: str) -> str:
        upper = product.upper()
        if upper.endswith("_NRT"):
            return "NRT"
        if upper.endswith("_SP"):
            return "SP"
        return "ARCHIVE"

    @staticmethod
    def _csv_record_count(content: bytes) -> int:
        lines = content.decode("utf-8-sig").splitlines()
        return max(len(lines) - 1, 0)

    @staticmethod
    def _sha256(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _safe_segment(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]", "_", value)

    @staticmethod
    def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
        if column not in frame:
            return pd.Series([None] * len(frame), index=frame.index, dtype="object")
        return pd.to_numeric(frame[column], errors="coerce")

    @staticmethod
    def _string(frame: pd.DataFrame, column: str) -> pd.Series:
        if column not in frame:
            return pd.Series([""] * len(frame), index=frame.index, dtype="object")
        return frame[column].astype(str).str.strip()

    @staticmethod
    def _detection_id(row: pd.Series) -> str:
        identity = "|".join(
            (
                str(row["source_product"]), str(row["source_version"]),
                str(row["satellite"]), row["acquired_at_utc"].isoformat(),
                format(float(row["latitude"]), ".7f"),
                format(float(row["longitude"]), ".7f"),
            )
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    @staticmethod
    def _empty_detections() -> gpd.GeoDataFrame:
        columns = [
            "detection_id", "source_product", "source_version", "source_processing",
            "acquired_at_utc", "latitude", "longitude", "satellite", "instrument",
            "confidence_raw", "confidence_scheme", "confidence_score", "confidence_class",
            "publication_rule_version", "publish_gold", "frp_mw", "brightness_kelvin",
            "brightness_secondary_kelvin", "scan_km", "track_km", "daynight",
            "detection_type", "source_checksum", "run_id", "geometry",
        ]
        return gpd.GeoDataFrame({column: [] for column in columns}, geometry="geometry", crs="EPSG:4674")

    @staticmethod
    def _empty_relations() -> gpd.GeoDataFrame:
        frame = FirmsPipelineService._empty_detections()
        frame["id_uc"] = pd.Series(dtype="int64")
        frame["id_za_oficial"] = pd.Series(dtype="Int64")
        frame["id_buffer_abrangencia"] = pd.Series(dtype="Int64")
        frame["tipo_cruzamento"] = pd.Series(dtype="object")
        return frame

    @staticmethod
    def _rows_to_gdf(rows: list[tuple[Any, ...]], columns: list[str]) -> gpd.GeoDataFrame:
        frame = pd.DataFrame(rows, columns=columns)
        if frame.empty:
            return gpd.GeoDataFrame(frame, geometry=[], crs="EPSG:4674")
        frame["geometry"] = frame["geometry"].apply(
            lambda value: wkb.loads(bytes(value)) if value is not None else None
        )
        result = gpd.GeoDataFrame(frame, geometry="geometry", crs="EPSG:4674")
        return result[result.geometry.notna() & ~result.geometry.is_empty].copy()

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        if value is None or pd.isna(value):
            return None
        return float(value)

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value is None or pd.isna(value):
            return None
        return int(value)

    def _sanitize_error(self, error: Exception) -> str:
        message = str(error)
        if self.config.firms_map_key:
            message = message.replace(self.config.firms_map_key, "{MAP_KEY}")
        return message[:1000]
