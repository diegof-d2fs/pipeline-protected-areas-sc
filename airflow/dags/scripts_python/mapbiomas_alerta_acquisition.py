"""Acquire a complete, immutable MapBiomas Alerta snapshot for the thematic DAG."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import re
import shutil
import tempfile
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.mapbiomas_alerta_client import MapbiomasAlertaClient


ALERTS_QUERY = """
query Alerts($startDate: BaseDate!, $endDate: BaseDate!, $territoryIds: [Int!],
             $page: Int!, $limit: Int!) {
  alerts(startDate: $startDate, endDate: $endDate, dateType: PublishedAt,
         territoryIds: $territoryIds, page: $page, limit: $limit,
         sortField: ALERT_CODE, sortDirection: ASC) {
    metadata { totalCount totalPages currentPage limitValue }
    collection {
      alertCode publishedAt detectedAt areaHa sources crossedBiomes
      crossedStates crossedCities deforestationClasses datum geometryWkt
      imageAcquiredBeforeAt imageAcquiredAfterAt republished publicationCycles
    }
  }
}
"""
DATE_RANGE_QUERY = "query { alertDateRange { maxPublishedAt } }"
START_DATE = "2019-01-01"
SOURCE_NAME = "mapbiomas_alerta_graphql_v2"
SNAPSHOT_STEM = "dashboard_alerts-shapefile"
TARGET_STATES = {"SANTACATARINA", "PARANA", "RIOGRANDEDOSUL"}
LOGGER = logging.getLogger("pipeline.mapbiomas_alerta.acquisition")


class MapbiomasAlertaAcquisitionError(RuntimeError):
    """Failure before a complete Bronze snapshot can be published."""


class MapbiomasAlertaAcquisitionService:
    """Fetch all published alerts, compare source content, then publish atomically."""

    def __init__(self, config: PipelineConfig | None = None) -> None:
        self.config = config or PipelineConfig.from_env()
        self._cache_dir: Path | None = None

    def acquire(self, context: TaskExecutionContext) -> bool:
        # A cadastral update must re-use the already published historical snapshot. The same holds
        # for an explicit reprocessing request (e.g. a freshly provisioned database): the Bronze
        # snapshot is already complete, only Silver/Gold/PostGIS must be rebuilt from it.
        conf = context.conf or {}
        if conf.get("manifest_key") or conf.get("reprocess_published_snapshot"):
            return True

        territory_ids = list(self.config.mapbiomas_alerta_territory_ids)
        root = Path(self.config.medallion_bronze_path) / "mapbiomas_alerta"
        root.mkdir(parents=True, exist_ok=True)
        located = self._latest_api_manifest(root)
        if located is not None and located[1].get("territory_ids") == territory_ids:
            previous_folder, previous_manifest = located
            fetch_start_date = previous_manifest.get("max_published_at") or START_DATE
        else:
            # No prior snapshot, or the territory scope changed: only a full re-fetch
            # from the historical anchor can guarantee a complete dataset.
            previous_folder, previous_manifest = None, None
            fetch_start_date = START_DATE

        requested_start = conf.get("acquisition_start_date")
        requested_end = conf.get("acquisition_end_date")
        try:
            if requested_start:
                requested_start = date.fromisoformat(str(requested_start)).isoformat()
                if requested_start < START_DATE:
                    raise ValueError("before source historical anchor")
                if previous_manifest is None and requested_start > START_DATE:
                    raise ValueError("partial acquisition requires an existing full snapshot")
                fetch_start_date = min(fetch_start_date, requested_start)
            if requested_end:
                requested_end = date.fromisoformat(str(requested_end)).isoformat()
                if requested_end < fetch_start_date:
                    raise ValueError("end precedes acquisition start")
                if previous_manifest and requested_end < previous_manifest["max_published_at"]:
                    raise ValueError("end precedes the existing publication checkpoint")
        except (ValueError, KeyError):
            raise MapbiomasAlertaAcquisitionError("Invalid explicit acquisition interval or incomplete baseline.") from None

        with MapbiomasAlertaClient(
            email=self.config.mapbiomas_alerta_email,
            password=self.config.mapbiomas_alerta_password,
            api_url=self.config.mapbiomas_alerta_api_url,
            timeout_seconds=self.config.mapbiomas_alerta_timeout_seconds,
            max_attempts=self.config.mapbiomas_alerta_max_attempts,
        ) as client:
            published_range = client.query(DATE_RANGE_QUERY).get("alertDateRange") or {}
            end_date = published_range.get("maxPublishedAt")
            if not isinstance(end_date, str) or not end_date:
                raise MapbiomasAlertaAcquisitionError("API returned no maximum publication date.")
            available_end_date = end_date
            if requested_end:
                end_date = min(end_date, requested_end)
            if end_date < fetch_start_date:
                raise MapbiomasAlertaAcquisitionError("Source publication availability precedes the requested start.")
            LOGGER.info("MapBiomas Alerta API interval: %s..%s; available publication end=%s",
                        fetch_start_date, end_date, available_end_date)
            new_records = self._fetch_all(
                client, end_date, start_date=fetch_start_date, run_id=context.run_id
            )

        previous_by_code = {}
        if previous_manifest is not None:
            previous_by_code = {row["alertCode"]: row for row in self._load_previous_records(previous_folder)}
            by_code = dict(previous_by_code)
            by_code.update({row["alertCode"]: row for row in new_records})
            records = [by_code[code] for code in sorted(by_code)]
        else:
            records = new_records

        canonical = [json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for row in records]
        source_sha256 = hashlib.sha256(("\n".join(canonical) + "\n").encode("utf-8")).hexdigest()
        unchanged = previous_manifest is not None and previous_manifest.get("source_sha256") == source_sha256
        audit_dir = (Path(self.config.medallion_bronze_path).parent / "quality" /
                     "mapbiomas_alerta" / "acquisition" /
                     re.sub(r"[^A-Za-z0-9_-]", "_", context.run_id))
        audit_dir.mkdir(parents=True, exist_ok=True)
        audit = {
            "run_id": context.run_id, "checked_at": datetime.now(timezone.utc).isoformat(),
            "fetch_start_date": fetch_start_date, "fetch_end_date": end_date,
            "available_max_published_at": available_end_date,
            "previous_records": len(previous_by_code), "returned_records": len(new_records),
            "new_or_changed_records": sum(previous_by_code.get(row["alertCode"]) != row for row in new_records),
            "merged_records": len(records), "source_sha256": source_sha256,
            "snapshot_changed": not unchanged, "territory_ids": territory_ids,
        }
        (audit_dir / "summary.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
        if unchanged:
            self._discard_cache()
            LOGGER.info("MapBiomas Alerta API snapshot unchanged. records=%s sha256=%s", len(records), source_sha256[:12])
            return False

        self._publish(root, context, records, canonical, source_sha256, end_date, fetch_start_date)
        self._discard_cache()
        LOGGER.info(
            "MapBiomas Alerta API snapshot published. records=%s new_records=%s sha256=%s",
            len(records), len(new_records), source_sha256[:12],
        )
        return True

    def _fetch_all(
        self,
        client: MapbiomasAlertaClient,
        end_date: str,
        start_date: str = START_DATE,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        page = 1
        expected_total: int | None = None
        expected_pages: int | None = None
        by_code: dict[int, dict[str, Any]] = {}
        while True:
            variables = {
                "startDate": start_date,
                "endDate": end_date,
                "territoryIds": list(self.config.mapbiomas_alerta_territory_ids),
                "page": page,
                "limit": self.config.mapbiomas_alerta_page_size,
            }
            cache_path = self._cache_dir / f"page-{page:06d}.json" if self._cache_dir and page > 1 else None
            from_cache = cache_path is not None and cache_path.is_file()
            if from_cache:
                try:
                    data = json.loads(cache_path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    from_cache = False
            if not from_cache:
                data = client.query(ALERTS_QUERY, variables).get("alerts")
            if not isinstance(data, dict):
                raise MapbiomasAlertaAcquisitionError("API returned no alerts collection.")
            if page == 1 and run_id:
                self._cache_dir = self._page_cache_dir(run_id, variables, data)
                self._cache_dir.mkdir(parents=True, exist_ok=True)
            metadata = data.get("metadata") or {}
            rows = data.get("collection")
            if not isinstance(rows, list):
                raise MapbiomasAlertaAcquisitionError("API returned no alerts page.")
            total = metadata.get("totalCount")
            pages = metadata.get("totalPages")
            if not isinstance(total, int) or not isinstance(pages, int) or pages < 0:
                raise MapbiomasAlertaAcquisitionError("API returned invalid pagination metadata.")
            if expected_total is None:
                expected_total, expected_pages = total, pages
            elif (total, pages) != (expected_total, expected_pages):
                if from_cache:
                    data = client.query(ALERTS_QUERY, variables).get("alerts") or {}
                    metadata = data.get("metadata") or {}
                    rows = data.get("collection")
                    total, pages = metadata.get("totalCount"), metadata.get("totalPages")
                    from_cache = False
            if (total, pages) != (expected_total, expected_pages):
                raise MapbiomasAlertaAcquisitionError("API pagination changed during acquisition.")
            if metadata.get("currentPage") != page:
                raise MapbiomasAlertaAcquisitionError("API returned an unexpected page number.")
            if not isinstance(rows, list):
                raise MapbiomasAlertaAcquisitionError("API returned no alerts page.")
            if self._cache_dir and not from_cache:
                self._write_cache_page(self._cache_dir / f"page-{page:06d}.json", data)
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get("alertCode"), int):
                    raise MapbiomasAlertaAcquisitionError("API returned an alert without code.")
                code = row["alertCode"]
                if code in by_code:
                    raise MapbiomasAlertaAcquisitionError("API returned a duplicate alert code across pages.")
                by_code[code] = row
            if page == 1 or page % 10 == 0 or page >= pages:
                LOGGER.info(
                    "MapBiomas Alerta page progress: page=%s/%s records=%s/%s cached=%s",
                    page, pages, len(by_code), expected_total, from_cache,
                )
            if page >= pages:
                break
            page += 1
        if len(by_code) != expected_total:
            raise MapbiomasAlertaAcquisitionError("API alert count did not match pagination metadata.")
        # A second count catches additions or removals while the snapshot was read.
        check = client.query(
            ALERTS_QUERY,
            {
                "startDate": start_date,
                "endDate": end_date,
                "territoryIds": list(self.config.mapbiomas_alerta_territory_ids),
                "page": 1,
                "limit": self.config.mapbiomas_alerta_page_size,
            },
        ).get("alerts") or {}
        if (check.get("metadata") or {}).get("totalCount") != expected_total:
            raise MapbiomasAlertaAcquisitionError("API alert count changed during acquisition.")
        return [by_code[code] for code in sorted(by_code)]

    def _page_cache_dir(
        self, run_id: str, variables: dict[str, Any], first_page: dict[str, Any]
    ) -> Path:
        safe_run_id = re.sub(r"[^A-Za-z0-9_-]", "_", run_id)
        fingerprint = hashlib.sha256(
            json.dumps(
                {"variables": variables, "first_page": first_page},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:24]
        return Path(self.config.medallion_tmp_path) / "mapbiomas_alerta_api" / safe_run_id / fingerprint

    @staticmethod
    def _write_cache_page(path: Path, data: dict[str, Any]) -> None:
        pending = path.with_suffix(".pending")
        pending.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        pending.replace(path)

    def _discard_cache(self) -> None:
        if self._cache_dir is None:
            return
        root = Path(self.config.medallion_tmp_path).resolve()
        target = self._cache_dir.resolve()
        if root not in target.parents:
            raise MapbiomasAlertaAcquisitionError("Unsafe page-cache path.")
        shutil.rmtree(target)
        self._cache_dir = None

    @staticmethod
    def _latest_api_manifest(root: Path) -> tuple[Path, dict[str, Any]] | None:
        for folder in sorted(root.iterdir(), reverse=True):
            if not folder.is_dir() or not re.fullmatch(r"\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}", folder.name):
                continue
            path = folder / "acquisition.json"
            if path.is_file():
                manifest = json.loads(path.read_text(encoding="utf-8"))
                if manifest.get("source") == SOURCE_NAME:
                    files = manifest.get("files") or {}
                    if not isinstance(files, dict) or not files:
                        raise MapbiomasAlertaAcquisitionError("Previous API snapshot has no file checksums.")
                    for name, expected_sha256 in files.items():
                        if not isinstance(name, str) or Path(name).name != name:
                            raise MapbiomasAlertaAcquisitionError("Previous API snapshot has an unsafe file name.")
                        artifact = folder / name
                        if not artifact.is_file() or MapbiomasAlertaAcquisitionService._sha256(artifact) != expected_sha256:
                            raise MapbiomasAlertaAcquisitionError("Previous API snapshot failed checksum validation.")
                    return folder, manifest
        return None

    @staticmethod
    def _load_previous_records(folder: Path) -> list[dict[str, Any]]:
        with gzip.GzipFile(folder / "alerts.jsonl.gz", "rb") as compressed:
            text = compressed.read().decode("utf-8")
        return [json.loads(line) for line in text.splitlines() if line]

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _publish(
        self,
        root: Path,
        context: TaskExecutionContext,
        records: list[dict[str, Any]],
        canonical: list[str],
        source_sha256: str,
        end_date: str,
        fetch_start_date: str | None = None,
    ) -> None:
        if not records:
            raise MapbiomasAlertaAcquisitionError("Refusing to publish an empty historical snapshot.")
        stage = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
        try:
            raw_path = stage / "alerts.jsonl.gz"
            with raw_path.open("wb") as stream:
                with gzip.GzipFile(filename="", mode="wb", fileobj=stream, mtime=0) as compressed:
                    compressed.write(("\n".join(canonical) + "\n").encode("utf-8"))

            features = [self._to_feature(record) for record in records]
            frame = gpd.GeoDataFrame(features, geometry="geometry", crs="EPSG:4674")
            frame.to_file(stage / f"{SNAPSHOT_STEM}.shp", driver="ESRI Shapefile", encoding="UTF-8")
            file_checksums = {
                path.name: self._sha256(path)
                for path in sorted(stage.iterdir())
                if path.is_file()
            }
            with (stage / "acquisition.json").open("w", encoding="utf-8") as stream:
                json.dump(
                    {
                        "source": SOURCE_NAME,
                        "api_url": self.config.mapbiomas_alerta_api_url,
                        "fetch_start_date": fetch_start_date or START_DATE,
                        "max_published_at": end_date,
                        "territory_ids": list(self.config.mapbiomas_alerta_territory_ids),
                        "record_count": len(records),
                        "source_sha256": source_sha256,
                        "files": file_checksums,
                        "run_id": context.run_id,
                        "created_at_utc": datetime.now(timezone.utc).isoformat(),
                        "vpressao_mapping": "unavailable_in_api_v2_left_blank",
                    },
                    stream,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            stamp = datetime.now(timezone.utc)
            target = root / stamp.strftime("%Y-%m-%d-%H-%M-%S")
            while target.exists():
                stamp += timedelta(seconds=1)
                target = root / stamp.strftime("%Y-%m-%d-%H-%M-%S")
            stage.replace(target)
        except Exception:
            shutil.rmtree(stage, ignore_errors=True)
            raise

    @staticmethod
    def _to_feature(record: dict[str, Any]) -> dict[str, Any]:
        code = record["alertCode"]
        raw_wkt = record.get("geometryWkt")
        if not isinstance(raw_wkt, str) or not raw_wkt:
            raise MapbiomasAlertaAcquisitionError(f"Alert {code} has no geometry WKT.")
        try:
            geometry = wkt.loads(raw_wkt)
        except Exception:
            raise MapbiomasAlertaAcquisitionError(f"Alert {code} has invalid geometry WKT.") from None
        if not isinstance(geometry, (Polygon, MultiPolygon)) or geometry.is_empty:
            raise MapbiomasAlertaAcquisitionError(f"Alert {code} has no polygon geometry.")
        datum = str(record.get("datum") or "")
        if "4674" not in datum and "SIRGAS 2000" not in datum.upper():
            raise MapbiomasAlertaAcquisitionError(f"Alert {code} has an unexpected datum.")
        states = record.get("crossedStates") or []
        selected_state = next(
            (
                state for state in states
                if "".join(
                    char for char in unicodedata.normalize("NFKD", str(state).upper())
                    if char.isalnum() and not unicodedata.combining(char)
                ) in TARGET_STATES
            ),
            None,
        )
        if not selected_state:
            raise MapbiomasAlertaAcquisitionError(f"Alert {code} has no expected state.")
        detected = record.get("detectedAt")
        if not isinstance(detected, str) or not re.match(r"^\d{4}-\d{2}-\d{2}$", detected):
            raise MapbiomasAlertaAcquisitionError(f"Alert {code} has no detection date.")
        sources = record.get("sources") or []
        biomes = record.get("crossedBiomes") or []
        cities = record.get("crossedCities") or []
        return {
            "CODEALERTA": code,
            "FONTE": "; ".join(map(str, sources))[:254],
            "BIOMA": "; ".join(map(str, biomes))[:254],
            "ESTADO": str(selected_state),
            "MUNICIPIO": "; ".join(map(str, cities))[:254],
            "AREAHA": float(record.get("areaHa") or 0),
            "ANODETEC": float(detected[:4]),
            "DATADETEC": detected,
            "DTIMGANT": record.get("imageAcquiredBeforeAt") or "",
            "DTIMGDEP": record.get("imageAcquiredAfterAt") or "",
            "DTPUBLI": record.get("publishedAt") or "",
            "VPRESSAO": "",
            "geometry": geometry,
        }
