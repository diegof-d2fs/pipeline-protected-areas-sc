"""Bronze ingestion service for the MapBiomas land-use and land-cover package."""

from __future__ import annotations

import hashlib
import json
import csv
import time
import re
from dataclasses import dataclass
from zipfile import ZipFile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import rasterio
from rasterio.mask import mask
from rasterio.features import geometry_mask, geometry_window
from rasterio.errors import WindowError
from rasterio.io import MemoryFile
from rasterio.windows import Window
from rasterio.shutil import copy as raster_copy
from pyproj import Geod
from shapely import make_valid
from shapely.geometry import MultiPolygon, Polygon, shape
from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.manifest_input import load_manifest_input
from scripts_python.object_storage import MedallionStore


class MapbiomasPipelineError(RuntimeError):
    """Raised when the immutable MapBiomas Bronze contract cannot be satisfied."""


@dataclass(frozen=True)
class MapbiomasDataset:
    """One immutable MapBiomas coverage dataset identified by collection, version and year.

    Every path, filename and manifest value in the pipeline is derived from these
    three coordinates so that a historical backfill only needs a different ``year``
    (or ``collection``) in ``dag_run.conf``; nothing is hardcoded to a single year.
    """

    collection: str
    version: str
    year: int

    @property
    def partition(self) -> tuple[str, ...]:
        """Medallion partition segments shared by Bronze, Silver and Gold."""
        return (f"collection={self.collection}", f"version={self.version}", f"year={self.year}")

    @property
    def source_raster_name(self) -> str:
        """Official MapBiomas download filename for this collection and year."""
        return f"brazil_coverage-col{self.collection}_{self.year}.tif"

    @property
    def silver_raster_name(self) -> str:
        """Santa Catarina clip filename written to Silver."""
        return f"mapbiomas_sc_coverage_col{self.collection}_{self.year}.tif"

    @property
    def gold_cog_name(self) -> str:
        """Cloud Optimized GeoTIFF filename published to Gold."""
        return f"mapbiomas_sc_coverage_col{self.collection}_{self.year}_cog.tif"

    @property
    def legend_name(self) -> str:
        """Structured legend filename; scoped to the collection, not the year."""
        return f"mapbiomas_legend_col{self.collection}.json"

    @property
    def statistics_stem(self) -> str:
        """Stem for the published area-by-AOI statistics files."""
        return f"mapbiomas_area_by_aoi_class_col{self.collection}_{self.year}"

    @property
    def raster_table(self) -> str:
        """PostGIS in-db raster table for this year (one physical table per year)."""
        return f"mapbiomas_raster_{int(self.year)}"


class MapbiomasPipelineService:
    """Publish the official MapBiomas package and IBGE boundary to Bronze safely."""

    # Collection-11 companion files. Their names identify the collection, not the
    # year, so they are shared by every yearly raster of Collection 11. A future
    # collection would need its own list.
    MAPBIOMAS_STATIC_FILES = (
        "CodigosDeLegenda_LegendCodes_MapBiomas_Brazil_Collection11_PDF.pdf",
        "COVERAGE_QGIS_COL11_PT_EN.zip",
        "DESCRICAO_DA_LEGENDA_MAPBIOMAS_BRASIL_COLECAO_11_PDF.pdf",
        "ESTILO_QGIS_N_CHANGES_COL11_EN_PT-BR.zip",
        "ESTILO_QGIS_N_CLASSES_COL11_PT-BR_EN.zip",
        "Factsheet-Colecao-11-12082026-1.pdf",
        "legend_code_mapbiomas_brazil_collection_11.csv",
        "legend_code_n_changes_mapbiomas_brazil_collection_11.csv",
        "legend_code_n_classes_mapbiomas_brazil_collection_11.csv",
    )
    LEGEND_SOURCE_CSV = "legend_code_mapbiomas_brazil_collection_11.csv"
    QGIS_STYLE_ZIP = "COVERAGE_QGIS_COL11_PT_EN.zip"
    LEGEND_SEED_FILE = Path(__file__).with_name("data") / "mapbiomas_legend_col11.json"
    BOUNDARY_FILES = (
        "limites_SC.cpg",
        "limites_SC.dbf",
        "limites_SC.geojson",
        "limites_SC.prj",
        "limites_SC.qmd",
        "limites_SC.shp",
        "limites_SC.shx",
    )
    COPY_BUFFER_SIZE = 1024 * 1024
    RECONCILIATION_TOTAL_TOLERANCE_PCT = 0.5
    RECONCILIATION_NON_AQUATIC_TOLERANCE_PCT = 0.1
    BETA_CLASS_IDS = frozenset({7, 62, 84, 91})
    RASTER_TILE_SIZE = 512

    def __init__(self, config: PipelineConfig | None = None) -> None:
        """Create the service with the current medallion and source configuration."""
        self.config = config or PipelineConfig.from_env()

    def _dataset(self, context: TaskExecutionContext) -> MapbiomasDataset:
        """Resolve the target dataset from ``dag_run.conf`` over configured defaults.

        ``conf`` keys ``collection``, ``version`` and ``year`` override the
        ``MAPBIOMAS_*`` configuration so a single DAG serves every historical year.
        """
        conf = context.conf or {}
        return MapbiomasDataset(
            collection=str(conf.get("collection", self.config.mapbiomas_collection)),
            version=str(conf.get("version", self.config.mapbiomas_version)),
            year=int(conf.get("year", self.config.mapbiomas_year)),
        )

    def _mapbiomas_files(self, dataset: MapbiomasDataset) -> tuple[str, ...]:
        """Full Bronze source inventory for one dataset: yearly raster plus companions."""
        return (dataset.source_raster_name, *self.MAPBIOMAS_STATIC_FILES)

    def discover_datasets(self, context: TaskExecutionContext) -> list[dict[str, Any]]:
        """Discover every available year of the configured collection/version.

        Availability is the union of local annual sources, committed Bronze
        packages and published PostGIS assets. A stale year in the incoming
        cadastral conf never narrows the default all-years workflow.
        """
        dataset = self._dataset(context)
        years: set[int] = set()
        pattern = re.compile(r"brazil_coverage-col" + re.escape(dataset.collection) + r"_(\d{4})\.tif$")
        source = Path(self.config.mapbiomas_source_dir)
        for directory in (source, source / "tifs"):
            for path in directory.glob("*.tif"):
                match = pattern.fullmatch(path.name)
                if match:
                    years.add(int(match[1]))
        bronze = Path(self.config.medallion_bronze_path) / "mapbiomas_lulc" / f"collection={dataset.collection}" / f"version={dataset.version}"
        for path in bronze.glob("year=*/manifest.json"):
            match = re.fullmatch(r"year=(\d{4})", path.parent.name)
            if match:
                years.add(int(match[1]))
        # On AWS the S3 Bronze is the source of truth; the local disk is only a cache refreshed
        # when the node boots, so a year uploaded while it runs exists only in the bucket.
        store = self._medallion_store()
        if store is not None:
            prefix = f"mapbiomas_lulc/collection={dataset.collection}/version={dataset.version}/"
            for key in store.remote_keys("bronze", prefix):
                match = re.fullmatch(re.escape(prefix) + r"year=(\d{4})/manifest\.json", key)
                if match:
                    years.add(int(match[1]))
        if self.config.project_db_url:
            with psycopg2.connect(self.config.project_db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT reference_year FROM mapbiomas_raster_asset WHERE collection_code=%s "
                        "AND collection_version=%s AND publication_status='PUBLISHED';",
                        (dataset.collection, dataset.version),
                    )
                    years.update(int(row[0]) for row in cur.fetchall())
        if not years:
            raise MapbiomasPipelineError("No available MapBiomas years were found in raw, Bronze or published assets.")
        if any(year < 1985 or year > 2100 for year in years):
            raise MapbiomasPipelineError("Available MapBiomas year is outside the supported range.")
        return [{"collection": dataset.collection, "version": dataset.version, "year": year} for year in sorted(years)]

    def _medallion_store(self) -> MedallionStore | None:
        """S3 mirror of the Medallion, or None when the pipeline runs local-only."""
        return MedallionStore.from_config(self.config)

    def _bronze_lulc_dir(self, dataset: MapbiomasDataset) -> Path:
        """Bronze partition holding the immutable MapBiomas package for one year."""
        return Path(self.config.medallion_bronze_path, "mapbiomas_lulc", *dataset.partition)

    def _silver_lulc_dir(self, dataset: MapbiomasDataset) -> Path:
        """Silver partition holding the Santa Catarina clip for one year."""
        return Path(self.config.medallion_silver_path, "mapbiomas_lulc", *dataset.partition)

    def _gold_lulc_dir(self, dataset: MapbiomasDataset) -> Path:
        """Gold partition holding the published COG and sidecars for one year."""
        return Path(self.config.medallion_gold_path, "mapbiomas_lulc", *dataset.partition)

    def _boundary_dir(self) -> Path:
        """Bronze partition holding the IBGE 2025 Santa Catarina boundary package."""
        return Path(self.config.medallion_bronze_path, "boundaries", "source=ibge", "year=2025", "area=sc")

    def _validation_reference_dir(self, dataset: MapbiomasDataset) -> Path:
        """Bronze partition holding the official state-area workbook for the collection."""
        return Path(
            self.config.medallion_bronze_path,
            "mapbiomas_lulc",
            "validation_reference",
            f"collection={dataset.collection}",
            f"version={dataset.version}",
            "area=state",
        )

    def bootstrap_bronze(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Publish source packages atomically and return their immutable manifests."""
        dataset = self._dataset(context)
        source = Path(self.config.mapbiomas_source_dir)
        store = self._medallion_store()
        if store is not None and not (self._bronze_lulc_dir(dataset) / "manifest.json").is_file():
            # A year published only in the S3 Bronze is brought to the local cache before use.
            store.pull(("bronze",), key_prefix="mapbiomas_lulc/" + "/".join(dataset.partition) + "/")
        # Published packages remain usable when the raw landing has been removed.
        if (self._bronze_lulc_dir(dataset) / "manifest.json").is_file() and not any(
            (directory / dataset.source_raster_name).is_file() for directory in (source, source / "tifs")
        ):
            return {"mapbiomas": {"status": "replayed", **self._validate_package(self._bronze_lulc_dir(dataset), "mapbiomas_lulc")},
                    "boundary": {"status": "replayed", **self._validate_package(self._boundary_dir(), "ibge_sc_boundary")}}
        mapbiomas = self._publish_package(
            source_dir=Path(self.config.mapbiomas_source_dir),
            filenames=self._mapbiomas_files(dataset),
            destination=self._bronze_lulc_dir(dataset),
            domain="mapbiomas_lulc",
            context=context,
            collection=dataset.collection,
            version=dataset.version,
        )
        boundary = self._publish_package(
            source_dir=Path(self.config.sc_boundary_source_dir),
            filenames=self.BOUNDARY_FILES,
            destination=self._boundary_dir(),
            domain="ibge_sc_boundary",
            context=context,
        )
        return {"mapbiomas": mapbiomas, "boundary": boundary}

    def validate_bronze(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Recompute checksums from published Bronze files and validate both manifests."""
        dataset = self._dataset(context)
        packages = (
            (self._bronze_lulc_dir(dataset), "mapbiomas_lulc"),
            (self._boundary_dir(), "ibge_sc_boundary"),
        )
        results = [self._validate_package(path, domain) for path, domain in packages]
        return {"run_id": context.run_id, "packages": results}

    def publish_validation_reference(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Publish the official state-area workbook as an immutable validation input."""
        source_path = Path(self.config.mapbiomas_statistics_source_path)
        dataset = self._dataset(context)
        reference_dir = self._validation_reference_dir(dataset)
        # Like the raster package, a published reference remains usable without the raw landing.
        if (reference_dir / "manifest.json").is_file() and not source_path.is_file():
            return {"status": "replayed", **self._validate_package(reference_dir, "mapbiomas_area_reference")}
        if not self.config.mapbiomas_statistics_source_path or not source_path.is_file():
            raise MapbiomasPipelineError("Configured official MapBiomas state-statistics workbook is unavailable.")
        return self._publish_package(
            source_dir=source_path.parent,
            filenames=(source_path.name,),
            destination=self._validation_reference_dir(dataset),
            domain="mapbiomas_area_reference",
            context=context,
            collection=dataset.collection,
            version=dataset.version,
        )

    def build_silver(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Crop the Brazilian raster to Santa Catarina without changing its native grid."""
        dataset = self._dataset(context)
        source_dir = self._bronze_lulc_dir(dataset)
        boundary_dir = self._boundary_dir()
        source_manifest = self._validate_package(source_dir, "mapbiomas_lulc")
        boundary_manifest = self._validate_package(boundary_dir, "ibge_sc_boundary")
        source_raster = source_dir / dataset.source_raster_name
        source_legend = source_dir / self.LEGEND_SOURCE_CSV
        boundary = boundary_dir / "limites_SC.geojson"
        destination = self._silver_lulc_dir(dataset)
        manifest_path = destination / "manifest.json"
        if manifest_path.is_file():
            manifest = self._validate_silver(destination, dataset)
            if manifest["source_raster_checksum"] != self._file_checksum(source_manifest, source_raster.name):
                raise MapbiomasPipelineError("Published Silver raster does not match the Bronze source checksum.")
            return {"status": "replayed", "manifest_key": self._relative_key(manifest_path, Path(self.config.medallion_silver_path)), **manifest}
        if destination.exists():
            raise MapbiomasPipelineError(f"Uncommitted Silver directory requires explicit cleanup: {destination}")

        destination.mkdir(parents=True)
        target_raster = destination / dataset.silver_raster_name
        try:
            with rasterio.open(source_raster) as raster:
                boundary_frame = gpd.read_file(boundary).to_crs(raster.crs)
                data, transform = mask(
                    raster,
                    [geometry.__geo_interface__ for geometry in boundary_frame.geometry],
                    crop=True,
                    all_touched=False,
                    nodata=0,
                    filled=True,
                )
                profile = raster.profile.copy()
                profile.update(
                    driver="GTiff", height=data.shape[1], width=data.shape[2], transform=transform,
                    nodata=0, compress="LZW", tiled=True,
                )
                with rasterio.open(target_raster, "w", **profile) as output:
                    output.write(data)
                classes = sorted(int(value) for value in np.unique(data) if value != 0)
                legend = self._load_legend(source_legend)
                unknown_classes = sorted(set(classes).difference(item["class_id"] for item in legend))
                if unknown_classes:
                    raise MapbiomasPipelineError(
                        f"Raster contains classes absent from the official legend: {unknown_classes}"
                    )
                legend_path = destination / dataset.legend_name
                legend_path.write_text(
                    json.dumps(legend, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                manifest = {
                    "schema_version": 1,
                    "domain": "mapbiomas_lulc_silver",
                    "collection": dataset.collection,
                    "version": dataset.version,
                    "year": dataset.year,
                    "source_raster_checksum": self._file_checksum(source_manifest, source_raster.name),
                    "boundary_checksum": self._file_checksum(boundary_manifest, boundary.name),
                    "boundary_policy": "pixel_center",
                    "crs_epsg": raster.crs.to_epsg(),
                    "transform": list(transform)[:6],
                    "width_px": data.shape[2],
                    "height_px": data.shape[1],
                    "nodata_value": 0,
                    "classes_present": classes,
                    "legend_checksum_sha256": self._sha256(legend_path),
                    "legend_source_checksum": self._file_checksum(source_manifest, source_legend.name),
                    "legend_class_count": len(legend),
                    "raster_checksum_sha256": self._sha256(target_raster),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "run_id": context.run_id,
                }
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        except Exception:
            raise
        return {"status": "published", "manifest_key": self._relative_key(manifest_path, Path(self.config.medallion_silver_path)), **manifest}

    def validate_silver(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Validate the committed Silver raster and report its immutable contract."""
        dataset = self._dataset(context)
        return {"run_id": context.run_id, "silver": self._validate_silver(self._silver_lulc_dir(dataset), dataset)}

    def _cadastral_partition(self, context: TaskExecutionContext) -> tuple[str, ...]:
        """Isolate API-derived artifacts from the legacy immutable yearly products."""
        conf = context.conf or {}
        if not conf.get("manifest_key"):
            return ()
        import_id = str(conf.get("import_id") or "").strip()
        if not import_id:
            raise MapbiomasPipelineError("Directed MapBiomas processing requires import_id.")
        return ("imports", "import=" + hashlib.sha256(import_id.encode()).hexdigest())

    def _artifact_partition(self, context: TaskExecutionContext) -> tuple[str, ...]:
        """Isolate the AOI snapshot and statistics of each run.

        A directed run uses its import partition. A full run gets its own partition per run_id:
        its snapshot freezes the areas active when the run starts, and a retry of the same run
        replays what it already committed, while a later full run never reuses an older snapshot.
        """
        partition = self._cadastral_partition(context)
        if partition:
            return partition
        return ("runs", "run=" + hashlib.sha256(context.run_id.encode()).hexdigest())

    def _aoi_snapshot_dir(self, context: TaskExecutionContext) -> Path:
        partition = self._artifact_partition(context)
        return Path(self.config.medallion_silver_path) / "mapbiomas_lulc" / "aoi_snapshot" / Path(*partition)

    def _statistics_dir(self, context: TaskExecutionContext, *, gold: bool = False) -> Path:
        root = self.config.medallion_gold_path if gold else self.config.medallion_silver_path
        folder = "statistics" if gold else "area_statistics"
        return Path(root) / "mapbiomas_lulc" / folder / Path(*self._dataset(context).partition) / Path(*self._artifact_partition(context))

    def _directed_aoi_records(self, context: TaskExecutionContext) -> list[dict[str, Any]]:
        """Resolve each manifest feature to its committed UC and active surroundings.

        PostGIS is read only here: UC/ZA/Buffer de Abrangência have already committed in the parent
        DAGs. The snapshot freezes those geometries and their internal identities
        for all subsequent tasks, including retries and publication.
        """
        conf = context.conf or {}
        domain = str(conf.get("domain") or "uc")
        if domain not in {"uc", "za_oficial"}:
            raise MapbiomasPipelineError("Unsupported cadastral manifest domain.")
        directed = load_manifest_input(Path(self.config.medallion_bronze_path), conf, expected_domain=domain)
        if directed is None:
            raise MapbiomasPipelineError("A cadastral manifest is required.")
        metadata = directed.payload.get("metadata") or {}
        canonical = json.loads(directed.canonical_path.read_text(encoding="utf-8"))
        features = canonical.get("features") or []
        targets = []
        for feature in features if domain == "uc" else [None]:
            props = {str(k).casefold(): v for k, v in ((feature or {}).get("properties") or {}).items()}
            def identity(keys):
                return next((str(props[k]).strip() for k in keys if props.get(k) is not None and str(props[k]).strip()), None)
            official = identity(("uc_id", "id_uc", "id", "gid"))
            cnuc = identity(("cd_cnuc", "cod_cnuc", "cnuc"))
            wdpa = identity(("wdpa_pid", "wdpaid", "wdpa"))
            if domain == "za_oficial":
                official = metadata.get("uc_identifier")
                cnuc = wdpa = official
            elif len(features) == 1:
                official = metadata.get("official_identifier") or official
                cnuc = metadata.get("cd_cnuc") or cnuc
                wdpa = metadata.get("wdpa_pid") or wdpa
                if not any((official, cnuc, wdpa)):
                    official = directed.import_id
            if not any((official, cnuc, wdpa)):
                raise MapbiomasPipelineError("UC feature has no identity for cadastral reprocessing.")
            targets.append((official, cnuc, wdpa))
        if not targets:
            raise MapbiomasPipelineError("Cadastral import has no UC targets.")
        records = []
        seen = set()
        with psycopg2.connect(self.config.project_db_url) as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn.cursor() as cur:
                for official, cnuc, wdpa in targets:
                    cur.execute(
                        "SELECT id_uc, ST_AsGeoJSON(geom) FROM uc WHERE situacao = 'ATIVA' AND (uc_id = %s OR cd_cnuc = %s OR wdpa_pid = %s);",
                        (official, cnuc, wdpa),
                    )
                    matches = cur.fetchall()
                    if len(matches) != 1:
                        raise MapbiomasPipelineError("Cadastral identity must resolve to exactly one committed UC.")
                    id_uc, geometry = matches[0]
                    if id_uc in seen:
                        raise MapbiomasPipelineError("Cadastral batch repeats a committed UC.")
                    seen.add(id_uc)
                    records.append({"id_uc": int(id_uc), "aoi_type": "UC", "zone_id": 0, "geometry": shape(json.loads(geometry))})
                    cur.execute(
                        "SELECT 'ZA', id_za_oficial, ST_AsGeoJSON(geom) FROM za_oficial WHERE id_uc = %s AND fl_ativa "
                        "UNION ALL SELECT 'BUFFER_ABRANGENCIA', id_buffer_abrangencia, ST_AsGeoJSON(geom) FROM buffer_abrangencia WHERE id_uc = %s AND fl_ativa;",
                        (id_uc, id_uc),
                    )
                    zones = cur.fetchall()
                    if len(zones) != 1:
                        raise MapbiomasPipelineError(f"UC {id_uc} must have exactly one active ZA or Buffer de Abrangência before MapBiomas.")
                    kind, zone_id, geometry = zones[0]
                    records.append({"id_uc": int(id_uc), "aoi_type": kind, "zone_id": int(zone_id), "geometry": shape(json.loads(geometry))})
        return records

    # Every active UC with its active zones (ZA oficial or Buffer de Abrangência); `zones` must be 1.
    ACTIVE_AOI_SQL = """
        SELECT u.id_uc, ST_AsGeoJSON(u.geom), z.kind, z.zone_id, ST_AsGeoJSON(z.geom), coalesce(z.zones, 0)
        FROM uc u
        LEFT JOIN LATERAL (
            SELECT kind, zone_id, geom, count(*) OVER () AS zones
            FROM (
                SELECT 'ZA' AS kind, id_za_oficial AS zone_id, geom
                FROM za_oficial WHERE id_uc = u.id_uc AND fl_ativa
                UNION ALL
                SELECT 'BUFFER_ABRANGENCIA', id_buffer_abrangencia, geom
                FROM buffer_abrangencia WHERE id_uc = u.id_uc AND fl_ativa
            ) active_zones
        ) z ON TRUE
        WHERE u.situacao = 'ATIVA'
        ORDER BY u.id_uc, z.kind;
    """

    # Rows of one raster for the snapshotted UCs whose (aoi_type, geometry) is no longer current.
    SUPERSEDE_STATISTICS_SQL = """
        DELETE FROM mapbiomas_clip
        WHERE id_raster_asset = %s
          AND id_uc = ANY(%s)
          AND (id_uc, aoi_type::text, aoi_geometry_sha256::text) NOT IN (
              SELECT * FROM unnest(%s::bigint[], %s::text[], %s::text[]));
    """

    def _active_aoi_records(self) -> list[dict[str, Any]]:
        """Every active UC with its single active ZA or Buffer de Abrangência, read from PostGIS."""
        records = []
        with psycopg2.connect(self.config.project_db_url) as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn.cursor() as cur:
                cur.execute(self.ACTIVE_AOI_SQL)
                rows = cur.fetchall()
        if not rows:
            raise MapbiomasPipelineError("No active UC is committed for the MapBiomas snapshot.")
        for id_uc, uc_geometry, kind, zone_id, zone_geometry, zones in rows:
            if zones != 1:
                raise MapbiomasPipelineError(f"UC {id_uc} must have exactly one active ZA or Buffer de Abrangência before MapBiomas.")
            records.append({"id_uc": int(id_uc), "aoi_type": "UC", "zone_id": 0, "geometry": shape(json.loads(uc_geometry))})
            records.append({"id_uc": int(id_uc), "aoi_type": kind, "zone_id": int(zone_id), "geometry": shape(json.loads(zone_geometry))})
        return records

    @staticmethod
    def _polygonal(geometry):
        """Keep only the polygonal parts of a clipped AOI.

        Clipping a zone by the state boundary can leave slivers as lines or points next to the
        polygons (a GeometryCollection). The raster statistics only accept areas, so the polygons
        are kept as a MultiPolygon; points and lines of a point UC are returned unchanged.
        """
        if geometry.geom_type != "GeometryCollection":
            return geometry
        parts = []
        for part in geometry.geoms:
            if isinstance(part, Polygon):
                parts.append(part)
            elif isinstance(part, MultiPolygon):
                parts.extend(part.geoms)
        return MultiPolygon(parts) if parts else geometry

    def _build_aoi_snapshot(self, context: TaskExecutionContext) -> dict[str, Any]:
        output_dir = self._aoi_snapshot_dir(context)
        manifest_path = output_dir / "manifest.json"
        snapshot_path = output_dir / "mapbiomas_aoi_snapshot.geojson"
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if self._sha256(snapshot_path) != manifest["checksum_sha256"]:
                raise MapbiomasPipelineError("AOI snapshot checksum mismatch.")
            return {"status": "replayed", **manifest}
        directed = bool(self._cadastral_partition(context))
        records = self._directed_aoi_records(context) if directed else self._active_aoi_records()
        frame = gpd.GeoDataFrame(records, geometry="geometry", crs=4674).to_crs(31982)
        boundary = gpd.read_file(self._boundary_dir() / "limites_SC.geojson").to_crs(31982)
        state_geometry = make_valid(boundary.geometry.unary_union)
        uc_geometries = {int(row.id_uc): make_valid(row.geometry) for row in frame.itertuples() if row.aoi_type == "UC"}
        for index, row in frame.iterrows():
            geometry = make_valid(row.geometry)
            if row.aoi_type != "UC":
                geometry = geometry.difference(uc_geometries[int(row.id_uc)])
            frame.at[index, "geometry"] = self._polygonal(geometry.intersection(state_geometry))
        frame = frame[~frame.geometry.is_empty].to_crs(4326)
        if frame.empty:
            raise MapbiomasPipelineError("No AOI intersects Santa Catarina.")
        output_dir.mkdir(parents=True, exist_ok=True)
        frame.to_file(snapshot_path, driver="GeoJSON")
        manifest = {
            "schema_version": 1, "domain": "mapbiomas_aoi_snapshot",
            "identity_kind": "postgis_id_uc", "import_id": (context.conf or {}).get("import_id") if directed else None,
            "aoi_count": len(frame), "uc_count": int(frame["id_uc"].nunique()),
            "counts_by_type": frame["aoi_type"].value_counts().to_dict(),
            "checksum_sha256": self._sha256(snapshot_path), "run_id": context.run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        return {"status": "published", **manifest}

    def build_aoi_snapshot(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Freeze the exclusive UC, official ZA and Buffer de Abrangência AOIs committed in PostGIS.

        A directed run freezes the UCs of its import; a full run freezes every active UC with its
        active zone, including UCs registered through the API. Retries of the same run replay the
        frozen snapshot.
        """
        return self._build_aoi_snapshot(context)

    def compute_area_statistics(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Aggregate MapBiomas classes by exclusive AOI using ellipsoidal pixel areas."""
        dataset = self._dataset(context)
        silver = Path(self.config.medallion_silver_path) / "mapbiomas_lulc"
        silver_dir = self._silver_lulc_dir(dataset)
        raster_path = silver_dir / dataset.silver_raster_name
        legend_path = silver_dir / dataset.legend_name
        aoi_path = self._aoi_snapshot_dir(context) / "mapbiomas_aoi_snapshot.geojson"
        output_dir = self._statistics_dir(context)
        manifest_path = output_dir / "manifest.json"
        if manifest_path.is_file():
            return {"status": "replayed", **json.loads(manifest_path.read_text(encoding="utf-8"))}
        if output_dir.exists():
            raise MapbiomasPipelineError(f"Uncommitted area statistics require explicit cleanup: {output_dir}")
        legend = {int(item["class_id"]): item for item in json.loads(legend_path.read_text(encoding="utf-8"))}
        aois = gpd.read_file(aoi_path).to_crs(4326)
        rows: list[dict[str, Any]] = []
        aois_without_selected_pixels: list[dict[str, Any]] = []
        geod = Geod(ellps="WGS84")
        with rasterio.open(raster_path) as raster:
            row_areas = self._pixel_area_by_row(raster, geod)
            for _, aoi in aois.iterrows():
                if aoi.geometry.geom_type not in {"Polygon", "MultiPolygon"}:
                    aois_without_selected_pixels.append({"id_uc": int(aoi.id_uc), "aoi_type": aoi.aoi_type})
                    continue
                try:
                    window = geometry_window(raster, [aoi.geometry.__geo_interface__]).round_offsets().round_lengths()
                except WindowError:
                    continue
                data = raster.read(1, window=window)
                transform = raster.window_transform(window)
                inside = geometry_mask([aoi.geometry.__geo_interface__], out_shape=data.shape, transform=transform, invert=True, all_touched=False)
                classes = data[inside]
                if not np.any(classes != 0):
                    aois_without_selected_pixels.append({"id_uc": int(aoi.id_uc), "aoi_type": aoi.aoi_type})
                    continue
                weights = np.broadcast_to(row_areas[int(window.row_off):int(window.row_off + window.height), None], data.shape)[inside]
                counts = np.bincount(classes.astype(np.int64), minlength=256)
                hectares = np.bincount(classes.astype(np.int64), weights=weights, minlength=256) / 10000
                aoi_area_ha = abs(geod.geometry_area_perimeter(aoi.geometry)[0]) / 10000
                for class_id in np.flatnonzero(counts):
                    if class_id == 0:
                        continue
                    if int(class_id) not in legend:
                        raise MapbiomasPipelineError(f"AOI raster contains class absent from legend: {class_id}")
                    rows.append({"id_uc": int(aoi.id_uc), "aoi_type": aoi.aoi_type, "class_id": int(class_id), "class_name_pt_br": legend[int(class_id)]["class_name_pt_br"], "pixel_count": int(counts[class_id]), "area_ha": float(hectares[class_id]), "aoi_area_ha_geodesic": float(aoi_area_ha), "area_method": "geodetic_pixel_row", "area_method_version": "1", "boundary_policy": "pixel_center"})
        output_dir.mkdir(parents=True)
        statistics_path = output_dir / "mapbiomas_area_by_aoi_class.parquet"
        pd.DataFrame(rows, columns=["id_uc", "aoi_type", "class_id", "class_name_pt_br", "pixel_count", "area_ha", "aoi_area_ha_geodesic", "area_method", "area_method_version", "boundary_policy"]).to_parquet(statistics_path, index=False)
        manifest = {"schema_version": 1, "domain": "mapbiomas_area_statistics", "collection": dataset.collection, "version": dataset.version, "year": dataset.year, "record_count": len(rows), "aoi_count": int(aois.shape[0]), "aoi_count_with_selected_pixels": int(aois.shape[0] - len(aois_without_selected_pixels)), "aois_without_selected_pixels": aois_without_selected_pixels, "area_method": "geodetic_pixel_row", "boundary_policy": "pixel_center", "statistics_checksum_sha256": self._sha256(statistics_path), "run_id": context.run_id, "created_at": datetime.now(timezone.utc).isoformat()}
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return {"status": "published", **manifest}

    def publish_gold_cog(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Publish a QGIS-ready COG and official MapBiomas visualization sidecars."""
        dataset = self._dataset(context)
        silver = self._silver_lulc_dir(dataset)
        silver_manifest = self._validate_silver(silver, dataset)
        bronze = self._bronze_lulc_dir(dataset)
        bronze_manifest = self._validate_package(bronze, "mapbiomas_lulc")
        output_dir = self._gold_lulc_dir(dataset)
        manifest_path = output_dir / "manifest.json"
        if manifest_path.is_file():
            return {"status": "replayed", **self._validate_gold_cog(output_dir, dataset)}
        if output_dir.exists():
            raise MapbiomasPipelineError(f"Uncommitted Gold COG requires explicit cleanup: {output_dir}")
        output_dir.mkdir(parents=True)
        source_raster = silver / dataset.silver_raster_name
        target_raster = output_dir / dataset.gold_cog_name
        temporary_raster = output_dir / f"{dataset.gold_cog_name}.partial"
        try:
            raster_copy(
                source_raster,
                temporary_raster,
                driver="COG",
                BLOCKSIZE=512,
                COMPRESS="LZW",
                RESAMPLING="NEAREST",
                BIGTIFF="IF_SAFER",
                NUM_THREADS="ALL_CPUS",
            )
            temporary_raster.replace(target_raster)
            self._validate_cog_equivalence(source_raster, target_raster)
            self._copy_and_verify(bronze / self.LEGEND_SOURCE_CSV, output_dir / "legend_terminal_col11.csv")
            with ZipFile(bronze / self.QGIS_STYLE_ZIP) as archive:
                for filename in ("ESTILO_QGIS_COL11_PT.qml", "ESTILO_QGIS_COL11_EN.qml"):
                    if filename not in archive.namelist():
                        raise MapbiomasPipelineError(f"Official QGIS style is missing from Bronze ZIP: {filename}")
                    (output_dir / filename).write_bytes(archive.read(filename))
            manifest = {
                "schema_version": 1,
                "domain": "mapbiomas_lulc_gold_cog",
                "collection": dataset.collection,
                "version": dataset.version,
                "year": dataset.year,
                "source_silver_checksum": silver_manifest["raster_checksum_sha256"],
                "source_legend_checksum": self._file_checksum(bronze_manifest, self.LEGEND_SOURCE_CSV),
                "raster_checksum_sha256": self._sha256(target_raster),
                "styles": {
                    filename: self._sha256(output_dir / filename)
                    for filename in ("ESTILO_QGIS_COL11_PT.qml", "ESTILO_QGIS_COL11_EN.qml")
                },
                "legend_checksum_sha256": self._sha256(output_dir / "legend_terminal_col11.csv"),
                "nodata_value": 0,
                "compression": "LZW",
                "block_size": 512,
                "overview_resampling": "nearest",
                "run_id": context.run_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        except Exception:
            raise
        return {"status": "published", **manifest}

    def reconcile_official_statistics(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Compare Silver statewide class areas with the official Collection 11 workbook."""
        dataset = self._dataset(context)
        silver = self._silver_lulc_dir(dataset)
        self._validate_silver(silver, dataset)
        reference_dir = self._validation_reference_dir(dataset)
        reference_manifest = self._validate_package(reference_dir, "mapbiomas_area_reference")
        workbook = reference_dir / Path(self.config.mapbiomas_statistics_source_path).name
        output_dir = Path(self.config.medallion_silver_path) / "mapbiomas_lulc" / "quality" / Path(*dataset.partition) / "area=state"
        manifest_path = output_dir / "manifest.json"
        if manifest_path.is_file():
            return {"status": "replayed", **json.loads(manifest_path.read_text(encoding="utf-8"))}
        if output_dir.exists():
            raise MapbiomasPipelineError(f"Uncommitted MapBiomas reconciliation requires explicit cleanup: {output_dir}")
        year_column = f"y{dataset.year}"
        official = pd.read_excel(workbook, sheet_name=f"COVERAGE_{dataset.collection}", usecols=["state", "class", year_column])
        legend = {int(item["class_id"]) for item in json.loads((silver / dataset.legend_name).read_text(encoding="utf-8"))}
        official = official.loc[official["state"].eq("Santa Catarina") & official["class"].isin(legend), ["class", year_column]]
        official = official.groupby("class", as_index=False)[year_column].sum().rename(columns={"class": "class_id", year_column: "official_area_ha"})
        calculated_m2 = np.zeros(256, dtype=np.float64)
        with rasterio.open(silver / dataset.silver_raster_name) as raster:
            row_areas = self._pixel_area_by_row(raster, Geod(ellps="WGS84"))
            for row in range(raster.height):
                values = raster.read(1, window=((row, row + 1), (0, raster.width))).ravel()
                calculated_m2 += np.bincount(values.astype(np.int64), minlength=256)[:256] * row_areas[row]
        calculated = pd.DataFrame({"class_id": sorted(legend)})
        calculated["calculated_area_ha"] = calculated["class_id"].map(lambda class_id: calculated_m2[class_id] / 10000)
        report = official.merge(calculated, on="class_id", how="outer", validate="one_to_one").fillna(0.0)
        report["difference_ha"] = report["calculated_area_ha"] - report["official_area_ha"]
        report["relative_difference_pct"] = np.where(report["official_area_ha"] > 0, report["difference_ha"] / report["official_area_ha"] * 100, np.nan)
        total_official_area = float(report["official_area_ha"].sum())
        total_calculated_area = float(report["calculated_area_ha"].sum())
        non_aquatic = report.loc[report["class_id"] != 33]
        non_aquatic_official_area = float(non_aquatic["official_area_ha"].sum())
        non_aquatic_calculated_area = float(non_aquatic["calculated_area_ha"].sum())
        output_dir.mkdir(parents=True)
        report_path = output_dir / "mapbiomas_sc_class_area_reconciliation.parquet"
        report.to_parquet(report_path, index=False)
        manifest = {
            "schema_version": 1,
            "domain": "mapbiomas_state_area_reconciliation",
            "state": "Santa Catarina",
            "collection": dataset.collection,
            "version": dataset.version,
            "year": dataset.year,
            "reference_checksum_sha256": self._file_checksum(reference_manifest, workbook.name),
            "record_count": int(report.shape[0]),
            "total_official_area_ha": total_official_area,
            "total_calculated_area_ha": total_calculated_area,
            "total_relative_difference_pct": (total_calculated_area / total_official_area - 1) * 100,
            "non_aquatic_class_ids_excluded": [33],
            "non_aquatic_official_area_ha": non_aquatic_official_area,
            "non_aquatic_calculated_area_ha": non_aquatic_calculated_area,
            "non_aquatic_relative_difference_pct": (non_aquatic_calculated_area / non_aquatic_official_area - 1) * 100,
            "maximum_absolute_difference_ha": float(report["difference_ha"].abs().max()),
            "maximum_relative_difference_pct": float(report["relative_difference_pct"].abs().max(skipna=True)),
            "report_checksum_sha256": self._sha256(report_path),
            "area_method": "geodetic_pixel_row",
            "boundary_policy": "pixel_center",
            "acceptance_tolerance_pct": None,
            "run_id": context.run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return {"status": "published", **manifest}

    def publish_gold_statistics(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Publish validated AOI class statistics as QGIS- and API-ready Gold files."""
        dataset = self._dataset(context)
        silver_root = Path(self.config.medallion_silver_path) / "mapbiomas_lulc"
        statistics_path = self._statistics_dir(context) / "mapbiomas_area_by_aoi_class.parquet"
        reconciliation_path = silver_root / "quality" / Path(*dataset.partition) / "area=state" / "manifest.json"
        legend_path = self._silver_lulc_dir(dataset) / dataset.legend_name
        cog_manifest_path = self._gold_lulc_dir(dataset) / "manifest.json"
        output_dir = self._statistics_dir(context, gold=True)
        manifest_path = output_dir / "manifest.json"
        if manifest_path.is_file():
            return {"status": "replayed", **json.loads(manifest_path.read_text(encoding="utf-8"))}
        if output_dir.exists():
            raise MapbiomasPipelineError(f"Uncommitted Gold statistics require explicit cleanup: {output_dir}")
        reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
        total_difference = abs(float(reconciliation["total_relative_difference_pct"]))
        non_aquatic_difference = abs(float(reconciliation["non_aquatic_relative_difference_pct"]))
        if total_difference > self.RECONCILIATION_TOTAL_TOLERANCE_PCT or non_aquatic_difference > self.RECONCILIATION_NON_AQUATIC_TOLERANCE_PCT:
            raise MapbiomasPipelineError("Official state-area reconciliation does not meet the approved acceptance policy.")
        legend = pd.DataFrame(json.loads(legend_path.read_text(encoding="utf-8"))).rename(columns={"hex_code": "class_hex_color"})
        legend["class_is_beta"] = legend["class_id"].isin(self.BETA_CLASS_IDS)
        statistics = pd.read_parquet(statistics_path)
        enriched = statistics.drop(columns=["class_name_pt_br"]).merge(legend, on="class_id", how="left", validate="many_to_one")
        if enriched[["class_name_pt_br", "class_name_en", "class_hex_color"]].isna().any().any():
            raise MapbiomasPipelineError("Gold statistics contain a class absent from the official legend.")
        grouped = enriched.groupby(["id_uc", "aoi_type"], as_index=False)["area_ha"].sum().rename(columns={"area_ha": "classified_area_ha"})
        enriched = enriched.merge(grouped, on=["id_uc", "aoi_type"], how="left", validate="many_to_one")
        enriched["coverage_ratio"] = enriched["classified_area_ha"] / enriched["aoi_area_ha_geodesic"]
        cog_manifest = json.loads(cog_manifest_path.read_text(encoding="utf-8"))
        enriched.insert(0, "collection", dataset.collection)
        enriched.insert(1, "version", dataset.version)
        enriched.insert(2, "year", dataset.year)
        enriched.insert(3, "raster_checksum_sha256", cog_manifest["raster_checksum_sha256"])
        enriched["published_at"] = datetime.now(timezone.utc).isoformat()
        output_dir.mkdir(parents=True)
        parquet_path = output_dir / f"{dataset.statistics_stem}.parquet"
        csv_path = output_dir / f"{dataset.statistics_stem}.csv"
        enriched.to_parquet(parquet_path, index=False)
        enriched.to_csv(csv_path, index=False, encoding="utf-8")
        manifest = {
            "schema_version": 1,
            "domain": "mapbiomas_area_statistics_gold",
            "collection": dataset.collection,
            "version": dataset.version,
            "year": dataset.year,
            "record_count": int(enriched.shape[0]),
            "parquet_checksum_sha256": self._sha256(parquet_path),
            "csv_checksum_sha256": self._sha256(csv_path),
            "gold_cog_checksum_sha256": cog_manifest["raster_checksum_sha256"],
            "reconciliation_report_checksum_sha256": reconciliation["report_checksum_sha256"],
            "total_reconciliation_difference_pct": total_difference,
            "non_aquatic_reconciliation_difference_pct": non_aquatic_difference,
            "total_reconciliation_tolerance_pct": self.RECONCILIATION_TOTAL_TOLERANCE_PCT,
            "non_aquatic_reconciliation_tolerance_pct": self.RECONCILIATION_NON_AQUATIC_TOLERANCE_PCT,
            "area_method": "geodetic_pixel_row",
            "boundary_policy": "pixel_center",
            "run_id": context.run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return {"status": "published", **manifest}

    # ------------------------------------------------------------------
    # PostgreSQL/PostGIS publication (post-Gold)
    # ------------------------------------------------------------------

    def seed_legend_postgres(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Upsert the versioned MapBiomas legend into ``mapbiomas_legend_class``.

        The legend is scoped to the collection and version, not the year: a
        multi-year backfill seeds it once. Rows are rewritten only when the seed
        file content changes, tracked by ``source_checksum``.
        """
        if not self.config.project_db_url:
            raise MapbiomasPipelineError("PROJECT_DB_URL is required for seed_legend_postgres.")
        dataset = self._dataset(context)
        seed = json.loads(self.LEGEND_SEED_FILE.read_text(encoding="utf-8"))
        classes = seed["classes"]
        checksum = hashlib.sha256(
            json.dumps(classes, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        terminal_seed = sorted(item["class_code"] for item in classes if item["is_terminal"])
        inserted = updated = unchanged = 0
        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                for item in classes:
                    cur.execute(
                        """
                        INSERT INTO mapbiomas_legend_class
                          (collection_code, collection_version, class_code, parent_class_code,
                           hierarchy_level, name_pt_br, name_en, color_hex, is_terminal, is_active, source_checksum)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                        ON CONFLICT (collection_code, collection_version, class_code) DO UPDATE SET
                           parent_class_code = EXCLUDED.parent_class_code,
                           hierarchy_level   = EXCLUDED.hierarchy_level,
                           name_pt_br        = EXCLUDED.name_pt_br,
                           name_en           = EXCLUDED.name_en,
                           color_hex         = EXCLUDED.color_hex,
                           is_terminal       = EXCLUDED.is_terminal,
                           is_active         = TRUE,
                           source_checksum   = EXCLUDED.source_checksum
                        WHERE mapbiomas_legend_class.source_checksum IS DISTINCT FROM EXCLUDED.source_checksum
                        RETURNING (xmax = 0) AS was_insert;
                        """,
                        (
                            dataset.collection, dataset.version, item["class_code"], item["parent_class_code"],
                            item["hierarchy_level"], item["name_pt_br"], item["name_en"], item["color_hex"],
                            item["is_terminal"], checksum,
                        ),
                    )
                    row = cur.fetchone()
                    if row is None:
                        unchanged += 1
                    elif row[0]:
                        inserted += 1
                    else:
                        updated += 1
                cur.execute(
                    """
                    SELECT count(*), count(*) FILTER (WHERE is_terminal)
                    FROM mapbiomas_legend_class
                    WHERE collection_code = %s AND collection_version = %s;
                    """,
                    (dataset.collection, dataset.version),
                )
                total, terminal_total = cur.fetchone()
        if total != len(classes) or terminal_total != len(terminal_seed):
            raise MapbiomasPipelineError(
                f"Legend seed mismatch: database holds {total}/{terminal_total} classes, "
                f"seed defines {len(classes)}/{len(terminal_seed)}."
            )
        return {
            "status": "seeded", "collection": dataset.collection, "version": dataset.version,
            "source_checksum": checksum, "class_count": int(total), "terminal_count": int(terminal_total),
            "inserted": inserted, "updated": updated, "unchanged": unchanged, "run_id": context.run_id,
        }

    def load_raster_postgres(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Load the Gold COG for one dataset into PostGIS as an in-db tiled raster.

        There is **one physical table per year**, ``mapbiomas_raster_<year>``
        (e.g. ``mapbiomas_raster_2025``). It is registered in ``mapbiomas_raster_asset``
        and QGIS lists and renders it directly. Tiling is a physical partition only:
        each tile is a verbatim window of the COG at the native EPSG:4326 grid and
        resolution, same origin, no resampling, no reprojection, so the categorical
        class values are byte-identical to the COG. Edge tiles keep their natural
        (smaller) size so the tile set's global extent matches the COG exactly. The
        load is transactional and idempotent by the COG checksum; an incomplete or
        differently-tiled previous load drops and rebuilds the year table.

        There is no unified all-years raster table: cross-year analysis uses the
        area statistics in ``mapbiomas_clip``, not a raster union. In-db overviews
        are intentionally not created (the COG carries internal pyramids; nearest
        decimation of a categorical raster drops rare classes).
        """
        if not self.config.project_db_url:
            raise MapbiomasPipelineError("PROJECT_DB_URL is required for load_raster_postgres.")
        dataset = self._dataset(context)
        year_table = dataset.raster_table
        gold_dir = self._gold_lulc_dir(dataset)
        gold_manifest = self._validate_gold_cog(gold_dir, dataset)
        cog_path = gold_dir / dataset.gold_cog_name
        cog_checksum = gold_manifest["raster_checksum_sha256"]
        storage_key = self._relative_key(cog_path, Path(self.config.medallion_gold_path))
        # tile_size is a physical partition size (pixels per tile), never a
        # resampling factor; it is overridable via dag_run.conf for the benchmark.
        tile = int((context.conf or {}).get("raster_tile_size", self.RASTER_TILE_SIZE))

        with rasterio.open(cog_path) as src:
            width, height = src.width, src.height
            bounds = src.bounds
            affine6 = list(src.transform)[:6]
            expected_tiles = self._tile_count(width, height, tile)
            with psycopg2.connect(self.config.project_db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET search_path TO public;")
                    cur.execute(
                        "SELECT id_raster_asset FROM mapbiomas_raster_asset WHERE checksum_sha256 = %s;",
                        (cog_checksum,),
                    )
                    row = cur.fetchone()
                    if self._cadastral_partition(context) and row is not None:
                        asset_id = int(row[0])
                        cur.execute("SELECT publication_status FROM mapbiomas_raster_asset WHERE id_raster_asset = %s;", (asset_id,))
                        if cur.fetchone()[0] != "PUBLISHED":
                            raise MapbiomasPipelineError("Cadastral reprocessing requires a published raster asset.")
                        cur.execute("SELECT to_regclass(%s);", (f"public.{year_table}",))
                        if cur.fetchone()[0] is None:
                            raise MapbiomasPipelineError("Published annual raster table is unavailable.")
                        cur.execute(f"SELECT count(*) FROM {year_table} WHERE id_raster_asset = %s;", (asset_id,))
                        tile_count = int(cur.fetchone()[0])
                        if not tile_count:
                            raise MapbiomasPipelineError("Published annual raster contains no tiles.")
                        return {"status": "replayed", "id_raster_asset": asset_id, "year_table": year_table, "tile_count": tile_count, "run_id": context.run_id}
                    if row is not None:
                        asset_id = int(row[0])
                        cur.execute("SELECT to_regclass(%s);", (f"public.{year_table}",))
                        table_exists = cur.fetchone()[0] is not None
                        if table_exists:
                            cur.execute(f"SELECT count(*) FROM {year_table} WHERE id_raster_asset = %s;", (asset_id,))
                            if int(cur.fetchone()[0]) == expected_tiles:
                                return {
                                    "status": "replayed", "id_raster_asset": asset_id,
                                    "year_table": year_table, "tile_count": expected_tiles,
                                    "run_id": context.run_id,
                                }
                    else:
                        cur.execute(
                            """
                            INSERT INTO mapbiomas_raster_asset
                              (collection_code, collection_version, reference_year, coverage_scope,
                               storage_key, checksum_sha256, byte_size, media_type, suggested_filename,
                               crs_epsg, affine_transform, width_px, height_px, band_count, data_type,
                               nodata_value, compression, overview_levels, footprint, publication_status,
                               run_id, published_at)
                            VALUES (%s, %s, %s, 'SC', %s, %s, %s, 'image/tiff; application=geotiff', %s,
                                    4326, %s, %s, %s, 1, 'uint8', 0, 'LZW', '[]'::jsonb,
                                    ST_Multi(ST_Transform(ST_MakeEnvelope(%s, %s, %s, %s, 4326), 4674)),
                                    'STAGED', %s, NULL)
                            RETURNING id_raster_asset;
                            """,
                            (
                                dataset.collection, dataset.version, dataset.year,
                                storage_key, cog_checksum, cog_path.stat().st_size, dataset.gold_cog_name,
                                json.dumps(affine6), width, height,
                                bounds.left, bounds.bottom, bounds.right, bounds.top,
                                context.run_id,
                            ),
                        )
                        asset_id = int(cur.fetchone()[0])

                    self._create_year_raster_table(cur, year_table, dataset)
                    base_tiles = self._insert_raster_tiles(
                        cur, year_table, asset_id, dataset, self._iter_raster_tiles(src, tile),
                    )
                    self._finalise_year_raster_table(cur, year_table, dataset)
                    cur.execute(
                        "UPDATE mapbiomas_raster_asset SET publication_status = 'PUBLISHED', "
                        "published_at = now() WHERE id_raster_asset = %s;",
                        (asset_id,),
                    )
                    registered = self._raster_registration_report(cur, year_table)

        return {
            "status": "published", "id_raster_asset": asset_id, "year_table": year_table,
            "storage_key": storage_key, "checksum_sha256": cog_checksum, "tile_size": tile,
            "tile_count": base_tiles, "expected_tile_count": expected_tiles, "raster_columns": registered,
            "collection": dataset.collection, "version": dataset.version, "year": dataset.year,
            "run_id": context.run_id,
        }

    def load_area_statistics_postgres(self, context: TaskExecutionContext) -> dict[str, Any]:
        """Load the Gold area-by-AOI statistics for one dataset into ``mapbiomas_clip``.

        Maps the snapshot ``id_uc`` to the active UC, the active
        ``id_za_oficial``/``id_buffer_abrangencia`` per AOI type, the legend class and the raster
        asset, then inserts one fact row per class per AOI. Idempotent through the
        natural unique index.
        """
        if not self.config.project_db_url:
            raise MapbiomasPipelineError("PROJECT_DB_URL is required for load_area_statistics_postgres.")
        dataset = self._dataset(context)
        gold_dir = self._gold_lulc_dir(dataset)
        statistics_dir = self._statistics_dir(context, gold=True)
        parquet_path = statistics_dir / f"{dataset.statistics_stem}.parquet"
        if not parquet_path.is_file():
            raise MapbiomasPipelineError(f"Gold statistics parquet is missing: {parquet_path}")
        gold_manifest = self._validate_gold_cog(gold_dir, dataset)
        cog_checksum = gold_manifest["raster_checksum_sha256"]
        snapshot_dir = self._aoi_snapshot_dir(context)
        geometry_index, geometry_version = self._aoi_geometry_index(snapshot_dir)
        snapshot = json.loads((snapshot_dir / "mapbiomas_aoi_snapshot.geojson").read_text(encoding="utf-8"))
        snapshot_zones = {
            (int(feature["properties"]["id_uc"]), feature["properties"]["aoi_type"]): int(feature["properties"]["zone_id"])
            for feature in snapshot["features"]
        }

        frame = pd.read_parquet(parquet_path)
        with psycopg2.connect(self.config.project_db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id_raster_asset FROM mapbiomas_raster_asset WHERE checksum_sha256 = %s;",
                    (cog_checksum,),
                )
                asset_row = cur.fetchone()
                if asset_row is None:
                    raise MapbiomasPipelineError(
                        "Raster asset for this dataset is not loaded; run load_raster_postgres first."
                    )
                asset_id = int(asset_row[0])
                # The snapshot carries the internal PostGIS id_uc in both directed and full runs.
                cur.execute("SELECT id_uc FROM uc WHERE situacao = 'ATIVA' ORDER BY id_uc FOR SHARE;")
                uc_by_external = {str(row[0]): int(row[0]) for row in cur.fetchall()}
                cur.execute("SELECT id_uc FROM uc WHERE situacao = 'EXTINTA';")
                extinct_ids = {str(row[0]) for row in cur.fetchall()}
                cur.execute("SELECT id_uc, id_za_oficial FROM za_oficial WHERE fl_ativa;")
                za_by_uc = {int(id_uc): int(id_za) for id_uc, id_za in cur.fetchall()}
                cur.execute("SELECT id_uc, id_buffer_abrangencia FROM buffer_abrangencia WHERE fl_ativa;")
                buffer_by_uc = {int(id_uc): int(id_buffer_abrangencia) for id_uc, id_buffer_abrangencia in cur.fetchall()}
                cur.execute(
                    "SELECT class_code, id_legend_class FROM mapbiomas_legend_class "
                    "WHERE collection_code = %s AND collection_version = %s;",
                    (dataset.collection, dataset.version),
                )
                legend_by_code = {int(code): int(id_legend) for code, id_legend in cur.fetchall()}
                if not legend_by_code:
                    raise MapbiomasPipelineError("Legend is not seeded; run seed_legend_postgres first.")

                rows: list[tuple] = []
                for record in frame.itertuples(index=False):
                    external_uc = int(record.id_uc)
                    aoi_type = str(record.aoi_type)
                    class_code = int(record.class_id)
                    if str(external_uc) in extinct_ids:
                        continue
                    if str(external_uc) not in uc_by_external:
                        raise MapbiomasPipelineError(f"Gold statistics reference an unknown UC id: {external_uc}")
                    if class_code not in legend_by_code:
                        raise MapbiomasPipelineError(f"Gold statistics reference a class absent from the legend: {class_code}")
                    id_uc = uc_by_external[str(external_uc)]
                    id_za_oficial = za_by_uc.get(id_uc) if aoi_type == "ZA" else None
                    id_buffer_abrangencia = buffer_by_uc.get(id_uc) if aoi_type == "BUFFER_ABRANGENCIA" else None
                    if aoi_type != "UC" and snapshot_zones.get((id_uc, aoi_type)) != (id_za_oficial or id_buffer_abrangencia):
                        raise MapbiomasPipelineError("Active zone changed after the cadastral snapshot; submit a new reprocessing run.")
                    if aoi_type == "ZA" and id_za_oficial is None:
                        raise MapbiomasPipelineError(f"No active ZA for UC {external_uc} required by a ZA statistic row.")
                    if aoi_type == "BUFFER_ABRANGENCIA" and id_buffer_abrangencia is None:
                        raise MapbiomasPipelineError(f"No active Buffer de Abrangência for UC {external_uc} required by o Buffer de Abrangência statistic row.")
                    geometry_sha = geometry_index.get((external_uc, aoi_type))
                    if geometry_sha is None:
                        raise MapbiomasPipelineError(f"AOI snapshot has no geometry for UC {external_uc} / {aoi_type}.")
                    rows.append((
                        asset_id, legend_by_code[class_code], id_uc, id_za_oficial, id_buffer_abrangencia,
                        dataset.collection, dataset.version, dataset.year, aoi_type,
                        geometry_version, geometry_sha,
                        float(record.aoi_area_ha_geodesic), int(record.pixel_count),
                        float(record.area_ha), float(record.classified_area_ha), float(record.coverage_ratio),
                        str(record.area_method), str(record.area_method_version), str(record.boundary_policy),
                        cog_checksum, context.run_id,
                    ))
                # mapbiomas_clip holds the statistics of the current AOIs only. Rows of the same
                # raster computed for an older geometry of a snapshotted UC, or for a zone it no
                # longer has (Buffer replaced by an official ZA), are superseded in this
                # transaction; otherwise a sum by year would count the same area twice.
                current = sorted({(row[2], row[8], row[10]) for row in rows})
                cur.execute(
                    self.SUPERSEDE_STATISTICS_SQL,
                    (
                        asset_id,
                        sorted({int(id_uc) for id_uc, _, _ in current}),
                        [item[0] for item in current], [item[1] for item in current], [item[2] for item in current],
                    ),
                )
                superseded = cur.rowcount
                execute_values(
                    cur,
                    """
                    INSERT INTO mapbiomas_clip
                      (id_raster_asset, id_legend_class, id_uc, id_za_oficial, id_buffer_abrangencia,
                       collection_code, collection_version, reference_year, aoi_type,
                       aoi_geometry_version, aoi_geometry_sha256,
                       aoi_area_ha_geodesic, pixel_count,
                       class_area_ha, classified_area_ha, coverage_ratio,
                       area_method, area_method_version, boundary_policy,
                       source_checksum, run_id)
                    VALUES %s
                    ON CONFLICT (id_raster_asset, id_legend_class, aoi_type, id_uc,
                                 COALESCE(id_za_oficial, 0), COALESCE(id_buffer_abrangencia, 0),
                                 aoi_geometry_sha256, area_method_version) DO NOTHING;
                    """,
                    rows,
                    page_size=500,
                )
                cur.execute(
                    "SELECT count(*), aoi_type FROM mapbiomas_clip WHERE id_raster_asset = %s GROUP BY aoi_type ORDER BY aoi_type;",
                    (asset_id,),
                )
                counts_by_type = {aoi_type: int(count) for count, aoi_type in cur.fetchall()}
        total = sum(counts_by_type.values())
        return {
            "status": "loaded", "id_raster_asset": asset_id, "record_count": total,
            "superseded_record_count": int(superseded or 0),
            "source_record_count": int(frame.shape[0]), "counts_by_type": counts_by_type,
            "collection": dataset.collection, "version": dataset.version, "year": dataset.year,
            "run_id": context.run_id,
        }

    # ------------------------------------------------------------------
    # Raster load helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _tile_count(width: int, height: int, tile: int) -> int:
        """Number of ``tile`` x ``tile`` blocks that partition a raster of this size."""
        return ((width + tile - 1) // tile) * ((height + tile - 1) // tile)

    @staticmethod
    def _iter_raster_tiles(src, tile: int):
        """Yield ``(tile_row, tile_col, geotiff_bytes)`` verbatim windows of ``src``.

        Each tile is a plain read of the source at its native grid and resolution;
        no resampling, no reprojection, no padding. Edge tiles are their natural
        smaller size so the union keeps the exact source extent.
        """
        for row_index, row_off in enumerate(range(0, src.height, tile)):
            block_height = min(tile, src.height - row_off)
            for col_index, col_off in enumerate(range(0, src.width, tile)):
                block_width = min(tile, src.width - col_off)
                window = Window(col_off, row_off, block_width, block_height)
                data = src.read(1, window=window)
                profile = {
                    "driver": "GTiff", "height": block_height, "width": block_width, "count": 1,
                    "dtype": "uint8", "crs": "EPSG:4326",
                    "transform": src.window_transform(window), "nodata": 0,
                }
                with MemoryFile() as memfile:
                    with memfile.open(**profile) as dataset:
                        dataset.write(data, 1)
                    payload = memfile.read()
                yield row_index, col_index, payload

    @staticmethod
    def _create_year_raster_table(cur, table_name: str, dataset: MapbiomasDataset) -> None:
        """Drop and recreate the empty per-year raster table ``mapbiomas_raster_<year>``.

        One physical table per year is the QGIS-usable form: it is registered in
        ``raster_columns`` with a real SRID/scale/pixel type (a filtered view is
        not — it lands there with ``srid = 0``). Dropping first makes the load
        idempotent and lets ``tile_size`` change between runs.
        """
        if not 1985 <= dataset.year <= 2100 or table_name != f"mapbiomas_raster_{dataset.year}":
            raise MapbiomasPipelineError("Invalid annual raster table target.")
        cur.execute(f"DROP TABLE IF EXISTS public.{table_name};")
        cur.execute("SELECT public.ensure_mapbiomas_raster_year(%s);", (dataset.year,))
        cur.execute(
            f"COMMENT ON TABLE {table_name} IS "
            f"'MapBiomas cobertura {dataset.year} (colecao {dataset.collection}) - "
            f"raster in-db tiled para QGIS; recriada a cada carga.';"
        )

    def _finalise_year_raster_table(self, cur, table_name: str, dataset: MapbiomasDataset) -> None:
        """Add the spatial index and raster constraints after the tiles are loaded.

        The unqualified table-name overload of ``AddRasterConstraints`` is used with
        ``search_path`` pinned to ``public`` because the schema-qualified overload is
        ambiguous with its VARIADIC form. ``blocksize`` is not enforced because edge
        tiles keep their natural (smaller) size to preserve the exact COG extent.
        """
        cur.execute(
            f"CREATE INDEX idx_{table_name}_hull ON {table_name} USING GIST (ST_ConvexHull(rast));"
        )
        cur.execute(
            "SELECT AddRasterConstraints(%s::name, 'rast'::name, "
            "'srid','scale','same_alignment','num_bands','pixel_types','nodata_values','extent');",
            (table_name,),
        )

    @staticmethod
    def _insert_raster_tiles(cur, table_name: str, asset_id: int, dataset: MapbiomasDataset, tiles) -> int:
        """Insert an iterable of ``(row, col, geotiff_bytes)`` tiles into one year table."""
        count = 0
        for tile_row, tile_col, payload in tiles:
            cur.execute(
                f"INSERT INTO {table_name} "
                f"(id_raster_asset, collection_code, collection_version, reference_year, "
                f" tile_row, tile_col, rast) "
                f"VALUES (%s, %s, %s, %s, %s, %s, ST_FromGDALRaster(%s, 4326));",
                (
                    asset_id, dataset.collection, dataset.version, dataset.year,
                    tile_row, tile_col, psycopg2.Binary(payload),
                ),
            )
            count += 1
        return count

    @staticmethod
    def _raster_registration_report(cur, table_name: str) -> dict[str, Any]:
        """Read back the ``raster_columns`` row for one per-year raster table."""
        cur.execute(
            "SELECT srid, num_bands, pixel_types, nodata_values, "
            "scale_x, scale_y, ST_AsText(extent) "
            "FROM raster_columns WHERE r_table_name = %s;",
            (table_name,),
        )
        row = cur.fetchone()
        if row is None:
            return {}
        srid, num_bands, pixel_types, nodata_values, scale_x, scale_y, extent = row
        return {
            "table": table_name, "srid": srid, "num_bands": num_bands,
            "pixel_types": list(pixel_types) if pixel_types is not None else None,
            "nodata_values": list(nodata_values) if nodata_values is not None else None,
            "scale_x": float(scale_x) if scale_x is not None else None,
            "scale_y": float(scale_y) if scale_y is not None else None,
            "extent": extent,
        }

    @staticmethod
    def _aoi_geometry_index(snapshot_dir: Path) -> tuple[dict[tuple[int, str], str], str]:
        """Map ``(external_uc_id, aoi_type)`` to a stable SHA-256 of the AOI geometry."""
        manifest_path = snapshot_dir / "manifest.json"
        snapshot_path = snapshot_dir / "mapbiomas_aoi_snapshot.geojson"
        if not manifest_path.is_file() or not snapshot_path.is_file():
            raise MapbiomasPipelineError(f"AOI snapshot is missing: {snapshot_dir}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        version = f"snapshot:{str(manifest.get('checksum_sha256', ''))[:16]}"
        features = json.loads(snapshot_path.read_text(encoding="utf-8")).get("features", [])
        index: dict[tuple[int, str], str] = {}
        for feature in features:
            properties = feature.get("properties", {})
            key = (int(properties["id_uc"]), str(properties["aoi_type"]))
            index[key] = hashlib.sha256(shape(feature["geometry"]).wkb).hexdigest()
        return index, version

    def _publish_package(
        self,
        *,
        source_dir: Path,
        filenames: tuple[str, ...],
        destination: Path,
        domain: str,
        context: TaskExecutionContext,
        collection: str | None = None,
        version: str | None = None,
    ) -> dict[str, Any]:
        """Copy one finite package through a sibling staging directory and atomic rename."""
        if not source_dir.is_dir():
            raise MapbiomasPipelineError(f"Configured source directory is unavailable: {source_dir}")
        source_files = [source_dir / name for name in filenames]
        if domain == "mapbiomas_lulc":
            source_files = [
                source_dir / "tifs" / path.name if path.suffix == ".tif" and not path.is_file() else path
                for path in source_files
            ]
        missing = [str(path.name) for path in source_files if not path.is_file()]
        if missing:
            raise MapbiomasPipelineError(f"Missing required {domain} source files: {', '.join(missing)}")

        expected_files = self._describe_files(source_files, source_dir)
        if destination.exists():
            if not (destination / "manifest.json").is_file():
                raise MapbiomasPipelineError(
                    f"Uncommitted Bronze package (no manifest.json, likely an aborted run) "
                    f"requires explicit cleanup: {destination}"
                )
            manifest = self._validate_package(destination, domain)
            if manifest["files"] != expected_files:
                raise MapbiomasPipelineError(
                    f"Published Bronze package differs from configured source: {destination}"
                )
            return {"status": "replayed", "manifest_key": self._relative_key(destination / "manifest.json"), **manifest}

        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            destination.mkdir(parents=False)
            for source_path in source_files:
                target_path = destination / source_path.name
                self._copy_and_verify(source_path, target_path)
            manifest = {
                "schema_version": 1,
                "domain": domain,
                "collection": collection if collection is not None else ("11" if domain == "mapbiomas_lulc" else None),
                "version": version if version is not None else ("1" if domain == "mapbiomas_lulc" else "IBGE_2025"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "run_id": context.run_id,
                "files": expected_files,
            }
            (destination / "manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except Exception:
            raise
        return {"status": "published", "manifest_key": self._relative_key(destination / "manifest.json"), **manifest}

    def _validate_package(self, directory: Path, domain: str) -> dict[str, Any]:
        """Validate a published package without consulting the external source directory."""
        manifest_path = directory / "manifest.json"
        if not manifest_path.is_file():
            raise MapbiomasPipelineError(f"Bronze manifest is missing: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("domain") != domain:
            raise MapbiomasPipelineError(f"Bronze manifest domain is invalid: {manifest_path}")
        files = manifest.get("files")
        if not isinstance(files, list) or not files:
            raise MapbiomasPipelineError(f"Bronze manifest has no file inventory: {manifest_path}")
        for item in files:
            filename = item.get("name")
            target = directory / str(filename)
            if not target.is_file() or self._sha256(target) != item.get("checksum_sha256"):
                raise MapbiomasPipelineError(f"Bronze checksum validation failed: {target}")
            if target.stat().st_size != item.get("byte_size"):
                raise MapbiomasPipelineError(f"Bronze size validation failed: {target}")
        return manifest

    def _validate_silver(self, directory: Path, dataset: MapbiomasDataset) -> dict[str, Any]:
        """Validate raster checksum and metadata stored in a committed Silver manifest."""
        manifest_path = directory / "manifest.json"
        raster_path = directory / dataset.silver_raster_name
        legend_path = directory / dataset.legend_name
        if not manifest_path.is_file() or not raster_path.is_file() or not legend_path.is_file():
            raise MapbiomasPipelineError(f"Silver raster or manifest is missing: {directory}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("domain") != "mapbiomas_lulc_silver":
            raise MapbiomasPipelineError(f"Invalid Silver manifest domain: {manifest_path}")
        if self._sha256(raster_path) != manifest.get("raster_checksum_sha256"):
            raise MapbiomasPipelineError(f"Silver raster checksum validation failed: {raster_path}")
        with rasterio.open(raster_path) as raster:
            if raster.crs.to_epsg() != manifest.get("crs_epsg") or raster.nodata != 0:
                raise MapbiomasPipelineError(f"Silver raster metadata validation failed: {raster_path}")
        if self._sha256(legend_path) != manifest.get("legend_checksum_sha256"):
            raise MapbiomasPipelineError(f"Silver legend checksum validation failed: {legend_path}")
        legend = json.loads(legend_path.read_text(encoding="utf-8"))
        legend_ids = {int(item["class_id"]) for item in legend}
        if not set(manifest.get("classes_present", [])).issubset(legend_ids):
            raise MapbiomasPipelineError(f"Silver legend does not cover every raster class: {legend_path}")
        return manifest

    def _validate_gold_cog(self, directory: Path, dataset: MapbiomasDataset) -> dict[str, Any]:
        """Validate the committed COG, legend and styles without rebuilding them."""
        manifest_path = directory / "manifest.json"
        raster_path = directory / dataset.gold_cog_name
        if not manifest_path.is_file() or not raster_path.is_file():
            raise MapbiomasPipelineError(f"Gold COG or manifest is missing: {directory}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("domain") != "mapbiomas_lulc_gold_cog":
            raise MapbiomasPipelineError(f"Invalid Gold COG manifest domain: {manifest_path}")
        if self._sha256(raster_path) != manifest.get("raster_checksum_sha256"):
            raise MapbiomasPipelineError(f"Gold COG checksum validation failed: {raster_path}")
        with rasterio.open(raster_path) as raster:
            if raster.driver != "GTiff" or raster.nodata != 0 or raster.compression.value != "LZW":
                raise MapbiomasPipelineError(f"Gold COG metadata validation failed: {raster_path}")
            if not raster.overviews(1):
                raise MapbiomasPipelineError(f"Gold COG has no overview pyramid: {raster_path}")
        for filename, checksum in manifest.get("styles", {}).items():
            if self._sha256(directory / filename) != checksum:
                raise MapbiomasPipelineError(f"Gold QGIS style checksum validation failed: {filename}")
        if self._sha256(directory / "legend_terminal_col11.csv") != manifest.get("legend_checksum_sha256"):
            raise MapbiomasPipelineError("Gold terminal legend checksum validation failed.")
        return manifest

    @staticmethod
    def _validate_cog_equivalence(source_path: Path, target_path: Path) -> None:
        """Prove that COG encoding preserved the categorical Silver grid and values."""
        with rasterio.open(source_path) as source, rasterio.open(target_path) as target:
            if (
                source.crs != target.crs
                or source.transform != target.transform
                or source.width != target.width
                or source.height != target.height
                or source.count != target.count
                or source.nodata != target.nodata
                or source.dtypes != target.dtypes
            ):
                raise MapbiomasPipelineError("Gold COG grid contract differs from the Silver raster.")
            for _, window in source.block_windows(1):
                if not np.array_equal(source.read(1, window=window), target.read(1, window=window)):
                    raise MapbiomasPipelineError("Gold COG values differ from the Silver raster.")

    @staticmethod
    def _load_legend(path: Path) -> list[dict[str, Any]]:
        """Read the official terminal-class CSV into a stable typed legend contract."""
        with path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        legend = [
            {
                "class_id": int(row["class_id"]),
                "class_name_pt_br": row["class_name_pt_br"].strip(),
                "class_name_en": row["class_name_en"].strip(),
                "hex_code": row["hex_code"].strip(),
            }
            for row in rows
        ]
        if not legend or len({item["class_id"] for item in legend}) != len(legend):
            raise MapbiomasPipelineError(f"Official legend is empty or has duplicate class IDs: {path}")
        return sorted(legend, key=lambda item: item["class_id"])

    @staticmethod
    def _pixel_area_by_row(dataset: rasterio.io.DatasetReader, geod: Geod) -> np.ndarray:
        """Return WGS84 ellipsoidal area in square metres for one pixel in every raster row."""
        areas = np.empty(dataset.height, dtype=np.float64)
        left = dataset.transform.c
        right = left + dataset.transform.a
        for row in range(dataset.height):
            top = dataset.transform.f + row * dataset.transform.e
            bottom = top + dataset.transform.e
            areas[row] = abs(geod.polygon_area_perimeter([left, right, right, left], [top, top, bottom, bottom])[0])
        return areas

    def _describe_files(self, paths: list[Path], source_dir: Path) -> list[dict[str, Any]]:
        """Create a deterministic inventory with relative names, byte sizes and SHA-256 values."""
        return [
            {
                "name": path.name,
                "byte_size": path.stat().st_size,
                "checksum_sha256": self._sha256(path),
            }
            for path in paths
        ]

    COPY_MAX_ATTEMPTS = 4

    def _copy_and_verify(self, source_path: Path, target_path: Path) -> None:
        """Copy a file in bounded chunks and prove byte identity using SHA-256.

        Large reads over a virtualised or networked filesystem (e.g. a Docker
        Desktop bind mount) can raise a transient ``OSError`` mid-stream, so the
        streamed copy is retried a few times with a short backoff; the partial
        target is discarded before each retry. A checksum mismatch is a contract
        failure, not transient, and is never retried.
        """
        target_path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(1, self.COPY_MAX_ATTEMPTS + 1):
            source_hash = hashlib.sha256()
            try:
                with source_path.open("rb") as source_handle, target_path.open("xb") as target_handle:
                    while chunk := source_handle.read(self.COPY_BUFFER_SIZE):
                        source_hash.update(chunk)
                        target_handle.write(chunk)
                break
            except OSError as error:
                target_path.unlink(missing_ok=True)
                if attempt == self.COPY_MAX_ATTEMPTS:
                    raise MapbiomasPipelineError(
                        f"I/O error copying {source_path.name} after {attempt} attempts: {error}"
                    ) from error
                time.sleep(2 * attempt)
        if source_hash.hexdigest() != self._sha256(target_path):
            raise MapbiomasPipelineError(f"Checksum mismatch after Bronze copy: {source_path.name}")

    def _relative_key(self, path: Path, root: Path | None = None) -> str:
        """Return a POSIX storage key relative to the configured Bronze root."""
        return path.relative_to(root or Path(self.config.medallion_bronze_path)).as_posix()

    @staticmethod
    def _file_checksum(manifest: dict[str, Any], filename: str) -> str:
        """Return a declared file checksum or fail when the inventory is incomplete."""
        for item in manifest["files"]:
            if item["name"] == filename:
                return str(item["checksum_sha256"])
        raise MapbiomasPipelineError(f"Manifest does not inventory required file: {filename}")

    @staticmethod
    def _sha256(path: Path) -> str:
        """Calculate a file SHA-256 using bounded memory."""
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(MapbiomasPipelineService.COPY_BUFFER_SIZE):
                digest.update(chunk)
        return digest.hexdigest()
