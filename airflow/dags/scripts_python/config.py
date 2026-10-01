"""Application configuration objects for geospatial pipeline DAG tasks."""

from __future__ import annotations

from dataclasses import dataclass
from os import getenv


@dataclass(frozen=True)
class PipelineConfig:
    """Centralized runtime settings loaded from environment variables."""

    project_db_url: str
    mutation_db_url: str
    medallion_bronze_path: str
    medallion_silver_path: str
    medallion_gold_path: str
    medallion_tmp_path: str
    mapbiomas_source_dir: str = ""
    sc_boundary_source_dir: str = ""
    mapbiomas_statistics_source_path: str = ""
    mapbiomas_collection: str = "11"
    mapbiomas_version: str = "1"
    mapbiomas_year: int = 2025
    firms_map_key: str = ""
    firms_api_base_url: str = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
    firms_request_bbox: str = "-53.8371493,-29.3550659,-48.3278753,-25.9557228"
    firms_daily_products: tuple[str, ...] = ("VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT")
    firms_backfill_products: tuple[str, ...] = ("VIIRS_SNPP_SP", "VIIRS_NOAA20_SP", "MODIS_SP")
    firms_overlap_days: int = 1
    firms_backfill_batch_size: int = 12
    firms_connect_timeout_seconds: float = 10.0
    firms_read_timeout_seconds: float = 60.0
    firms_max_attempts: int = 4
    mapbiomas_alerta_email: str = ""
    mapbiomas_alerta_password: str = ""
    mapbiomas_alerta_api_url: str = "https://plataforma.alerta.mapbiomas.org/api/v2/graphql"
    mapbiomas_alerta_timeout_seconds: float = 60.0
    mapbiomas_alerta_territory_ids: tuple[int, ...] = (18393, 18387, 18407)
    mapbiomas_alerta_page_size: int = 100
    mapbiomas_alerta_max_attempts: int = 4
    # Com os dois buckets configurados, o S3 é a fonte da verdade e o disco local é cache.
    s3_bronze_bucket: str = ""
    s3_lake_bucket: str = ""
    aws_region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> PipelineConfig:
        """Build a strongly-typed configuration using current process environment."""
        return cls(
            project_db_url=getenv("PROJECT_DB_URL", ""),
            mutation_db_url=getenv("MUTATION_DB_URL", ""),
            medallion_bronze_path=getenv("MEDALLION_BRONZE_PATH", "./data/bronze"),
            medallion_silver_path=getenv("MEDALLION_SILVER_PATH", "./data/silver"),
            medallion_gold_path=getenv("MEDALLION_GOLD_PATH", "./data/gold"),
            medallion_tmp_path=getenv("MEDALLION_TMP_PATH", "./data/tmp"),
            mapbiomas_source_dir=getenv("MAPBIOMAS_SOURCE_DIR", "./data/raw/mapbiomas"),
            sc_boundary_source_dir=getenv("SC_BOUNDARY_SOURCE_DIR", "./data/raw/ibge"),
            mapbiomas_statistics_source_path=getenv(
                "MAPBIOMAS_STATISTICS_SOURCE_PATH",
                "./data/raw/mapbiomas/MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx",
            ),
            mapbiomas_collection=getenv("MAPBIOMAS_COLLECTION", "11"),
            mapbiomas_version=getenv("MAPBIOMAS_VERSION", "1"),
            mapbiomas_year=int(getenv("MAPBIOMAS_YEAR", "2025")),
            firms_map_key=getenv("FIRMS_MAP_KEY", ""),
            firms_api_base_url=getenv(
                "FIRMS_API_BASE_URL",
                "https://firms.modaps.eosdis.nasa.gov/api/area/csv",
            ).rstrip("/"),
            firms_request_bbox=getenv(
                "FIRMS_REQUEST_BBOX",
                "-53.8371493,-29.3550659,-48.3278753,-25.9557228",
            ),
            firms_daily_products=tuple(
                item.strip()
                for item in getenv(
                    "FIRMS_DAILY_PRODUCTS",
                    "VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT",
                ).split(",")
                if item.strip()
            ),
            firms_backfill_products=tuple(
                item.strip()
                for item in getenv(
                    "FIRMS_BACKFILL_PRODUCTS",
                    "VIIRS_SNPP_SP,VIIRS_NOAA20_SP,MODIS_SP",
                ).split(",")
                if item.strip()
            ),
            firms_overlap_days=int(getenv("FIRMS_OVERLAP_DAYS", "1")),
            firms_backfill_batch_size=int(getenv("FIRMS_BACKFILL_BATCH_SIZE", "12")),
            firms_connect_timeout_seconds=float(
                getenv("FIRMS_CONNECT_TIMEOUT_SECONDS", "10")
            ),
            firms_read_timeout_seconds=float(getenv("FIRMS_READ_TIMEOUT_SECONDS", "60")),
            firms_max_attempts=int(getenv("FIRMS_MAX_ATTEMPTS", "4")),
            mapbiomas_alerta_email=getenv("MAPBIOMAS_ALERTA_EMAIL", ""),
            mapbiomas_alerta_password=getenv("MAPBIOMAS_ALERTA_PASSWORD", ""),
            mapbiomas_alerta_api_url=getenv(
                "MAPBIOMAS_ALERTA_API_URL",
                "https://plataforma.alerta.mapbiomas.org/api/v2/graphql",
            ),
            mapbiomas_alerta_timeout_seconds=float(
                getenv("MAPBIOMAS_ALERTA_TIMEOUT_SECONDS", "60")
            ),
            mapbiomas_alerta_territory_ids=tuple(
                int(item.strip())
                for item in getenv("MAPBIOMAS_ALERTA_TERRITORY_IDS", "18393,18387,18407").split(",")
                if item.strip()
            ),
            mapbiomas_alerta_page_size=int(getenv("MAPBIOMAS_ALERTA_PAGE_SIZE", "100")),
            mapbiomas_alerta_max_attempts=int(getenv("MAPBIOMAS_ALERTA_MAX_ATTEMPTS", "4")),
            s3_bronze_bucket=getenv("S3_BRONZE_BUCKET", ""),
            s3_lake_bucket=getenv("S3_LAKE_BUCKET", ""),
            aws_region=getenv("AWS_REGION", "us-east-1"),
        )
