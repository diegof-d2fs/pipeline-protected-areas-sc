from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point, box

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.ucs_pipeline import UcsPipelineService
from scripts_python.za_buffer_pipeline import ZaBufferPipelineError, ZaBufferPipelineService


class DirectedUcImportTest(unittest.TestCase):
    def test_explicit_zone_link_cannot_be_reassigned_by_larger_intersection(self) -> None:
        service = ZaBufferPipelineService(PipelineConfig("", "", "/tmp/bronze", "/tmp/silver", "/tmp/gold", "/tmp/work"))
        units = gpd.GeoDataFrame(
            {"uc_id": ["TARGET", "OTHER"]},
            geometry=[Point(-50, -27), box(-50.02, -27.02, -49.98, -26.98)], crs="EPSG:4674",
        )
        zones = gpd.GeoDataFrame(
            {"_manifest_uc_identifier": ["TARGET", "MISSING"]},
            geometry=[box(-50.03, -27.03, -49.97, -26.97)] * 2, crs="EPSG:4674",
        )
        self.assertEqual(service._selected_za_matches(units, zones), {0: "TARGET"})

    def test_zone_readiness_blocks_zero_or_multiple_active_zones(self) -> None:
        ZaBufferPipelineService._assert_zone_readiness_rows(
            [(1, "UC-OFFICIAL", 1, 0, 0), (2, "UC-BUFFER", 0, 1, 0)]
        )

        with self.assertRaisesRegex(ZaBufferPipelineError, "ZONE_READINESS_FAILED"):
            ZaBufferPipelineService._assert_zone_readiness_rows(
                [(3, "UC-WITHOUT-ZONE", 0, 0, 0)]
            )

        with self.assertRaisesRegex(ZaBufferPipelineError, "ZONE_READINESS_FAILED"):
            ZaBufferPipelineService._assert_zone_readiness_rows(
                [(4, "UC-WITH-TWO-ZONES", 1, 1, 0)]
            )

    def test_uc_manifest_activates_historical_bronze_za_and_suppresses_buffer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bronze = root / "bronze"
            batch = bronze / "ucs" / "import_id=uc-with-historical-za"
            canonical = batch / "canonical" / "data.geojson"
            canonical.parent.mkdir(parents=True)
            uc_geometry = box(-52.60, -26.47, -52.58, -26.45)
            gpd.GeoDataFrame(
                {"uc_id": ["UC-HIST-ZA"], "nome_uc": ["UC com ZA histórica"]},
                geometry=[uc_geometry],
                crs="EPSG:4674",
            ).to_file(canonical, driver="GeoJSON")
            checksum = "c" * 64
            manifest = {
                "schema_version": "1.0",
                "import_id": "uc-with-historical-za",
                "domain": "uc",
                "operation": "create",
                "created_at": "2026-09-13T12:00:00+00:00",
                "metadata": {
                    "source": "teste automatizado",
                    "official_identifier": "UC-HIST-ZA",
                },
                "original": {"checksum_sha256": checksum},
                "bronze": {
                    "canonical_key": "ucs/import_id=uc-with-historical-za/canonical/data.geojson"
                },
            }
            (batch / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            historical_za = bronze / "za" / "2020-01-01-00-00-00"
            historical_za.mkdir(parents=True)
            za_geometry = box(-52.61, -26.48, -52.57, -26.44).difference(uc_geometry)
            gpd.GeoDataFrame(
                {
                    "id_za_ofic": [77],
                    "zam_nm_mic": ["ZA histórica da UC"],
                    "ds_fonte": ["bronze histórica"],
                },
                geometry=[za_geometry],
                crs="EPSG:4674",
            ).to_file(historical_za / "za.geojson", driver="GeoJSON")

            config = PipelineConfig(
                project_db_url="postgresql://unused",
                mutation_db_url="postgresql://unused",
                medallion_bronze_path=str(bronze),
                medallion_silver_path=str(root / "silver"),
                medallion_gold_path=str(root / "gold"),
                medallion_tmp_path=str(root / "tmp"),
            )
            context = TaskExecutionContext(
                dag_id="DAG_ZA_BUFFER",
                stage="extract",
                logical_date="2026-09-13T12:00:00+00:00",
                run_id="chain__api__uc-with-historical-za__DAG_ZA_BUFFER",
                conf={
                    "import_id": "uc-with-historical-za",
                    "domain": "uc",
                    "manifest_key": "ucs/import_id=uc-with-historical-za/manifest.json",
                    "checksum_sha256": checksum,
                },
            )
            service = ZaBufferPipelineService(config)

            extracted_za = service.extract_za(context)
            transformed_za = service.transform_za(context)
            service.extract_buffer(context)
            transformed_buffer = service.transform_buffer(context)
            za_rows = service._read_geodata(
                Path(transformed_za["transformed_path"]), layer="za_oficial_stage"
            )
            buffer_rows = service._read_geodata(
                Path(transformed_buffer["transformed_path"]), layer="buffer_abrangencia_stage"
            )

            self.assertEqual(
                extracted_za["source_mode"], "historical_za_bronze_for_uc_manifest"
            )
            self.assertEqual(extracted_za["selected_records"], 1)
            self.assertEqual(za_rows.iloc[0]["id_za_oficial_source"], 77)
            self.assertEqual(len(buffer_rows), 0)

    def test_postgres_official_za_cannot_override_bronze_buffer_eligibility(self) -> None:
        with self.assertRaisesRegex(ZaBufferPipelineError, "Bronze is the source of truth"):
            ZaBufferPipelineService._assert_postgres_does_not_override_bronze(
                {12, 13, 14}, {13}
            )

        ZaBufferPipelineService._assert_postgres_does_not_override_bronze(
            {12, 13, 14}, set()
        )

    def test_update_manifest_overrides_file_update_flag_and_target_identity(self) -> None:
        config = PipelineConfig(
            project_db_url="postgresql://unused",
            mutation_db_url="postgresql://unused",
            medallion_bronze_path="unused",
            medallion_silver_path="unused",
            medallion_gold_path="unused",
            medallion_tmp_path="unused",
        )
        source = gpd.GeoDataFrame(
            {"uc_id": ["IDENTIDADE-DO-ARQUIVO"], "update_geom": [False]},
            geometry=[Point(-49.1, -27.1)],
            crs="EPSG:4674",
        )
        state = {
            "manifest": {
                "operation": "update",
                "metadata": {"official_identifier": "UC-ALVO-001"},
            }
        }

        uc_result = UcsPipelineService(config)._apply_manifest_metadata(source, state)
        buffer_result = ZaBufferPipelineService(config)._apply_manifest_metadata(
            source, state, branch="buffer"
        )

        self.assertEqual(uc_result.iloc[0]["uc_id"], "UC-ALVO-001")
        self.assertTrue(bool(uc_result.iloc[0]["update_geom"]))
        self.assertEqual(buffer_result.iloc[0]["uc_id"], "UC-ALVO-001")
        self.assertTrue(bool(buffer_result.iloc[0]["update_geom"]))

    def test_manifest_selects_exact_canonical_and_injects_business_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bronze = root / "bronze"
            batch = bronze / "ucs" / "import_id=test-import-001"
            canonical = batch / "canonical" / "data.geojson"
            canonical.parent.mkdir(parents=True)
            canonical.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "crs": {"type": "name", "properties": {"name": "EPSG:4674"}},
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"update_geom": True},
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [-49.1, -27.1],
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            checksum = "a" * 64
            manifest = {
                "schema_version": "1.0",
                "import_id": "test-import-001",
                "domain": "uc",
                "operation": "create",
                "created_at": "2026-08-16T20:00:00+00:00",
                "metadata": {
                    "name": "UC pontual dirigida",
                    "source": "teste automatizado",
                    "official_identifier": "UC-DIRECTED-001",
                },
                "original": {"checksum_sha256": checksum},
                "bronze": {"canonical_key": "ucs/import_id=test-import-001/canonical/data.geojson"},
            }
            (batch / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            config = PipelineConfig(
                project_db_url="postgresql://unused",
                mutation_db_url="postgresql://unused",
                medallion_bronze_path=str(bronze),
                medallion_silver_path=str(root / "silver"),
                medallion_gold_path=str(root / "gold"),
                medallion_tmp_path=str(root / "tmp"),
            )
            context = TaskExecutionContext(
                dag_id="DAG_UCS",
                stage="extract",
                logical_date="2026-08-16T20:00:00+00:00",
                run_id="api__test-import-001",
                conf={
                    "import_id": "test-import-001",
                    "domain": "uc",
                    "manifest_key": "ucs/import_id=test-import-001/manifest.json",
                    "checksum_sha256": checksum,
                },
            )
            service = UcsPipelineService(config)

            extracted = service.extract(context)
            with patch.object(service, "_find_create_duplicates", return_value=[]):
                validated = service.validate(context)
            transformed = service.transform(context)

            result = service._read_geodata(Path(transformed["transformed_path"]), layer="uc")
            self.assertEqual(extracted["source_mode"], "manifest_geojson")
            self.assertEqual(validated["records"], 1)
            self.assertEqual(result.iloc[0]["uc_id"], "UC-DIRECTED-001")
            self.assertEqual(result.iloc[0]["nm_uc"], "UC pontual dirigida")
            self.assertEqual(result.iloc[0]["update_geom"], "FALSE")
            self.assertEqual(result.geometry.iloc[0].geom_type, "Point")

            legacy = bronze / "ucs" / "2026-08-15-00-00-00"
            legacy.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"uc_id": ["UC-LEGACY"], "nm_uc": ["UC legada"]},
                geometry=[Point(-48.9, -27.1)],
                crs="EPSG:4674",
            ).to_file(legacy / "legacy.geojson", driver="GeoJSON")

            zone_service = ZaBufferPipelineService(config)
            zone_extracted = zone_service.extract_buffer(context)
            zone_validated = zone_service.validate_buffer(context)
            zone_transformed = zone_service.transform_buffer(context)
            zones = zone_service._read_geodata(
                Path(zone_transformed["transformed_path"]), layer="buffer_abrangencia_stage"
            )
            self.assertEqual(zone_extracted["source_mode"], "manifest_geojson")
            self.assertEqual(zone_validated["records"], 1)
            self.assertEqual(len(zones), 1)
            self.assertEqual(zones.iloc[0]["uc_id_source"], "UC-DIRECTED-001")
            self.assertEqual(zones.geometry.iloc[0].geom_type, "MultiPolygon")

    def test_directed_create_writes_duplicate_result_for_api(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bronze = root / "bronze"
            batch = bronze / "ucs" / "import_id=duplicate-001"
            canonical = batch / "canonical" / "data.geojson"
            canonical.parent.mkdir(parents=True)
            canonical.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "crs": {"type": "name", "properties": {"name": "EPSG:4674"}},
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"uc_id": "UC-001", "nm_uc": "UC repetida"},
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [-49.1, -27.1],
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            checksum = "b" * 64
            manifest = {
                "schema_version": "1.0",
                "import_id": "duplicate-001",
                "domain": "uc",
                "operation": "create",
                "created_at": "2026-08-16T20:00:00+00:00",
                "metadata": {"source": "teste"},
                "original": {"checksum_sha256": checksum},
                "bronze": {
                    "canonical_key": "ucs/import_id=duplicate-001/canonical/data.geojson"
                },
            }
            (batch / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            config = PipelineConfig(
                project_db_url="postgresql://unused",
                mutation_db_url="postgresql://unused",
                medallion_bronze_path=str(bronze),
                medallion_silver_path=str(root / "silver"),
                medallion_gold_path=str(root / "gold"),
                medallion_tmp_path=str(root / "tmp"),
            )
            context = TaskExecutionContext(
                dag_id="DAG_UCS",
                stage="validate",
                logical_date="2026-08-16T20:00:00+00:00",
                run_id="api__duplicate-001",
                conf={
                    "import_id": "duplicate-001",
                    "domain": "uc",
                    "manifest_key": "ucs/import_id=duplicate-001/manifest.json",
                    "checksum_sha256": checksum,
                },
            )
            service = UcsPipelineService(config)
            service.extract(context)
            match = {
                "uc_id": 42,
                "official_identifier": "UC-001",
                "cd_cnuc": None,
                "wdpa_pid": None,
                "name": "UC repetida",
                "matched_by": "uc_id",
            }

            with patch.object(service, "_find_create_duplicates", return_value=[match]):
                with self.assertRaisesRegex(Exception, "UC_ALREADY_EXISTS"):
                    service.validate(context)

            result_path = (
                root
                / "quality"
                / "api_results"
                / "import_id=duplicate-001"
                / "result.json"
            )
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "DUPLICATE")
            self.assertEqual(result["duplicate_matches"], [match])


if __name__ == "__main__":
    unittest.main()
