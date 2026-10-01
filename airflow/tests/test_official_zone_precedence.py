from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import geopandas as gpd
import psycopg2
from shapely.geometry import MultiPolygon, Point, Polygon

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.za_buffer_pipeline import ZaBufferPipelineService


class OfficialZonePrecedenceIntegrationTest(unittest.TestCase):
    def test_replace_buffer_manifest_runs_only_official_zone_branch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bronze = root / "bronze"
            import_id = "replace-buffer-abrangencia-routing"
            batch = bronze / "za" / f"import_id={import_id}"
            canonical = batch / "canonical" / "data.geojson"
            canonical.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"name": ["ZA oficial dirigida"]},
                geometry=[Polygon([(-49.2, -27.2), (-49.0, -27.2), (-49.0, -27.0), (-49.2, -27.2)])],
                crs="EPSG:4674",
            ).to_file(canonical, driver="GeoJSON")
            checksum = "d" * 64
            manifest = {
                "schema_version": "1.0",
                "import_id": import_id,
                "domain": "za_oficial",
                "operation": "replace_buffer_abrangencia",
                "created_at": "2026-09-27T20:00:00+00:00",
                "metadata": {
                    "source": "teste automatizado",
                    "uc_identifier": "UC-ROUTING-001",
                    "reason": "Teste de roteamento",
                },
                "original": {"checksum_sha256": checksum},
                "bronze": {
                    "canonical_key": f"za/import_id={import_id}/canonical/data.geojson"
                },
            }
            (batch / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            service = ZaBufferPipelineService(self._config("postgresql://unused", root))
            context = TaskExecutionContext(
                dag_id="DAG_ZA_BUFFER",
                stage="extract",
                logical_date="2026-09-27T20:00:00+00:00",
                run_id=f"api__{import_id}",
                conf={
                    "import_id": import_id,
                    "domain": "za_oficial",
                    "manifest_key": f"za/import_id={import_id}/manifest.json",
                    "checksum_sha256": checksum,
                },
            )

            official = service.extract_za(context)
            buffer = service.extract_buffer(context)

            self.assertEqual(official["status"], "extracted")
            self.assertEqual(official["source_mode"], "manifest_geojson")
            self.assertEqual(buffer["status"], "skipped")
            self.assertEqual(buffer["reason"], "not_requested")

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_spatial_fk_requires_authoritative_source_and_is_disabled_for_api(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        self._create_uc_with_active_buffer(database_url, token)
        touching = Polygon(
            [(-49.0, -27.2), (-48.9, -27.2), (-48.9, -27.0), (-49.0, -27.0)]
        )

        with tempfile.TemporaryDirectory() as temporary:
            service = ZaBufferPipelineService(self._config(database_url, Path(temporary)))
            transformed = gpd.GeoDataFrame(
                {
                    "uc_id_source": [None],
                    "cd_cnuc_source": [None],
                    "nm_uc_source": [None],
                    "ds_fonte": ["https://fonte-oficial.example/za"],
                },
                geometry=[MultiPolygon([touching])],
                crs="EPSG:4674",
            ).rename_geometry("geom")

            api_rejected = service._resolve_uc_fk(
                transformed,
                allow_spatial=False,
                require_authoritative_source_for_spatial=True,
            )
            legacy_match = service._resolve_uc_fk(
                transformed,
                allow_spatial=True,
                require_authoritative_source_for_spatial=True,
            )
            without_source = transformed.copy()
            without_source["ds_fonte"] = None
            untrusted_rejected = service._resolve_uc_fk(
                without_source,
                allow_spatial=True,
                require_authoritative_source_for_spatial=True,
            )

        self.assertTrue(api_rejected.isna().all())
        # O banco de mutação acumula fixtures sobrepostas de execuções anteriores; o ponto
        # deste teste é provar que a fonte oficial habilita o fallback legado, não escolher uma
        # fixture específica entre geometrias deliberadamente idênticas.
        self.assertFalse(legacy_match.isna().any())
        self.assertTrue(untrusted_rejected.isna().all())

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_replace_buffer_is_atomic_audited_and_idempotent(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        id_uc, id_buffer_abrangencia = self._create_uc_with_active_buffer(database_url, token)
        import_id = str(uuid4())
        idempotency_key = f"idem-replace-buffer-abrangencia-{token}"

        with tempfile.TemporaryDirectory() as temporary:
            service = ZaBufferPipelineService(self._config(database_url, Path(temporary)))
            context = self._context(import_id)
            state = self._state(
                import_id,
                idempotency_key,
                uc_identifier=f"TEST-ZA-{token}",
            )
            transformed = self._frame(
                id_uc,
                MultiPolygon(
                    [Polygon([(-49.21, -27.21), (-48.99, -27.21), (-48.99, -26.99), (-49.21, -26.99)])]
                ),
            )

            result = service._load_directed_replace_buffer(
                context=context,
                state=state,
                transformed=transformed,
                source_records=1,
                skipped_no_fk=0,
                rejected_frames={},
            )
            replay = service._load_directed_replace_buffer(
                context=context,
                state=state,
                transformed=transformed,
                source_records=1,
                skipped_no_fk=0,
                rejected_frames={},
            )

        with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT fl_ativa, dt_fim_vigencia FROM buffer_abrangencia WHERE id_buffer_abrangencia = %s",
                (id_buffer_abrangencia,),
            )
            buffer = cursor.fetchone()
            cursor.execute(
                """
                SELECT COUNT(*), BOOL_AND(fl_ativa), MIN(numero_versao), MAX(import_id::text)
                FROM za_oficial WHERE id_uc = %s
                """,
                (id_uc,),
            )
            za = cursor.fetchone()
            cursor.execute(
                """
                SELECT event_type, previous_state->>'id_buffer_abrangencia', new_state->>'active_zone_type'
                FROM cadastral_event WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            )
            event = cursor.fetchone()

        self.assertEqual(result["inserted_records"], 1)
        self.assertEqual(result["deactivated_buffer"], 1)
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(buffer, (False, datetime.now(timezone.utc).date()))
        self.assertEqual(za, (1, True, 1, import_id))
        self.assertEqual(event, ("BUFFER_ABRANGENCIA_REPLACED_BY_OFFICIAL_ZA", str(id_buffer_abrangencia), "ZA_OFICIAL"))

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_failed_official_zone_insert_rolls_back_buffer_deactivation(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        id_uc, id_buffer_abrangencia = self._create_uc_with_active_buffer(database_url, token)
        import_id = str(uuid4())
        idempotency_key = f"idem-rollback-buffer-abrangencia-{token}"

        with tempfile.TemporaryDirectory() as temporary:
            service = ZaBufferPipelineService(self._config(database_url, Path(temporary)))
            with self.assertRaises(psycopg2.Error):
                service._load_directed_replace_buffer(
                    context=self._context(import_id),
                    state=self._state(import_id, idempotency_key, f"TEST-ZA-{token}"),
                    transformed=self._frame(id_uc, Point(-49.1, -27.1)),
                    source_records=1,
                    skipped_no_fk=0,
                    rejected_frames={},
                )

        with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT fl_ativa, dt_fim_vigencia FROM buffer_abrangencia WHERE id_buffer_abrangencia = %s",
                (id_buffer_abrangencia,),
            )
            buffer = cursor.fetchone()
            cursor.execute("SELECT COUNT(*) FROM za_oficial WHERE id_uc = %s", (id_uc,))
            za_count = cursor.fetchone()[0]
            cursor.execute(
                "SELECT COUNT(*) FROM cadastral_event WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            event_count = cursor.fetchone()[0]

        self.assertEqual(buffer, (True, None))
        self.assertEqual(za_count, 0)
        self.assertEqual(event_count, 0)

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_database_trigger_rejects_two_active_zone_types(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        id_uc, id_buffer_abrangencia = self._create_uc_with_active_buffer(database_url, token)
        polygon_wkt = MultiPolygon(
            [Polygon([(-49.21, -27.21), (-48.99, -27.21), (-48.99, -26.99), (-49.21, -26.99)])]
        ).wkt

        with self.assertRaises(psycopg2.errors.CheckViolation):
            with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO za_oficial (id_uc, ds_fonte, geom)
                    VALUES (%s, 'teste de trigger', ST_SetSRID(ST_GeomFromText(%s), 4674));
                    """,
                    (id_uc, polygon_wkt),
                )

        with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT fl_ativa FROM buffer_abrangencia WHERE id_buffer_abrangencia = %s", (id_buffer_abrangencia,))
            self.assertTrue(cursor.fetchone()[0])
            cursor.execute("SELECT COUNT(*) FROM za_oficial WHERE id_uc = %s", (id_uc,))
            self.assertEqual(cursor.fetchone()[0], 0)

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_scheduled_buffer_load_versions_only_changed_geometry(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        id_uc, id_buffer_abrangencia = self._create_uc_with_active_buffer(database_url, token)
        same_ring = MultiPolygon(
            [Polygon([(-49.2, -27.2), (-49.0, -27.2), (-49.0, -27.0), (-49.2, -27.0)])]
        )
        changed_ring = MultiPolygon(
            [Polygon([(-49.3, -27.3), (-49.0, -27.3), (-49.0, -27.0), (-49.3, -27.0)])]
        )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = ZaBufferPipelineService(self._config(database_url, root))

            unchanged = service.load_postgres_buffer(
                self._scheduled_buffer_context(service, root, f"TEST-ZA-{token}", same_ring, "unchanged")
            )
            changed = service.load_postgres_buffer(
                self._scheduled_buffer_context(service, root, f"TEST-ZA-{token}", changed_ring, "changed")
            )

        with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id_buffer_abrangencia, numero_versao, fl_ativa
                FROM buffer_abrangencia
                WHERE id_uc = %s
                ORDER BY numero_versao
                """,
                (id_uc,),
            )
            versions = cursor.fetchall()

        self.assertEqual(unchanged["inserted_records"], 0)
        self.assertEqual(unchanged["reason"], "active_buffer_unchanged")
        self.assertEqual(changed["inserted_records"], 1)
        self.assertEqual([row[1:] for row in versions], [(1, False), (2, True)])
        self.assertEqual(versions[0][0], id_buffer_abrangencia)

    @staticmethod
    def _scheduled_buffer_context(
        service: ZaBufferPipelineService,
        root: Path,
        uc_identifier: str,
        ring: MultiPolygon,
        label: str,
    ) -> TaskExecutionContext:
        context = TaskExecutionContext(
            dag_id="DAG_ZA_BUFFER",
            stage="load_postgres_buffer",
            logical_date=datetime.now(timezone.utc).isoformat(),
            run_id=f"scheduled__{label}-{uuid4().hex}",
            conf=None,
        )
        transformed_path = root / f"buffer_{label}.gpkg"
        gpd.GeoDataFrame(
            {"uc_id_source": [uc_identifier], "ds_fonte": ["fixture de integração"]},
            geometry=[ring],
            crs="EPSG:4674",
        ).rename_geometry("geom").to_file(transformed_path, layer="buffer_abrangencia_stage", driver="GPKG")
        service._save_state(
            context,
            branch="buffer",
            state={
                "available": True,
                "transformed_path": str(transformed_path),
                "dt_bronze": datetime.now(timezone.utc).date().isoformat(),
            },
        )
        return context

    @staticmethod
    def _create_uc_with_active_buffer(database_url: str, token: str) -> tuple[int, int]:
        polygon = MultiPolygon(
            [Polygon([(-49.2, -27.2), (-49.0, -27.2), (-49.0, -27.0), (-49.2, -27.0)])]
        )
        with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO uc (uc_id, nm_uc, sg_uf, geom, dt_bronze, versao_dag)
                VALUES (%s, %s, 'SC', ST_SetSRID(ST_GeomFromText(%s), 4674), CURRENT_DATE, 'TEST')
                RETURNING id_uc;
                """,
                (f"TEST-ZA-{token}", f"UC teste ZA {token}", polygon.wkt),
            )
            id_uc = int(cursor.fetchone()[0])
            cursor.execute(
                """
                INSERT INTO buffer_abrangencia (
                    id_uc, ds_fonte, dist_buffer_m, geom, numero_versao,
                    motivo, ator, correlation_id
                ) VALUES (
                    %s, 'teste', 3000, ST_SetSRID(ST_GeomFromText(%s), 4674), 1,
                    'Buffer de Abrangência inicial de teste', 'integration-test', %s
                ) RETURNING id_buffer_abrangencia;
                """,
                (id_uc, polygon.wkt, f"corr-{token}"),
            )
            id_buffer_abrangencia = int(cursor.fetchone()[0])
        return id_uc, id_buffer_abrangencia

    @staticmethod
    def _config(database_url: str, root: Path) -> PipelineConfig:
        return PipelineConfig(
            project_db_url=database_url,
            mutation_db_url=database_url,
            medallion_bronze_path=str(root / "bronze"),
            medallion_silver_path=str(root / "silver"),
            medallion_gold_path=str(root / "gold"),
            medallion_tmp_path=str(root / "tmp"),
        )

    @staticmethod
    def _context(import_id: str) -> TaskExecutionContext:
        return TaskExecutionContext(
            dag_id="DAG_ZA_BUFFER",
            stage="load_postgres_za",
            logical_date=datetime.now(timezone.utc).isoformat(),
            run_id=f"api__{import_id}",
            conf=None,
        )

    @staticmethod
    def _state(import_id: str, idempotency_key: str, uc_identifier: str) -> dict:
        return {
            "import_id": import_id,
            "dt_bronze": datetime.now(timezone.utc).isoformat(),
            "manifest": {
                "import_id": import_id,
                "operation": "replace_buffer_abrangencia",
                "correlation_id": f"corr-{import_id}",
                "idempotency_key": idempotency_key,
                "actor": "integration-test",
                "metadata": {
                    "source": "fixture de integração",
                    "uc_identifier": uc_identifier,
                    "reason": "Publicação de ZA oficial em teste",
                },
            },
        }

    @staticmethod
    def _frame(id_uc: int, geometry) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(
            {"id_uc": [id_uc], "ds_fonte": ["fixture de integração"]},
            geometry=[geometry],
            crs="EPSG:4674",
        ).rename_geometry("geom")


if __name__ == "__main__":
    unittest.main()
