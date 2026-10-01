from __future__ import annotations

import os
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import geopandas as gpd
import psycopg2
from shapely.geometry import MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry

from scripts_python.config import PipelineConfig
from scripts_python.domain_pipeline import TaskExecutionContext
from scripts_python.ucs_pipeline import InputValidationError, UcsPipelineService


class VersionedUcMutationIntegrationTest(unittest.TestCase):
    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_create_point_preserves_uc_geometry_and_initial_history(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        official_identifier = f"TEST-POINT-{token}"
        import_id = str(uuid4())

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = PipelineConfig(
                project_db_url=database_url,
                mutation_db_url=database_url,
                medallion_bronze_path=str(root / "bronze"),
                medallion_silver_path=str(root / "silver"),
                medallion_gold_path=str(root / "gold"),
                medallion_tmp_path=str(root / "tmp"),
            )
            service = UcsPipelineService(config)
            created_at = datetime.now(timezone.utc).isoformat()
            state = {
                "import_id": import_id,
                "manifest": {
                    "import_id": import_id,
                    "operation": "create",
                    "correlation_id": f"corr-point-{token}",
                    "idempotency_key": f"idem-point-{token}",
                    "actor": "integration-test",
                    "metadata": {
                        "source": "teste pontual de integração",
                        "official_identifier": official_identifier,
                    },
                },
            }
            frame = self._frame(
                official_identifier,
                "UC pontual versionada",
                Point(-49.1, -27.1),
                update_geom="FALSE",
            )

            result = service._load_directed_postgres(
                context=self._context(import_id, created_at),
                state=state,
                transformed=frame,
                source_records=1,
                skipped_no_uc_id=0,
                rejected_frames={},
            )

            with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT u.id_uc, u.versao_registro, ST_GeometryType(u.geom),
                           v.numero_versao, v.fl_ativa, ST_GeometryType(v.geom), e.event_type
                    FROM uc u
                    JOIN uc_geometry_version v ON v.id_uc = u.id_uc
                    JOIN cadastral_event e ON e.entity_id = u.id_uc AND e.entity_type = 'UC'
                    WHERE u.uc_id = %s
                    """,
                    (official_identifier,),
                )
                persisted = cursor.fetchone()

            self.assertEqual(result["inserted_new_records"], 1)
            self.assertEqual(persisted[1:], (1, "ST_Point", 1, True, "ST_Point", "UC_CREATED"))

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_create_update_and_replay_preserve_uc_identity_and_history(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        official_identifier = f"TEST-VERSIONED-{token}"

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = PipelineConfig(
                project_db_url=database_url,
                mutation_db_url=database_url,
                medallion_bronze_path=str(root / "bronze"),
                medallion_silver_path=str(root / "silver"),
                medallion_gold_path=str(root / "gold"),
                medallion_tmp_path=str(root / "tmp"),
            )
            service = UcsPipelineService(config)
            created_at = datetime.now(timezone.utc).isoformat()
            create_import = str(uuid4())
            create_state = {
                "import_id": create_import,
                "manifest": {
                    "import_id": create_import,
                    "operation": "create",
                    "correlation_id": f"corr-create-{token}",
                    "idempotency_key": f"idem-create-{token}",
                    "actor": "integration-test",
                    "metadata": {
                        "source": "teste de integração",
                        "reason": "cadastro inicial",
                        "official_identifier": official_identifier,
                    },
                },
            }
            create_context = self._context(create_import, created_at)
            first_geometry = MultiPolygon(
                [Polygon([(-49.2, -27.2), (-49.0, -27.2), (-49.0, -27.0), (-49.2, -27.0)])]
            )
            create_frame = self._frame(official_identifier, "UC versionada", first_geometry)

            create_result = service._load_directed_postgres(
                context=create_context,
                state=create_state,
                transformed=create_frame,
                source_records=1,
                skipped_no_uc_id=0,
                rejected_frames={},
            )

            update_import = str(uuid4())
            update_state = {
                "import_id": update_import,
                "manifest": {
                    "import_id": update_import,
                    "operation": "update",
                    "correlation_id": f"corr-update-{token}",
                    "idempotency_key": f"idem-update-{token}",
                    "actor": "integration-test",
                    "metadata": {
                        "source": "teste de integração",
                        "reason": "correção do limite",
                        "official_identifier": official_identifier,
                        "expected_version": 1,
                    },
                },
            }
            update_context = self._context(update_import, created_at)
            second_geometry = MultiPolygon(
                [Polygon([(-49.2, -27.2), (-48.9, -27.2), (-48.9, -26.9), (-49.2, -26.9)])]
            )
            update_frame = self._frame(official_identifier, "UC versionada atualizada", second_geometry)

            update_result = service._load_directed_postgres(
                context=update_context,
                state=update_state,
                transformed=update_frame,
                source_records=1,
                skipped_no_uc_id=0,
                rejected_frames={},
            )
            replay_result = service._load_directed_postgres(
                context=update_context,
                state=update_state,
                transformed=update_frame,
                source_records=1,
                skipped_no_uc_id=0,
                rejected_frames={},
            )

            with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id_uc, versao_registro, nm_uc FROM uc WHERE uc_id = %s",
                    (official_identifier,),
                )
                current = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT numero_versao, fl_ativa
                    FROM uc_geometry_version
                    WHERE id_uc = %s ORDER BY numero_versao
                    """,
                    (current[0],),
                )
                versions = cursor.fetchall()
                cursor.execute(
                    "SELECT event_type FROM cadastral_event WHERE entity_id = %s ORDER BY occurred_at",
                    (current[0],),
                )
                events = [row[0] for row in cursor.fetchall()]

            self.assertEqual(create_result["inserted_new_records"], 1)
            self.assertEqual(update_result["inserted_updated_records"], 1)
            self.assertTrue(replay_result["idempotent_replay"])
            self.assertEqual(current[1:], (2, "UC versionada atualizada"))
            self.assertEqual(versions, [(1, False), (2, True)])
            self.assertEqual(events, ["UC_CREATED", "UC_UPDATED"])

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_replace_point_with_polygon_is_versioned_specific_and_idempotent(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        official_identifier = f"TEST-RP-{token}"

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = PipelineConfig(
                project_db_url=database_url,
                mutation_db_url=database_url,
                medallion_bronze_path=str(root / "bronze"),
                medallion_silver_path=str(root / "silver"),
                medallion_gold_path=str(root / "gold"),
                medallion_tmp_path=str(root / "tmp"),
            )
            service = UcsPipelineService(config)
            created_at = datetime.now(timezone.utc).isoformat()
            create_import = str(uuid4())
            create_state = {
                "import_id": create_import,
                "manifest": {
                    "import_id": create_import,
                    "operation": "create",
                    "correlation_id": f"corr-create-point-{token}",
                    "idempotency_key": f"idem-create-point-{token}",
                    "actor": "integration-test",
                    "metadata": {
                        "source": "teste replace_point",
                        "official_identifier": official_identifier,
                    },
                },
            }
            service._load_directed_postgres(
                context=self._context(create_import, created_at),
                state=create_state,
                transformed=self._frame(
                    official_identifier,
                    "UC pontual para substituição",
                    Point(-49.1, -27.1),
                    update_geom="FALSE",
                ),
                source_records=1,
                skipped_no_uc_id=0,
                rejected_frames={},
            )

            replace_import = str(uuid4())
            replace_state = {
                "import_id": replace_import,
                "manifest": {
                    "import_id": replace_import,
                    "operation": "replace_point",
                    "correlation_id": f"corr-replace-point-{token}",
                    "idempotency_key": f"idem-replace-point-{token}",
                    "actor": "integration-test",
                    "metadata": {
                        "source": "limite oficial",
                        "reason": "substituição do ponto pelo polígono oficial",
                        "official_identifier": official_identifier,
                        "expected_version": 1,
                    },
                },
            }
            polygon = MultiPolygon(
                [Polygon([(-49.2, -27.2), (-49.0, -27.2), (-49.0, -27.0), (-49.2, -27.0)])]
            )
            replace_context = self._context(replace_import, created_at)
            replace_frame = self._frame(official_identifier, "UC agora poligonal", polygon)

            result = service._load_directed_postgres(
                context=replace_context,
                state=replace_state,
                transformed=replace_frame,
                source_records=1,
                skipped_no_uc_id=0,
                rejected_frames={},
            )
            replay = service._load_directed_postgres(
                context=replace_context,
                state=replace_state,
                transformed=replace_frame,
                source_records=1,
                skipped_no_uc_id=0,
                rejected_frames={},
            )

            second_replace_state = {
                **replace_state,
                "import_id": str(uuid4()),
                "manifest": {
                    **replace_state["manifest"],
                    "import_id": str(uuid4()),
                    "idempotency_key": f"idem-replace-point-again-{token}",
                    "expected_version": 2,
                    "metadata": {**replace_state["manifest"]["metadata"], "expected_version": 2},
                },
            }
            with self.assertRaisesRegex(InputValidationError, "UC_REPLACE_POINT_TARGET_NOT_POINT"):
                service._load_directed_postgres(
                    context=self._context(second_replace_state["import_id"], created_at),
                    state=second_replace_state,
                    transformed=replace_frame,
                    source_records=1,
                    skipped_no_uc_id=0,
                    rejected_frames={},
                )

            with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id_uc, versao_registro, ST_GeometryType(geom) FROM uc WHERE uc_id = %s",
                    (official_identifier,),
                )
                current = cursor.fetchone()
                cursor.execute(
                    "SELECT numero_versao, fl_ativa, ST_GeometryType(geom) "
                    "FROM uc_geometry_version WHERE id_uc = %s ORDER BY numero_versao",
                    (current[0],),
                )
                versions = cursor.fetchall()
                cursor.execute(
                    "SELECT event_type FROM cadastral_event WHERE entity_id = %s ORDER BY occurred_at",
                    (current[0],),
                )
                events = [row[0] for row in cursor.fetchall()]

            self.assertEqual(result["inserted_updated_records"], 1)
            self.assertTrue(replay["idempotent_replay"])
            self.assertEqual(current[1:], (2, "ST_MultiPolygon"))
            self.assertEqual(
                versions,
                [(1, False, "ST_Point"), (2, True, "ST_MultiPolygon")],
            )
            self.assertEqual(events, ["UC_CREATED", "UC_POINT_REPLACED_BY_POLYGON"])
            extinguish_id = str(uuid4())
            extinguish_state = {
                "manifest": {
                    "import_id": extinguish_id,
                    "operation": "extinguish",
                    "idempotency_key": f"extinguish-{token}",
                    "actor": "integration-test",
                    "metadata": {
                        "official_identifier": official_identifier,
                        "expected_version": 2, "reason": "Extinção de teste", "source": "Teste",
                    },
                }
            }
            extinct_context = self._context(extinguish_id, created_at)
            service._load_directed_extinguish(extinct_context, extinguish_state)
            extinct_replay = service._load_directed_extinguish(extinct_context, extinguish_state)
            self.assertTrue(extinct_replay["idempotent_replay"])
            with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
                cursor.execute("SELECT situacao, versao_registro FROM uc WHERE id_uc = %s", (current[0],))
                self.assertEqual(cursor.fetchone(), ("EXTINTA", 3))
                cursor.execute(
                    "SELECT count(*), count(*) FILTER (WHERE fl_ativa) FROM uc_geometry_version WHERE id_uc = %s",
                    (current[0],),
                )
                self.assertEqual(cursor.fetchone(), (2, 0))
                cursor.execute(
                    "SELECT count(*) FROM cadastral_event WHERE entity_id = %s AND event_type = 'UC_EXTINGUISHED'",
                    (current[0],),
                )
                self.assertEqual(cursor.fetchone()[0], 1)

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_scheduled_load_records_geometry_history_and_events_idempotently(self) -> None:
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        uc_id = f"SCHEDULED-{uuid4().hex}"
        first_run = f"scheduled__{uuid4().hex}"
        second_run = f"scheduled__{uuid4().hex}"
        root = Path(tempfile.gettempdir()) / f"scheduled-{uuid4().hex}"
        service = UcsPipelineService(
            PipelineConfig(
                project_db_url=database_url,
                mutation_db_url=database_url,
                medallion_bronze_path=str(root / "bronze"),
                medallion_silver_path=str(root / "silver"),
                medallion_gold_path=str(root / "gold"),
                medallion_tmp_path=str(root / "tmp"),
            )
        )

        def context(run_id: str) -> TaskExecutionContext:
            return TaskExecutionContext(
                dag_id="DAG_UCS",
                stage="load_postgres",
                logical_date=datetime.now(timezone.utc).isoformat(),
                run_id=run_id,
                conf=None,
            )

        def history(cursor) -> tuple[list[tuple], list[str]]:
            cursor.execute(
                """
                SELECT v.numero_versao, v.fl_ativa
                FROM uc_geometry_version v JOIN uc u ON u.id_uc = v.id_uc
                WHERE u.uc_id = %s ORDER BY v.numero_versao;
                """,
                (uc_id,),
            )
            versions = cursor.fetchall()
            cursor.execute(
                """
                SELECT e.event_type
                FROM cadastral_event e JOIN uc u ON u.id_uc = e.entity_id AND e.entity_type = 'UC'
                WHERE u.uc_id = %s ORDER BY e.occurred_at, e.event_type;
                """,
                (uc_id,),
            )
            return versions, [row[0] for row in cursor.fetchall()]

        with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO uc (uc_id, nm_uc, sg_uf, geom, dt_bronze)
                VALUES (%s, 'UC carga agendada', 'SC',
                        ST_SetSRID(ST_GeomFromText('POINT(-49.2 -27.2)'), 4674), CURRENT_DATE);
                """,
                (uc_id,),
            )
            service._record_scheduled_geometry_versions(cursor, context(first_run), [uc_id])
            service._record_scheduled_geometry_versions(cursor, context(first_run), [uc_id])
            self.assertEqual(history(cursor), ([(1, True)], ["UC_CREATED"]))

            cursor.execute(
                """
                UPDATE uc
                SET geom = ST_SetSRID(ST_GeomFromText(
                        'MULTIPOLYGON(((-49.21 -27.21,-49.19 -27.21,-49.19 -27.19,-49.21 -27.19,-49.21 -27.21)))'
                    ), 4674),
                    versao_registro = versao_registro + 1
                WHERE uc_id = %s;
                """,
                (uc_id,),
            )
            service._record_scheduled_geometry_versions(cursor, context(second_run), [uc_id])
            service._record_scheduled_geometry_versions(cursor, context(second_run), [uc_id])
            self.assertEqual(history(cursor), ([(1, False), (2, True)], ["UC_CREATED", "UC_UPDATED"]))

    @staticmethod
    def _context(import_id: str, logical_date: str) -> TaskExecutionContext:
        return TaskExecutionContext(
            dag_id="DAG_UCS",
            stage="load_postgres",
            logical_date=logical_date,
            run_id=f"api__{import_id}",
            conf=None,
        )

    @unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
    def test_uc_zone_batch_rolls_back_and_replays_atomically(self):
        database_url = os.environ["MUTATION_TEST_DB_URL"]
        token = uuid4().hex
        identity = f"BATCH-{token}"
        import_id = str(uuid4())
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bronze = root / "bronze"
            bronze.mkdir()
            zone_path = bronze / "zone.geojson"
            zone_path.write_text(json.dumps({"features": [{"geometry": {
                "type": "Point", "coordinates": [-49.1, -27.1]
            }}]}), encoding="utf-8")
            service = UcsPipelineService(PipelineConfig(
                project_db_url=database_url, mutation_db_url=database_url,
                medallion_bronze_path=str(bronze), medallion_silver_path=str(root / "silver"),
                medallion_gold_path=str(root / "gold"), medallion_tmp_path=str(root / "tmp"),
            ))
            state = {"manifest": {
                "operation": "create_with_zone", "import_id": import_id,
                "idempotency_key": f"batch-{token}", "actor": "integration-test",
                "metadata": {"official_identifier": identity, "source": "Test",
                             "official_zone": {"canonical_key": "zone.geojson",
                                               "metadata": {"source": "Official test"}}},
            }}
            polygon = MultiPolygon([Polygon([(-49.2, -27.2), (-49.0, -27.2), (-49.0, -27.0), (-49.2, -27.0)])])
            args = dict(context=self._context(import_id, datetime.now(timezone.utc).isoformat()),
                        state=state, transformed=self._frame(identity, "Atomic UC", polygon),
                        source_records=1, skipped_no_uc_id=0, rejected_frames={})
            with self.assertRaisesRegex(InputValidationError, "Invalid official zone"):
                service._load_directed_postgres(**args)
            with psycopg2.connect(database_url) as conn, conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM uc WHERE uc_id = %s", (identity,))
                self.assertEqual(cur.fetchone()[0], 0)
            zone_path.write_text(json.dumps({"features": [{"geometry": polygon.__geo_interface__}]}), encoding="utf-8")
            service._load_directed_postgres(**args)
            self.assertTrue(service._load_directed_postgres(**args)["idempotent_replay"])
            with psycopg2.connect(database_url) as conn, conn.cursor() as cur:
                cur.execute(
                    """SELECT u.id_uc, count(z.id_za_oficial) FROM uc u
                       JOIN za_oficial z ON z.id_uc=u.id_uc WHERE u.uc_id=%s GROUP BY u.id_uc""",
                    (identity,),
                )
                uc_id, count = cur.fetchone()
                self.assertEqual(count, 1)
                cur.execute("SELECT count(*) FROM buffer_abrangencia WHERE id_uc=%s", (uc_id,))
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("SELECT event_type FROM cadastral_event WHERE entity_id=%s AND entity_type='UC'", (uc_id,))
                self.assertEqual(cur.fetchall(), [("UC_CREATED_WITH_OFFICIAL_ZA",)])

    @staticmethod
    def _frame(
        official_identifier: str,
        name: str,
        geometry: BaseGeometry,
        *,
        update_geom: str = "TRUE",
    ) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(
            {
                "uc_id": [official_identifier],
                "nm_uc": [name],
                "sg_uf": ["SC"],
                "update_geom": [update_geom],
                "dt_bronze": [datetime.now(timezone.utc).date()],
                "versao_dag": ["DAG_UCS"],
            },
            geometry=[geometry],
            crs="EPSG:4674",
        ).rename_geometry("geom")


if __name__ == "__main__":
    unittest.main()
