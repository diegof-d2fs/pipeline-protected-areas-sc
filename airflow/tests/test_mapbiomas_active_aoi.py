"""Consulta das áreas vigentes da execução completa do MapBiomas contra um PostGIS real.

A transação é desfeita no fim: o banco de teste é compartilhado com outros testes, por isso só
as UCs criadas aqui são conferidas.
"""

from __future__ import annotations

import os
import unittest
import uuid

import psycopg2

from scripts_python.mapbiomas_pipeline import MapbiomasPipelineService

SQUARE = "MULTIPOLYGON(((-49.2 -27.2,-49.0 -27.2,-49.0 -27.0,-49.2 -27.0,-49.2 -27.2)))"
RING = "MULTIPOLYGON(((-49.3 -27.3,-48.9 -27.3,-48.9 -26.9,-49.3 -26.9,-49.3 -27.3)))"


@unittest.skipUnless(os.getenv("MUTATION_TEST_DB_URL"), "MUTATION_TEST_DB_URL não configurada")
class ActiveAoiQueryTest(unittest.TestCase):
    def test_each_active_uc_has_its_single_active_zone(self) -> None:
        token = uuid.uuid4().hex[:8]
        with psycopg2.connect(os.environ["MUTATION_TEST_DB_URL"]) as connection:
            try:
                with connection.cursor() as cursor:
                    ids = {}
                    for label in ("buffer", "za", "extinta"):
                        cursor.execute(
                            "INSERT INTO uc (uc_id, nm_uc, sg_uf, geom, dt_bronze, versao_dag) "
                            "VALUES (%s, %s, 'SC', ST_SetSRID(ST_GeomFromText(%s), 4674), CURRENT_DATE, 'TEST') "
                            "RETURNING id_uc;",
                            (f"TEST-AOI-{label}-{token}", f"UC {label} {token}", SQUARE),
                        )
                        ids[label] = cursor.fetchone()[0]
                    cursor.execute("UPDATE uc SET situacao = 'EXTINTA' WHERE id_uc = %s;", (ids["extinta"],))
                    cursor.execute(
                        "INSERT INTO buffer_abrangencia (id_uc, ds_fonte, dist_buffer_m, geom, numero_versao, motivo, ator, correlation_id) "
                        "VALUES (%s, 'teste', 3000, ST_SetSRID(ST_GeomFromText(%s), 4674), 1, 'teste', 'integration-test', %s);",
                        (ids["buffer"], RING, f"corr-{token}"),
                    )
                    # UC that moved to an official ZA keeps its closed buffer in the history.
                    cursor.execute(
                        "INSERT INTO buffer_abrangencia (id_uc, ds_fonte, dist_buffer_m, geom, numero_versao, fl_ativa, motivo, ator, correlation_id) "
                        "VALUES (%s, 'teste', 3000, ST_SetSRID(ST_GeomFromText(%s), 4674), 1, false, 'teste', 'integration-test', %s);",
                        (ids["za"], RING, f"corr-za-{token}"),
                    )
                    cursor.execute(
                        "INSERT INTO za_oficial (id_uc, ds_fonte, geom) VALUES (%s, 'teste', ST_SetSRID(ST_GeomFromText(%s), 4674)) "
                        "RETURNING id_za_oficial;",
                        (ids["za"], RING),
                    )
                    id_za = cursor.fetchone()[0]
                    cursor.execute(MapbiomasPipelineService.ACTIVE_AOI_SQL)
                    rows = {row[0]: row for row in cursor.fetchall() if row[0] in ids.values()}
                self.assertNotIn(ids["extinta"], rows)
                self.assertEqual(rows[ids["buffer"]][2], "BUFFER_ABRANGENCIA")
                self.assertEqual(rows[ids["buffer"]][5], 1)
                self.assertEqual(rows[ids["za"]][2:4], ("ZA", id_za))
                self.assertEqual(rows[ids["za"]][5], 1)
            finally:
                connection.rollback()
