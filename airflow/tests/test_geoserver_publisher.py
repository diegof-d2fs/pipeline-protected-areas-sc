from __future__ import annotations

import sys
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

GEOSERVER_DIR = Path("/opt/airflow/geoserver")


@unittest.skipUnless(GEOSERVER_DIR.is_dir(), "diretório geoserver não montado")
class GeoServerPublisherTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        sys.path.insert(0, str(GEOSERVER_DIR))
        import bootstrap_layers

        cls.module = bootstrap_layers

    def test_raster_style_carries_every_official_qml_class(self) -> None:
        qml_entries = ET.parse(self.module.RASTER_QML).getroot().findall(".//colorPalette/paletteEntry")
        sld = ET.fromstring(self.module.raster_sld_from_qml(self.module.RASTER_QML))
        entries = sld.findall(".//{http://www.opengis.net/sld}ColorMapEntry")
        quantities = {int(entry.get("quantity")) for entry in entries}
        self.assertEqual(len(qml_entries), 33)
        self.assertEqual(quantities, {0} | {int(entry.get("value")) for entry in qml_entries})

    def test_vector_styles_are_valid_sld(self) -> None:
        for layer in self.module.VECTOR_LAYERS:
            with self.subTest(layer=layer.name):
                ET.fromstring(self.module.vector_sld(layer))

    def test_sync_dag_imports_publisher(self) -> None:
        from airflow.models import DagBag

        bag = DagBag(str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
        self.assertEqual(bag.import_errors, {})
        dag = bag.get_dag("DAG_GEOSERVER_SYNC")
        self.assertEqual(sorted(task.task_id for task in dag.tasks), ["push_medallion", "sync_geoserver"])
        self.assertEqual(
            {dag_id for dag_id, candidate in bag.dags.items() if any(t.outlets for t in candidate.tasks)},
            {"DAG_UCS", "DAG_UC_ZA", "DAG_ZA_BUFFER", "DAG_PRODES", "DAG_MAPBIOMAS_ALERTA",
             "DAG_MAPBIOMAS", "DAG_FIRMS", "DAG_FIRMS_BACKFILL"},
        )
