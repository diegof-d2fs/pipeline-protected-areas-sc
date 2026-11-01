from __future__ import annotations

import unittest

import pandas as pd

from scripts_python import mapbiomas_alerta_pipeline, prodes_pipeline


class RejectionTotalsTest(unittest.TestCase):
    """The quality report must count every rejected row, not the capped sample."""

    def _assert_total_exceeds_sample(self, module, service_cls, reason: str) -> None:
        service = object.__new__(service_cls)
        limit = module.REJECTION_SAMPLE_LIMIT
        rejected_frames: dict[str, pd.DataFrame] = {}
        rejection_totals: dict[str, int] = {}

        for _ in range(3):
            chunk = pd.DataFrame({"id": range(limit)})
            service._record_rejection(rejected_frames, rejection_totals, reason, chunk)

        self.assertEqual(rejection_totals[reason], 3 * limit)
        self.assertEqual(len(rejected_frames[reason]), limit)

    def test_prodes_duplicate_total_is_not_capped(self) -> None:
        self._assert_total_exceeds_sample(
            prodes_pipeline, prodes_pipeline.ProdesPipelineService, "DUPLICATE_PRODES_ID_IN_DB"
        )

    def test_alerta_duplicate_total_is_not_capped(self) -> None:
        self._assert_total_exceeds_sample(
            mapbiomas_alerta_pipeline,
            mapbiomas_alerta_pipeline.MapbiomasAlertaPipelineService,
            "DUPLICATE_ALERT_CODE_IN_DB",
        )


if __name__ == "__main__":
    unittest.main()
