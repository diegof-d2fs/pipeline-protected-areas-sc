"""Retention cleanup deletes only old dated batches; permanent partitions are never touched."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from dag_cleanup_medallion_retention import _cleanup_layer, _parse_batch_datetime

CUTOFF = datetime(2026, 10, 10, tzinfo=timezone.utc)


def _dir(root: Path, relative: str, *, old_mtime: bool = False) -> Path:
    path = root / relative
    path.mkdir(parents=True)
    (path / "data.bin").write_bytes(b"x")
    if old_mtime:
        stamp = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
        os.utime(path, (stamp, stamp))
    return path


def test_only_dated_names_are_batches() -> None:
    assert _parse_batch_datetime(Path("2026-10-04-06-30-00")) == datetime(2026, 10, 4, 6, 30, tzinfo=timezone.utc)
    for name in ("collection=11", "product=VIIRS_NOAA20_NRT", "statistics", "aoi_snapshot", "quality"):
        assert _parse_batch_datetime(Path(name)) is None


def test_permanent_partitions_survive_even_with_old_mtime(tmp_path: Path) -> None:
    """Regression of Nov/2026: the mtime fallback deleted the published COGs and FIRMS partitions."""
    cog = _dir(tmp_path, "mapbiomas_lulc/collection=11/version=1/year=1985", old_mtime=True)
    os.utime(tmp_path / "mapbiomas_lulc/collection=11", (0, 0))
    _dir(tmp_path, "mapbiomas_lulc/statistics", old_mtime=True)
    _dir(tmp_path, "firms/product=VIIRS_NOAA20_NRT", old_mtime=True)

    stats = _cleanup_layer(tmp_path, CUTOFF)

    assert cog.is_dir()
    assert (tmp_path / "mapbiomas_lulc/statistics").is_dir()
    assert (tmp_path / "firms/product=VIIRS_NOAA20_NRT").is_dir()
    assert stats == {"deleted_dirs": 0, "kept_dirs": 0, "skipped_dirs": 3}


def test_old_batches_go_recent_and_newest_stay(tmp_path: Path) -> None:
    old = _dir(tmp_path, "ucs/2026-09-20-06-00-00")
    older = _dir(tmp_path, "ucs/2026-09-13-06-00-00")
    recent = _dir(tmp_path, "ucs/2026-11-01-06-00-00")

    stats = _cleanup_layer(tmp_path, CUTOFF)

    assert not old.exists() and not older.exists()
    assert recent.is_dir()
    assert stats == {"deleted_dirs": 2, "kept_dirs": 1, "skipped_dirs": 0}


def test_newest_batch_is_kept_even_when_older_than_cutoff(tmp_path: Path) -> None:
    """A domain loaded only yearly (PRODES) must keep its current batch for GeoServer and the API."""
    stale = _dir(tmp_path, "prodes/2026-08-01-06-00-00")
    current = _dir(tmp_path, "prodes/2026-09-01-06-00-00")

    stats = _cleanup_layer(tmp_path, CUTOFF)

    assert not stale.exists()
    assert current.is_dir()
    assert stats["deleted_dirs"] == 1 and stats["kept_dirs"] == 1


def test_missing_layer_root_is_a_noop(tmp_path: Path) -> None:
    assert _cleanup_layer(tmp_path / "nao-existe", CUTOFF) == {"deleted_dirs": 0, "kept_dirs": 0, "skipped_dirs": 0}
