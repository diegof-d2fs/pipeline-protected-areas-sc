from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest

from scripts_python.config import PipelineConfig
from scripts_python.object_storage import MedallionStore, ObjectStorageError


class FakeS3:
    """In-memory S3 with ETags and paginated listing, enough for the sync contract."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.uploads: list[str] = []
        self.downloads: list[str] = []

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.objects[(bucket, key)] = Path(filename).read_bytes()
        self.uploads.append(key)

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        Path(filename).write_bytes(self.objects[(bucket, key)])
        self.downloads.append(key)

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        return {"ETag": self._etag(self.objects[(Bucket, Key)])}

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket: str, Prefix: str):
        contents = [
            {"Key": key, "Size": len(body), "ETag": self._etag(body)}
            for (bucket, key), body in sorted(self.objects.items())
            if bucket == Bucket and key.startswith(Prefix)
        ]
        yield {"Contents": contents[:2]}
        yield {"Contents": contents[2:]}

    @staticmethod
    def _etag(body: bytes) -> str:
        return f'"{hashlib.md5(body).hexdigest()}"'


def _store(root: Path, s3: FakeS3) -> MedallionStore:
    config = PipelineConfig(
        project_db_url="",
        mutation_db_url="",
        medallion_bronze_path=str(root / "bronze"),
        medallion_silver_path=str(root / "silver"),
        medallion_gold_path=str(root / "gold"),
        medallion_tmp_path=str(root / "tmp"),
        s3_bronze_bucket="bronze",
        s3_lake_bucket="lake",
    )
    return MedallionStore.from_config(config, client=s3)


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_store_is_disabled_without_buckets(tmp_path: Path) -> None:
    config = PipelineConfig("", "", str(tmp_path), str(tmp_path), str(tmp_path), str(tmp_path))
    assert MedallionStore.from_config(config, client=FakeS3()) is None


def test_push_maps_layers_to_buckets_and_only_sends_changes(tmp_path: Path) -> None:
    s3 = FakeS3()
    store = _store(tmp_path, s3)
    _write(tmp_path / "bronze/ucs/import_id=1/manifest.json", "{}")
    gold = _write(tmp_path / "gold/prodes/a.geojson", "v1")
    _write(tmp_path / "quality/api_results/import_id=1/result.json", "{}")
    _write(tmp_path / "gold/.staging/ignorar", "x")

    assert store.push() == {"bronze": 1, "silver": 0, "gold": 1, "quality": 1}
    assert ("bronze", "ucs/import_id=1/manifest.json") in s3.objects
    assert ("lake", "gold/prodes/a.geojson") in s3.objects
    assert ("lake", "quality/api_results/import_id=1/result.json") in s3.objects
    assert not any(".staging" in key for _, key in s3.objects)

    assert store.push() == {"bronze": 0, "silver": 0, "gold": 0, "quality": 0}

    gold.write_text("v2-maior", encoding="utf-8")
    assert store.push(("gold",)) == {"gold": 1}
    assert s3.objects[("lake", "gold/prodes/a.geojson")] == b"v2-maior"


def test_bronze_is_immutable_in_both_directions(tmp_path: Path) -> None:
    s3 = FakeS3()
    s3.objects[("bronze", "ucs/import_id=1/manifest.json")] = b"remoto"
    store = _store(tmp_path, s3)
    _write(tmp_path / "bronze/ucs/import_id=1/manifest.json", "local-diferente")

    with pytest.raises(ObjectStorageError, match="immutable"):
        store.push(("bronze",))
    with pytest.raises(ObjectStorageError, match="differs"):
        store.pull(("bronze",))


def test_pull_restores_a_fresh_cache_and_is_incremental(tmp_path: Path) -> None:
    s3 = FakeS3()
    source = tmp_path / "origem"
    _store(source, s3)
    _write(source / "bronze/firms/x.csv", "a")
    _write(source / "bronze/ucs/import_id=2/manifest.json", "{}")
    _store(source, s3).push(("bronze",))
    shutil.rmtree(source)

    store = _store(tmp_path / "novo", s3)
    assert store.pull(("bronze",)) == {"bronze": 2}
    assert (tmp_path / "novo/bronze/ucs/import_id=2/manifest.json").read_text() == "{}"
    assert store.pull(("bronze",)) == {"bronze": 0}


def test_pull_by_prefix_fetches_only_the_requested_batch(tmp_path: Path) -> None:
    s3 = FakeS3()
    s3.objects[("bronze", "ucs/import_id=1/manifest.json")] = b"{}"
    s3.objects[("bronze", "ucs/import_id=1/canonical/data.geojson")] = b"{}"
    s3.objects[("bronze", "ucs/import_id=2/manifest.json")] = b"{}"

    _store(tmp_path, s3).pull(("bronze",), key_prefix="ucs/import_id=1/")

    assert sorted(s3.downloads) == ["ucs/import_id=1/canonical/data.geojson", "ucs/import_id=1/manifest.json"]


def test_existing_local_copy_is_adopted_without_transfer(tmp_path: Path) -> None:
    s3 = FakeS3()
    s3.objects[("lake", "gold/a.txt")] = b"mesmo"
    _write(tmp_path / "gold/a.txt", "mesmo")
    store = _store(tmp_path, s3)

    assert store.pull(("gold",)) == {"gold": 0}
    assert store.push(("gold",)) == {"gold": 0}
    assert s3.uploads == [] and s3.downloads == []


def test_unsafe_remote_key_is_rejected(tmp_path: Path) -> None:
    s3 = FakeS3()
    s3.objects[("lake", "gold/../fora.txt")] = b"x"

    with pytest.raises(ObjectStorageError, match="Unsafe"):
        _store(tmp_path, s3).pull(("gold",))
