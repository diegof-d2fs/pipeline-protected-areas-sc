"""Mirror of the local Medallion with S3, the source of truth when the pipeline runs on AWS.

The processing node keeps the Medallion on local disk (every reader and writer stays file
based) and treats it as a cache: `pull` before work, `push` after loads and before shutdown.
A per-layer sync state records size, mtime and ETag of each object last exchanged, so a sync
only transfers what changed on either side. Bronze is immutable: an existing object with a
different size is a conflict, never an overwrite.

Layout: the Bronze bucket mirrors `bronze/` key by key; the lake bucket holds `silver/`,
`gold/` and `quality/` (where the API reads pipeline results).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from scripts_python.config import PipelineConfig

STATE_FILE = ".s3_sync_state.json"
LOGGER = logging.getLogger(__name__)


class ObjectStorageError(RuntimeError):
    """Raised when local and remote Medallion copies disagree in a way that cannot be merged."""


@dataclass(frozen=True)
class Layer:
    name: str
    root: Path
    bucket: str
    prefix: str
    immutable: bool = False

    def key(self, path: Path) -> str:
        return self.prefix + path.relative_to(self.root).as_posix()

    def path(self, key: str) -> Path:
        relative = PurePosixPath(key[len(self.prefix):])
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise ObjectStorageError(f"Unsafe object key: {key}")
        return self.root.joinpath(*relative.parts)


class MedallionStore:
    def __init__(self, client: Any, layers: dict[str, Layer]) -> None:
        self.client = client
        self.layers = layers

    @classmethod
    def from_config(cls, config: PipelineConfig, client: Any | None = None) -> MedallionStore | None:
        """Return the store when both buckets are configured; local-only otherwise."""
        if not config.s3_bronze_bucket or not config.s3_lake_bucket:
            return None
        if client is None:
            import boto3

            client = boto3.client("s3", region_name=config.aws_region)
        bronze_root = Path(config.medallion_bronze_path)
        layers = {
            "bronze": Layer("bronze", bronze_root, config.s3_bronze_bucket, "", immutable=True),
            "silver": Layer("silver", Path(config.medallion_silver_path), config.s3_lake_bucket, "silver/"),
            "gold": Layer("gold", Path(config.medallion_gold_path), config.s3_lake_bucket, "gold/"),
            "quality": Layer("quality", bronze_root.parent / "quality", config.s3_lake_bucket, "quality/"),
        }
        return cls(client, layers)

    def push(self, names: tuple[str, ...] | None = None) -> dict[str, int]:
        return {name: self._push_layer(self.layers[name]) for name in names or tuple(self.layers)}

    def remote_keys(self, name: str, key_prefix: str = "") -> list[str]:
        """List object keys of one layer without downloading them."""
        return sorted(self._remote_index(self.layers[name], key_prefix))

    def pull(self, names: tuple[str, ...] | None = None, *, key_prefix: str = "") -> dict[str, int]:
        return {
            name: self._pull_layer(self.layers[name], key_prefix) for name in names or tuple(self.layers)
        }

    def _push_layer(self, layer: Layer) -> int:
        if not layer.root.is_dir():
            return 0
        state = self._load_state(layer)
        remote = self._remote_index(layer, "")
        uploaded = 0
        for path in sorted(_files(layer.root)):
            key = layer.key(path)
            stat = path.stat()
            known = state.get(key)
            if known and known["size"] == stat.st_size and known["mtime_ns"] == stat.st_mtime_ns:
                continue
            existing = remote.get(key)
            if existing is not None and (layer.immutable or known is None) and existing["size"] == stat.st_size:
                # Same immutable object, or a fresh node adopting what it previously pulled.
                state[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "etag": existing["etag"]}
                continue
            if existing is not None and layer.immutable:
                raise ObjectStorageError(f"Bronze object differs from the bucket and is immutable: {key}")
            self.client.upload_file(str(path), layer.bucket, key)
            etag = self.client.head_object(Bucket=layer.bucket, Key=key)["ETag"]
            state[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "etag": etag}
            uploaded += 1
        self._save_state(layer, state)
        return uploaded

    def _pull_layer(self, layer: Layer, key_prefix: str) -> int:
        state = self._load_state(layer)
        downloaded = 0
        for key, remote in self._remote_index(layer, key_prefix).items():
            path = layer.path(key)
            known = state.get(key)
            if path.is_file():
                stat = path.stat()
                if known and known["etag"] == remote["etag"] and known["size"] == stat.st_size:
                    continue
                if known is None and stat.st_size == remote["size"]:
                    state[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "etag": remote["etag"]}
                    continue
                if layer.immutable:
                    raise ObjectStorageError(f"Local Bronze object differs from the bucket: {key}")
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.download")
            self.client.download_file(layer.bucket, key, str(temporary))
            os.replace(temporary, path)
            stat = path.stat()
            state[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "etag": remote["etag"]}
            downloaded += 1
        self._save_state(layer, state)
        return downloaded

    def _remote_index(self, layer: Layer, key_prefix: str) -> dict[str, dict[str, Any]]:
        index: dict[str, dict[str, Any]] = {}
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=layer.bucket, Prefix=layer.prefix + key_prefix):
            for item in page.get("Contents", []):
                if PurePosixPath(item["Key"]).name.startswith("."):
                    continue
                index[item["Key"]] = {"size": item["Size"], "etag": item["ETag"]}
        return index

    @staticmethod
    def _state_path(layer: Layer) -> Path:
        return layer.root.parent / f".{layer.name}{STATE_FILE}"

    def _load_state(self, layer: Layer) -> dict[str, dict[str, Any]]:
        path = self._state_path(layer)
        if not path.is_file():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_state(self, layer: Layer, state: dict[str, dict[str, Any]]) -> None:
        path = self._state_path(layer)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)


def _files(root: Path):
    """Regular files under root, skipping hidden staging and temporary artifacts."""
    for path in root.rglob("*"):
        if path.is_file() and not any(part.startswith(".") for part in path.relative_to(root).parts):
            yield path


def push_quality(config: PipelineConfig) -> None:
    """Publish pipeline results for the API right away (no-op when S3 is not configured)."""
    store = MedallionStore.from_config(config)
    if store is not None:
        store.push(("quality",))


def fetch_bronze_batch(config: PipelineConfig, manifest_key: str) -> None:
    """Bring one API-published Bronze batch to the local cache (no-op when S3 is not configured)."""
    store = MedallionStore.from_config(config)
    if store is not None:
        batch_prefix = manifest_key.rsplit("/", 1)[0] + "/"
        store.pull(("bronze",), key_prefix=batch_prefix)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sincroniza a Medallion local com o S3.")
    parser.add_argument("action", choices=("push", "pull"))
    parser.add_argument("--layers", nargs="*", choices=("bronze", "silver", "gold", "quality"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    store = MedallionStore.from_config(PipelineConfig.from_env())
    if store is None:
        raise SystemExit("S3_BRONZE_BUCKET e S3_LAKE_BUCKET não configurados.")
    layers = tuple(args.layers) if args.layers else None
    result = store.push(layers) if args.action == "push" else store.pull(layers)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
