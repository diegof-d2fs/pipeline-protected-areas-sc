"""Safe resolution of API-published, manifest-directed Bronze inputs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


class ManifestInputError(RuntimeError):
    """Raised when dag_run.conf does not identify a valid immutable Bronze batch."""


@dataclass(frozen=True)
class ManifestInput:
    import_id: str
    manifest_key: str
    manifest_path: Path
    canonical_path: Path
    payload: dict[str, Any]


def load_manifest_input(
    bronze_root: Path,
    conf: dict[str, Any] | None,
    *,
    expected_domain: str,
) -> ManifestInput | None:
    """Return a directed input when conf is present; preserve scheduled-run compatibility otherwise."""

    if not conf or not conf.get("manifest_key"):
        return None
    import_id = str(conf.get("import_id", "")).strip()
    if not import_id:
        raise ManifestInputError("dag_run.conf must contain import_id with manifest_key.")

    manifest_key = str(conf["manifest_key"])
    manifest_path = _resolve_under(bronze_root, manifest_key)
    if not manifest_path.is_file():
        raise ManifestInputError(f"Bronze manifest does not exist: {manifest_key}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    if str(payload.get("import_id")) != import_id:
        raise ManifestInputError("Manifest import_id does not match dag_run.conf.")
    if payload.get("domain") != expected_domain:
        raise ManifestInputError(
            f"Manifest domain '{payload.get('domain')}' is not valid for '{expected_domain}'."
        )
    expected_checksum = str(conf.get("checksum_sha256", "")).strip()
    actual_checksum = str(payload.get("original", {}).get("checksum_sha256", "")).strip()
    if expected_checksum and actual_checksum != expected_checksum:
        raise ManifestInputError("Manifest checksum does not match dag_run.conf.")

    canonical_key = str(payload.get("bronze", {}).get("canonical_key", "")).strip()
    if not canonical_key:
        raise ManifestInputError("Manifest does not contain bronze.canonical_key.")
    canonical_path = _resolve_under(bronze_root, canonical_key)
    if not canonical_path.is_file():
        raise ManifestInputError(f"Canonical Bronze artifact does not exist: {canonical_key}")

    return ManifestInput(
        import_id=import_id,
        manifest_key=manifest_key,
        manifest_path=manifest_path,
        canonical_path=canonical_path,
        payload=payload,
    )


def _resolve_under(root: Path, key: str) -> Path:
    resolved_root = root.resolve()
    normalized = PurePosixPath(key)
    if normalized.is_absolute() or not normalized.parts or any(
        part in {"", ".", ".."} for part in normalized.parts
    ):
        raise ManifestInputError(f"Invalid Bronze key: {key}")
    if "\\" in key:
        raise ManifestInputError("Bronze keys must use POSIX separators.")
    candidate = resolved_root.joinpath(*normalized.parts).resolve()
    if resolved_root not in candidate.parents:
        raise ManifestInputError(f"Bronze key escapes configured root: {key}")
    return candidate
