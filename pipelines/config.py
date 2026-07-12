from __future__ import annotations

import os

STORAGE_MODE = os.environ.get("CMS_STORAGE_MODE", "local").lower()
S3_BUCKET = os.environ.get("CMS_S3_BUCKET", "healthcare-ed-throughput")

_LOCAL_ROOTS = {
    "raw": "data/raw",
    "curated": "data/curated",
}


def get_dir(layer: str) -> str:
    if layer not in _LOCAL_ROOTS:
        raise ValueError(f"Unknown storage layer: {layer!r}. Expected 'raw' or 'curated'.")

    if STORAGE_MODE == "s3":
        return f"s3a://{S3_BUCKET}/{layer}"
    if STORAGE_MODE == "local":
        return _LOCAL_ROOTS[layer]
    raise ValueError(
        f"Unknown CMS_STORAGE_MODE: {STORAGE_MODE!r}. Expected 'local' or 's3'."
    )


def is_local() -> bool:
    return STORAGE_MODE == "local"
