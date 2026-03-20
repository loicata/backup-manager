"""Centralized SHA-256 file hashing utility.

Single source of truth for file hashing across the pipeline.
Replaces the duplicated compute_file_hash() in filter.py and manifest.py.
"""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

HASH_CHUNK_SIZE: int = 128 * 1024  # 128 KiB


def compute_sha256(filepath: Path) -> str:
    """Compute SHA-256 hash of a file.

    Reads the file in chunks of HASH_CHUNK_SIZE to keep
    memory usage constant regardless of file size.

    Args:
        filepath: Absolute path to the file to hash.

    Returns:
        Lowercase hex digest string (64 characters).

    Raises:
        TypeError: If filepath is not a Path instance.
        ValueError: If filepath points to a directory.
        FileNotFoundError: If the file does not exist.
        PermissionError: If the file is not readable.
        OSError: On other I/O errors.
    """
    if not isinstance(filepath, Path):
        raise TypeError(f"Expected Path, got {type(filepath).__name__}: {filepath!r}")

    if filepath.is_dir():
        raise ValueError(f"Expected a file, not a directory: {filepath}")

    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
