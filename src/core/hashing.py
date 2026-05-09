"""Centralized SHA-256 file hashing utility.

Single source of truth for file hashing across the pipeline. Used by
the manifest phase (parallel hash of every source) and the verify
phase (parallel re-hash of every destination); see
``src/core/phases/manifest.py`` and ``src/core/phases/verifier.py``.
"""

import hashlib
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# 4 MiB matches the buffer size most efficient for sustained SSD/USB
# throughput. SHA-256 absorbs big chunks without CPU penalty (~500 MB/s
# on a modern desktop core) and fewer Python-loop iterations mean fewer
# syscall round-trips on external SSDs. The previous 128 KiB value
# capped backup throughput at ~7 MB/s on a Samsung T7 USB SSD; 4 MiB
# restores the saturating curve (vs 50+ MB/s in v3.3.14, which already
# went through ``shutil.copy2`` for the byte transfer).
HASH_CHUNK_SIZE: int = 4 * 1024 * 1024  # 4 MiB


def _long_path(p: Path) -> str:
    """Return a Windows long-path-prefixed string when applicable.

    On Windows, prefixing with ``\\\\?\\`` lifts the legacy MAX_PATH
    limit (260 chars) so deep backup hierarchies do not trip
    ``FileNotFoundError`` on perfectly valid paths.
    """
    s = str(p)
    if os.name == "nt" and not s.startswith("\\\\?\\"):
        return f"\\\\?\\{s}"
    return s


def compute_sha256(filepath: Path) -> str:
    """Compute SHA-256 hash of a file.

    Reads the file in chunks of HASH_CHUNK_SIZE to keep memory usage
    constant regardless of file size.

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

    str_path = _long_path(filepath)

    if filepath.is_dir():
        raise ValueError(f"Expected a file, not a directory: {filepath}")

    if not os.path.exists(str_path):
        raise FileNotFoundError(f"File not found: {filepath}")

    h = hashlib.sha256()
    with open(str_path, "rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
