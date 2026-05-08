"""Centralized SHA-256 file hashing utility.

Single source of truth for file hashing across the pipeline.
Replaces the duplicated compute_file_hash() in filter.py and manifest.py.
"""

import hashlib
import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# 4 MiB matches the buffer size most efficient for sustained
# SSD/USB throughput. ``shutil.copy2`` uses 1 MiB internally on
# Windows; we go a bit larger because SHA-256 absorbs big chunks
# without CPU penalty (~500 MB/s on a modern desktop core) and
# fewer Python-loop iterations means fewer syscall round-trips on
# external SSDs. The previous 128 KiB value was inherited from the
# old read-only ``compute_sha256`` helper, where it was fine; once
# ``copy_and_hash`` started feeding the same chunk into a write,
# 128 KiB became the bottleneck and capped backup throughput at
# ~7 MB/s on a Samsung T7 USB SSD (vs 50+ MB/s in 3.3.14, which
# went through ``shutil.copy2``).
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


def copy_and_hash(src_path: Path, dst_path: Path) -> str:
    """Copy a file and compute its SHA-256 in a single source read.

    Defeats the manifest→write TOCTOU window: with two-pass copying,
    a source file modified between the hash pass and the copy pass
    ends up on the destination with content that does NOT match the
    manifest's hash, and the verify phase rejects the entire backup.
    By hashing the bytes that we are actually writing to the
    destination, the manifest is guaranteed to describe exactly what
    landed on disk.

    The verify phase remains useful: it re-reads the destination and
    detects corruption introduced by the storage medium (USB cable,
    NTFS bug, ECC failure) between write and read. This function
    closes only the source-side TOCTOU; verify still closes the
    destination-side one.

    Args:
        src_path: Source file path. Must exist and be a regular file.
        dst_path: Destination file path. Parent must exist.

    Returns:
        Lowercase hex SHA-256 digest of the bytes copied to ``dst_path``.

    Raises:
        TypeError: If either argument is not a ``Path``.
        FileNotFoundError: If the source file does not exist.
        ValueError: If the source is a directory.
        OSError: On any I/O failure during read or write. Caller
            should treat the destination file as garbage and clean it
            up — we do not attempt cleanup here because the caller has
            broader context (write phase rolls back the entire backup).
    """
    if not isinstance(src_path, Path):
        raise TypeError(f"src_path must be a Path, got {type(src_path).__name__}")
    if not isinstance(dst_path, Path):
        raise TypeError(f"dst_path must be a Path, got {type(dst_path).__name__}")
    src_str = _long_path(src_path)
    dst_str = _long_path(dst_path)

    if not os.path.exists(src_str):
        raise FileNotFoundError(f"Source not found: {src_path}")
    if src_path.is_dir():
        raise ValueError(f"Source is a directory, not a file: {src_path}")

    h = hashlib.sha256()
    with open(src_str, "rb") as src, open(dst_str, "wb") as dst:
        while True:
            chunk = src.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
            dst.write(chunk)

    # Preserve mtime + permission bits as ``shutil.copy2`` would.
    # ``copystat`` is best-effort: an unsupported filesystem (FAT
    # without extended attributes, network share with limited ACLs)
    # raises and we let it propagate so the caller sees a real
    # backup-integrity failure rather than a silently inconsistent
    # mtime that confuses incremental detection later.
    shutil.copystat(src_str, dst_str)

    return h.hexdigest()
