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
    """Copy a file via the OS-native copy primitive, then hash the
    bytes that landed on the destination.

    Implementation history
    ----------------------
    The first cut (3.3.15) used a Python ``read → hash → write`` loop
    with ``HASH_CHUNK_SIZE`` chunks so the manifest could be built from
    the bytes we wrote, closing the manifest→write TOCTOU window of
    the previous two-pass design (hash source, copy source — source
    could mutate between the two opens).

    On a workload of 30 k+ small files (e.g. a ``site-packages``
    snapshot full of JSON data files), that loop was 30–60× slower
    than 3.3.14's plain ``shutil.copy2``. The reason: ``shutil.copy2``
    on Windows delegates to ``CopyFileExW``, which packs the open +
    transfer + metadata copy into a single tightly-tuned kernel
    transaction. A Python user-space loop pays Python+syscall overhead
    on every chunk and on every per-file ``open/close/copystat`` —
    that overhead dominates when each file is only a few KB.

    The current shape preserves the anti-TOCTOU guarantee while
    restoring native-copy throughput:

    1. ``shutil.copy2`` writes ``src → dst`` in kernel-space.
    2. ``compute_sha256(dst)`` reads back ``dst`` and returns its
       digest. Reading from the just-written destination is mostly
       served by the OS file-system cache (the bytes are still hot in
       RAM for small files), so the second pass is near-free; on
       multi-MB files we pay one more linear read but the savings
       from the native copy on the rest of the workload swamp it.

    Why this still defeats the TOCTOU
    ---------------------------------
    The hash describes exactly the bytes that are on the destination
    after the copy.  Whatever the source mutated to between or after
    those two operations is irrelevant: the verify phase later re-hashes
    the destination and matches it against the manifest we built here,
    so a sleeping-pill scenario where the source becomes inconsistent
    AFTER the copy can never produce a "valid" manifest that disagrees
    with what is on disk.

    Args:
        src_path: Source file path. Must exist and be a regular file.
        dst_path: Destination file path. Parent must exist.

    Returns:
        Lowercase hex SHA-256 digest of the bytes that ended up on
        ``dst_path``.

    Raises:
        TypeError: If either argument is not a ``Path``.
        FileNotFoundError: If the source file does not exist.
        ValueError: If the source is a directory.
        OSError: On any I/O failure during the copy or the read-back
            hash. The destination should be considered garbage by the
            caller (the write phase rolls back the entire backup).
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

    # Native copy — on Windows this is CopyFileExW, on Linux it
    # uses sendfile/copy_file_range when the source and destination
    # filesystems allow it. ``copy2`` also calls ``copystat`` for us,
    # so the mtime + read-only bit + extended attributes are
    # preserved with the fewest possible round-trips.
    shutil.copy2(src_str, dst_str)

    # Hash from the destination, not the source: the destination is
    # frozen now (the copy returned), so the hash is guaranteed to
    # describe exactly what verify will later re-read. The OS file
    # cache absorbs most of the cost for small files; large files
    # pay one extra linear read.
    return compute_sha256(dst_path)
