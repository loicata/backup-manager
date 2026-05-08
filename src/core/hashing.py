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
    """Hash the source file, then copy it via the OS-native copy
    primitive. The returned digest is the source's SHA-256.

    INVARIANT (see docs/INVARIANTS.md): the actual byte transfer MUST
    delegate to ``shutil.copy2`` (which resolves to ``CopyFileExW`` on
    Windows and ``sendfile`` / ``copy_file_range`` on Linux). A pure-
    Python read/hash/write loop here was empirically benchmarked at
    30–60× slower on a 30 k-small-file workload over USB; the loss
    was so severe (~7 MB/s vs ~50+ MB/s) it crashed the daily
    development cycle. Don't replace this delegation without a fresh
    benchmark on a real spinning/USB target.

    Why hash the SOURCE, not the destination
    -----------------------------------------
    Three pipelines were tried in v3.3.15–3.3.17:

    * v3.3.15 — Python read+hash+write loop. Hashes the bytes lying
      between source and destination, but uses no kernel primitive
      (slow).
    * v3.3.17 — ``shutil.copy2`` then ``compute_sha256(dst)``. Fast,
      but a silent corruption introduced by the kernel copy (rare
      hardware fault, bit-flip in a buggy driver) would be hashed
      from the corrupted destination and accepted as "valid": the
      manifest matches the destination, ``verify`` re-reads the
      destination, everything looks green even though the backup no
      longer matches the source.
    * v3.3.18 (current) — hash source first, then ``shutil.copy2``.
      The manifest contains a hash of what was on the source at hash
      time. The verify phase later re-hashes the destination and
      compares it against the manifest, so:
      - A corrupted copy → mismatch → ``verify`` rejects the backup
        (loud failure, user re-runs).
      - A source mutation during the copy → mismatch → same loud
        failure (a Frankenstein backup is never silently committed).

    Trade-off: the v3.3.18 model has a TOCTOU window between the
    hash and the copy (source could mutate in those few ms). The
    consequence is a *false-positive verify failure* — the user
    re-runs the backup. That's strictly safer than the v3.3.17
    silent acceptance of an inconsistent destination.

    Cost: the source is read twice — once for the hash, once by the
    kernel copy. The OS file-system cache serves the second read
    almost for free on small files (the bytes are hot in RAM); on
    large files we pay one extra linear read but the savings on the
    rest of the workload swamp it.

    Args:
        src_path: Source file path. Must exist and be a regular file.
        dst_path: Destination file path. Parent must exist.

    Returns:
        Lowercase hex SHA-256 digest of the source bytes (which, on
        a successful copy, equal the destination bytes).

    Raises:
        TypeError: If either argument is not a ``Path``.
        FileNotFoundError: If the source file does not exist.
        ValueError: If the source is a directory.
        OSError: On any I/O failure during the hash or the copy. The
            destination should be considered garbage by the caller
            (the write phase rolls back the entire backup).
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

    # Pre-pass: hash the source. This is what lands in the manifest.
    src_hash = compute_sha256(src_path)

    # Native copy. INVARIANT: do not replace this with a Python loop.
    shutil.copy2(src_str, dst_str)

    return src_hash
