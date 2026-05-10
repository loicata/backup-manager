r"""End-to-end coverage of the >260-char Windows path limit through the pipeline.

The legacy ``MAX_PATH = 260`` rule on Windows silently breaks ``open()``,
``shutil.copy2`` and ``hashlib`` if the absolute path is not prefixed
with ``\\?\``. ``safe_remove_tree`` is already covered by
``unit/test_safe_remove_tree.py``, but the *write/hash* side of the
pipeline was untested. A regression there would cause:

- ``compute_sha256`` to raise ``FileNotFoundError`` on a path the user
  thinks exists (deep CJK source tree, e.g. ``Cours\\Module\\...``);
- ``write_flat`` to raise a generic ``WriteError`` mid-backup;
- ``build_integrity_manifest`` to raise from inside its
  ``ThreadPoolExecutor`` and abort the whole run.

These tests build a >260-char tree via the long-path prefix and exercise
each public entry point on it. They are Windows-only by design — on
POSIX the limit doesn't exist and the long-path prefix is a no-op.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.core.hashing import compute_sha256
from src.core.phases.collector import FileInfo
from src.core.phases.local_writer import write_flat
from src.core.phases.manifest import build_integrity_manifest

WINDOWS_ONLY = pytest.mark.skipif(
    os.name != "nt", reason="MAX_PATH limit is Windows-specific"
)

# A path long enough to be unambiguously past MAX_PATH once the temp
# root prefix is added. 8 segments of ~30 chars + ~200-char filename
# makes the absolute path comfortably above 260 even when ``tmp_path``
# is short.
_DEEP_SEGMENTS = [f"segment_{i:02d}_" + "x" * 22 for i in range(8)]
_LEAF_NAME = "leaf_" + "y" * 200 + ".txt"


def _build_long_tree(
    root: Path, content: bytes = b"long-path payload"
) -> tuple[Path, int, float]:
    r"""Create a >260-char file under ``root``.

    Uses the ``\\?\`` long-path prefix to bypass MAX_PATH during creation
    (plain ``Path.mkdir`` itself trips the limit it is supposed to test).
    Returns the un-prefixed Path plus its size and mtime — both fetched
    via the long-path prefix because ``Path.stat()`` itself fails on
    >260-char paths (which is precisely the bug we test the production
    code against).
    """
    deep_rel = Path(*_DEEP_SEGMENTS)
    deep_abs = root / deep_rel
    deep_long = "\\\\?\\" + str(deep_abs)
    os.makedirs(deep_long, exist_ok=True)

    leaf_long = deep_long + "\\" + _LEAF_NAME
    with open(leaf_long, "wb") as f:
        f.write(content)

    leaf_abs = deep_abs / _LEAF_NAME
    # Sanity: the un-prefixed leaf path must exceed MAX_PATH or the test
    # is not actually testing what it claims to test.
    assert len(str(leaf_abs)) > 260, (
        f"Test fixture must produce >260-char path, got {len(str(leaf_abs))}"
    )
    st = os.stat(leaf_long)
    return leaf_abs, st.st_size, st.st_mtime


@WINDOWS_ONLY
class TestComputeSha256LongPath:
    """``compute_sha256`` must hash files past MAX_PATH."""

    def test_hashes_long_path_file(self, tmp_path: Path) -> None:
        leaf, _, _ = _build_long_tree(tmp_path, b"hello long")

        digest = compute_sha256(leaf)

        # The same content via a short-path file must produce the same
        # digest — the long-path handling is purely about reachability,
        # not content interpretation.
        short = tmp_path / "short.txt"
        short.write_bytes(b"hello long")
        assert digest == compute_sha256(short)

    def test_long_path_hash_is_deterministic(self, tmp_path: Path) -> None:
        leaf, _, _ = _build_long_tree(tmp_path, b"determinism check")
        assert compute_sha256(leaf) == compute_sha256(leaf)


@WINDOWS_ONLY
class TestBuildIntegrityManifestLongPath:
    """``build_integrity_manifest`` (parallel pool) must handle long paths.

    The hash workers run inside a ThreadPoolExecutor and the manifest
    phase is fail-fast — an unhandled FileNotFoundError on the worker
    would abort the whole backup with a misleading message.
    """

    def test_manifest_built_for_long_path_source(self, tmp_path: Path) -> None:
        leaf, size, mtime = _build_long_tree(tmp_path, b"manifest-via-long-path")
        rel_path = "deep/" + leaf.name  # arbitrary stable relative key

        files = [
            FileInfo(
                source_path=leaf,
                relative_path=rel_path,
                size=size,
                mtime=mtime,
                source_root=str(tmp_path),
            )
        ]

        manifest = build_integrity_manifest(files)

        assert rel_path in manifest["files"]
        entry = manifest["files"][rel_path]
        assert len(entry["hash"]) == 64  # sha256 hex digest
        assert entry["size"] == len(b"manifest-via-long-path")
        assert manifest["total_checksum"]


@WINDOWS_ONLY
class TestWriteFlatLongPath:
    """``write_flat`` must copy source files whose path exceeds MAX_PATH."""

    def test_copies_long_path_source(self, tmp_path: Path) -> None:
        source_root = tmp_path / "src_root"
        source_root.mkdir()
        leaf, size, mtime = _build_long_tree(source_root, b"flat-copy-payload")
        rel_path = "long/" + leaf.name

        files = [
            FileInfo(
                source_path=leaf,
                relative_path=rel_path,
                size=size,
                mtime=mtime,
                source_root=str(source_root),
            )
        ]

        dest_root = tmp_path / "dst_root"
        dest_root.mkdir()

        backup_dir = write_flat(files, dest_root, "bk_long")

        # The destination path is also >260 chars (dest_root + bk_long +
        # rel_path), so validating its existence exercises the same
        # long-path-aware probe in the verify direction.
        target = backup_dir / rel_path
        assert os.path.exists("\\\\?\\" + str(target.resolve()))

    def test_copies_to_long_destination_path(self, tmp_path: Path) -> None:
        """Even with a short source, a deep destination must succeed.

        Real-world failure mode: the source is short (e.g. ``C:\\src``)
        but the destination is a deeply-nested USB/NAS path
        (``G:\\Backups\\<profile>\\<long-name>\\<rel>``). The cumulative
        path exceeds MAX_PATH only on the destination side.
        """
        src = tmp_path / "src.txt"
        src.write_bytes(b"short source")
        rel_path = "deep/" + "/".join(_DEEP_SEGMENTS) + "/" + _LEAF_NAME

        files = [
            FileInfo(
                source_path=src,
                relative_path=rel_path,
                size=src.stat().st_size,
                mtime=src.stat().st_mtime,
                source_root=str(tmp_path),
            )
        ]

        dest_root = tmp_path / "dst_root"
        dest_root.mkdir()
        backup_dir = write_flat(files, dest_root, "bk_long_dst")

        target = backup_dir / rel_path
        assert os.path.exists("\\\\?\\" + str(target.resolve()))


@WINDOWS_ONLY
class TestRoundTripLongPath:
    """End-to-end: hash source → write → re-hash destination must match.

    Mirrors what the real pipeline does (manifest phase + write phase +
    verify phase). If any of the three layers loses its long-path
    awareness, the round-trip fails.
    """

    def test_source_and_destination_hashes_match(self, tmp_path: Path) -> None:
        source_root = tmp_path / "src_root"
        source_root.mkdir()
        payload = b"round-trip-payload-" + b"z" * 1024
        leaf, size, mtime = _build_long_tree(source_root, payload)
        rel_path = "deep/" + leaf.name

        files = [
            FileInfo(
                source_path=leaf,
                relative_path=rel_path,
                size=size,
                mtime=mtime,
                source_root=str(source_root),
            )
        ]

        manifest = build_integrity_manifest(files)
        source_hash = manifest["files"][rel_path]["hash"]

        dest_root = tmp_path / "dst_root"
        dest_root.mkdir()
        backup_dir = write_flat(files, dest_root, "bk_round")

        copied = backup_dir / rel_path
        dest_hash = compute_sha256(copied)

        assert source_hash == dest_hash
