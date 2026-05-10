"""Tests for ``filter_changed_files`` against state-poisoning scenarios.

The diff/incremental engine compares the live source tree against the
last-run manifest. The happy path (size differs → re-include; size
identical + same hash → skip) is covered elsewhere. This module covers
the *poison* cases — unusual sequences of source-tree mutations that
have historically caused silent backup misses or unbounded re-hash
loops:

- Same path, same size, different content (atomic replace, mtime may
  or may not have moved): must be re-included on hash mismatch.
- Mtime moved backwards / unchanged but content changed: hash check
  is the source of truth, mtime is **not** consulted.
- File deleted then a new one created at the same relative path
  (``rm a.txt && echo new > a.txt``): treated as a normal change,
  no orphan tracking needed.
- File renamed (same content, new path): old path drops out of the
  next manifest, new path enters as "new".
- Manifest JSON corrupted / truncated: ``load_manifest`` returns ``{}``
  so every file is treated as new (full backup) — never a partial
  silent diff.
- Manifest entry missing the ``hash`` field: degrade to re-hashing the
  source rather than crashing or skipping silently.
- ``OSError`` on hash compute (file locked, broken symlink): drop the
  file from the *changed* set so the integrity-manifest phase doesn't
  fail-fast on an unhashable input. The next run picks the file up via
  the unchanged old-hash entry, re-trying naturally.

``build_updated_manifest`` is exercised on the symmetric case (a
worker hits OSError mid-hash) to confirm the entry is dropped from the
saved manifest with a logged warning, so the next run sees the file as
new and retries.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import patch

from src.core.phases.collector import FileInfo
from src.core.phases.filter import (
    build_updated_manifest,
    filter_changed_files,
    load_manifest,
)


def _file_info(path: Path, source_root: Path, rel: str | None = None) -> FileInfo:
    """Build a FileInfo from a real on-disk file."""
    st = path.stat()
    return FileInfo(
        source_path=path,
        relative_path=rel or path.name,
        size=st.st_size,
        mtime=st.st_mtime,
        source_root=str(source_root),
    )


def _write_manifest(path: Path, manifest: dict) -> None:
    """Persist a manifest dict to disk in the format ``filter`` expects."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest), encoding="utf-8")


# ---------------------------------------------------------------------------
# Same path, same size, different content
# ---------------------------------------------------------------------------


class TestContentChangeWithSameSize:
    """A file rewritten with different bytes but identical length."""

    def test_same_size_different_content_is_reincluded(self, tmp_path: Path):
        src = tmp_path / "src"
        src.mkdir()
        f = src / "doc.txt"
        f.write_text("AAAA", encoding="utf-8")  # 4 bytes
        manifest_path = tmp_path / "manifest.json"

        # Manifest reflects the OLD content's hash.
        from src.core.hashing import compute_sha256

        old_hash = compute_sha256(f)
        _write_manifest(
            manifest_path,
            {"doc.txt": {"hash": old_hash, "size": 4, "mtime": 0.0}},
        )

        # Replace content in place with a same-length but different payload.
        f.write_text("BBBB", encoding="utf-8")
        fi = _file_info(f, src)

        changed, computed = filter_changed_files([fi], manifest_path)

        assert [c.relative_path for c in changed] == ["doc.txt"]
        # The new hash must have been computed for downstream phases.
        assert computed["doc.txt"] == compute_sha256(f)
        assert computed["doc.txt"] != old_hash

    def test_same_size_same_content_is_skipped(self, tmp_path: Path):
        """Sanity guard: identical content must NOT be re-included."""
        src = tmp_path / "src"
        src.mkdir()
        f = src / "doc.txt"
        f.write_text("AAAA", encoding="utf-8")
        manifest_path = tmp_path / "manifest.json"

        from src.core.hashing import compute_sha256

        h = compute_sha256(f)
        _write_manifest(
            manifest_path,
            {"doc.txt": {"hash": h, "size": 4, "mtime": 0.0}},
        )

        fi = _file_info(f, src)
        changed, _ = filter_changed_files([fi], manifest_path)

        assert changed == []


# ---------------------------------------------------------------------------
# Mtime is not authoritative
# ---------------------------------------------------------------------------


class TestMtimeIsNotConsulted:
    """The filter ignores mtime and relies on size + hash only.

    Justification: mtime can be reset (rsync ``-t``, ``touch -d``,
    cloud-sync clients) without the content changing, and conversely
    can stay identical while content changes (atomic replace on Btrfs
    or NTFS). A backup tool that trusted mtime would miss real
    changes — the only safe signal is the hash itself.
    """

    def test_mtime_moved_backwards_with_changed_content_still_reincluded(
        self, tmp_path: Path
    ):
        src = tmp_path / "src"
        src.mkdir()
        f = src / "doc.txt"
        f.write_text("OLD", encoding="utf-8")
        manifest_path = tmp_path / "manifest.json"

        from src.core.hashing import compute_sha256

        old_hash = compute_sha256(f)
        # Manifest stores a future mtime — the live mtime is "older"
        # than the recorded one, which would fool a naive mtime check.
        _write_manifest(
            manifest_path,
            {
                "doc.txt": {
                    "hash": old_hash,
                    "size": 3,
                    "mtime": 9_999_999_999.0,
                }
            },
        )

        # Now replace content with a different 3-byte payload — same
        # size, mtime is even older than the manifest's recorded value.
        f.write_text("NEW", encoding="utf-8")
        fi = _file_info(f, src)
        # Force the live mtime to be unambiguously older.
        import os

        os.utime(f, (1.0, 1.0))
        fi = _file_info(f, src)

        changed, _ = filter_changed_files([fi], manifest_path)
        assert [c.relative_path for c in changed] == ["doc.txt"]


# ---------------------------------------------------------------------------
# Delete + recreate at same path
# ---------------------------------------------------------------------------


class TestDeleteAndRecreate:
    """``rm path && echo new > path`` — treated as a normal change."""

    def test_recreated_file_with_different_size_reincluded(self, tmp_path: Path):
        src = tmp_path / "src"
        src.mkdir()
        f = src / "x.txt"
        f.write_text("original", encoding="utf-8")
        manifest_path = tmp_path / "manifest.json"

        from src.core.hashing import compute_sha256

        _write_manifest(
            manifest_path,
            {
                "x.txt": {
                    "hash": compute_sha256(f),
                    "size": 8,
                    "mtime": 0.0,
                }
            },
        )

        # Simulate delete + recreate with different content + size.
        f.unlink()
        f.write_text("brand new content here", encoding="utf-8")

        fi = _file_info(f, src)
        changed, _ = filter_changed_files([fi], manifest_path)

        assert [c.relative_path for c in changed] == ["x.txt"]


# ---------------------------------------------------------------------------
# Rename: old path drops out, new path enters as new
# ---------------------------------------------------------------------------


class TestRename:
    """A renamed file appears as a new entry; the old path is naturally absent."""

    def test_rename_emits_new_path_only(self, tmp_path: Path):
        src = tmp_path / "src"
        src.mkdir()
        old = src / "before.txt"
        old.write_text("static content", encoding="utf-8")

        from src.core.hashing import compute_sha256

        h = compute_sha256(old)

        manifest_path = tmp_path / "manifest.json"
        _write_manifest(
            manifest_path,
            {"before.txt": {"hash": h, "size": old.stat().st_size, "mtime": 0.0}},
        )

        # Rename on disk; the live tree only contains the new path.
        new = src / "after.txt"
        old.rename(new)

        fi_new = _file_info(new, src, rel="after.txt")
        changed, _ = filter_changed_files([fi_new], manifest_path)

        assert [c.relative_path for c in changed] == ["after.txt"]


# ---------------------------------------------------------------------------
# Corrupted / unreadable manifest → full backup, never partial diff
# ---------------------------------------------------------------------------


class TestManifestCorruption:
    """A corrupt manifest must yield a full backup, not a silent partial."""

    def test_truncated_json_treated_as_no_manifest(self, tmp_path: Path):
        src = tmp_path / "src"
        src.mkdir()
        a = src / "a.txt"
        a.write_text("a", encoding="utf-8")
        b = src / "b.txt"
        b.write_text("b", encoding="utf-8")

        manifest_path = tmp_path / "manifest.json"
        # Truncated JSON: opens an object but never closes it.
        manifest_path.write_text('{"a.txt": {"hash": "ab', encoding="utf-8")

        # ``load_manifest`` itself returns {} on corruption — guards the
        # invariant at the source of truth.
        assert load_manifest(manifest_path) == {}

        files = [_file_info(a, src), _file_info(b, src)]
        changed, _ = filter_changed_files(files, manifest_path)

        # Every live file is included — full backup behaviour.
        assert {c.relative_path for c in changed} == {"a.txt", "b.txt"}

    def test_missing_manifest_treated_as_no_manifest(self, tmp_path: Path):
        src = tmp_path / "src"
        src.mkdir()
        a = src / "a.txt"
        a.write_text("a", encoding="utf-8")

        # Path that never existed.
        manifest_path = tmp_path / "never_written.json"

        fi = _file_info(a, src)
        changed, computed = filter_changed_files([fi], manifest_path)

        # Filter returns the input list as-is (full backup) and
        # produces NO computed_hashes (no manifest to compare against).
        assert [c.relative_path for c in changed] == ["a.txt"]
        assert computed == {}


# ---------------------------------------------------------------------------
# Manifest entry missing the ``hash`` field (legacy / partial write)
# ---------------------------------------------------------------------------


class TestManifestEntryMissingHash:
    """A manifest entry without ``hash`` falls back to re-hashing live."""

    def test_missing_hash_field_triggers_rehash_and_inclusion(
        self, tmp_path: Path
    ):
        src = tmp_path / "src"
        src.mkdir()
        f = src / "x.txt"
        f.write_text("payload", encoding="utf-8")

        manifest_path = tmp_path / "manifest.json"
        # Same size, but the ``hash`` key is absent — the diff path
        # MUST not crash on the missing key. With ``prev.get("hash", "")``
        # comparing against the empty string, the live hash will never
        # match, so the file is re-included.
        _write_manifest(
            manifest_path,
            {
                "x.txt": {
                    "size": f.stat().st_size,
                    "mtime": f.stat().st_mtime,
                }
            },
        )

        fi = _file_info(f, src)
        changed, computed = filter_changed_files([fi], manifest_path)

        assert [c.relative_path for c in changed] == ["x.txt"]
        # Re-hash happened despite the malformed manifest entry.
        assert "x.txt" in computed


# ---------------------------------------------------------------------------
# OSError mid-hash: file dropped from the changed set, not propagated
# ---------------------------------------------------------------------------


class TestUnreadableFileDuringFilter:
    """An OSError during the filter's hash check must not bubble up.

    Otherwise the integrity-manifest phase (which is fail-fast by
    design) would later attempt to hash the same file, fail again, and
    abort the whole run — even though the file was never going to be
    included anyway. The filter drops it silently from ``changed`` and
    logs a WARNING so operators can see the recurring skip.
    """

    def test_oserror_during_hash_drops_file_silently(
        self, tmp_path: Path, caplog
    ):
        src = tmp_path / "src"
        src.mkdir()
        good = src / "good.txt"
        good.write_text("ok", encoding="utf-8")
        broken = src / "broken.txt"
        broken.write_text("locked", encoding="utf-8")

        from src.core.hashing import compute_sha256

        # Manifest matches the GOOD file's content; BROKEN has a
        # plausible old hash so the size-equal+hash-different path
        # doesn't short-circuit before we hit the OSError.
        manifest_path = tmp_path / "manifest.json"
        _write_manifest(
            manifest_path,
            {
                "good.txt": {
                    "hash": compute_sha256(good),
                    "size": good.stat().st_size,
                    "mtime": 0.0,
                },
                "broken.txt": {
                    "hash": "0" * 64,
                    "size": broken.stat().st_size,
                    "mtime": 0.0,
                },
            },
        )

        original = compute_sha256

        def selective_fail(path):
            if path.name == "broken.txt":
                raise OSError(13, "Access denied")
            return original(path)

        files = [_file_info(good, src), _file_info(broken, src)]

        with caplog.at_level(logging.WARNING), patch(
            "src.core.phases.filter.compute_sha256",
            side_effect=selective_fail,
        ):
            changed, computed = filter_changed_files(files, manifest_path)

        # GOOD: unchanged, not included.
        # BROKEN: unhashable, dropped silently from ``changed``.
        assert changed == []
        assert "broken.txt" not in computed
        # The skip is loud in the log — operators must see it.
        assert any("broken.txt" in r.message for r in caplog.records)


class TestUnreadableFileDuringManifestBuild:
    """``build_updated_manifest`` symmetrically skips unhashable files."""

    def test_oserror_drops_file_from_new_manifest(self, tmp_path: Path, caplog):
        src = tmp_path / "src"
        src.mkdir()
        a = src / "a.txt"
        a.write_text("a", encoding="utf-8")
        b = src / "b.txt"
        b.write_text("b", encoding="utf-8")

        files = [_file_info(a, src), _file_info(b, src)]

        from src.core.hashing import compute_sha256

        original = compute_sha256

        def selective_fail(path):
            if path.name == "b.txt":
                raise OSError(13, "Access denied during manifest build")
            return original(path)

        with caplog.at_level(logging.WARNING), patch(
            "src.core.phases.filter.compute_sha256",
            side_effect=selective_fail,
        ):
            manifest = build_updated_manifest(files)

        # The good file is in the manifest; the bad one is omitted so
        # the next run sees it as ``new`` and retries naturally.
        assert "a.txt" in manifest
        assert "b.txt" not in manifest
        # Skip is logged so the regression is observable.
        assert any("b.txt" in r.message for r in caplog.records)
