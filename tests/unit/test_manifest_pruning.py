"""Tests for ``prune_manifest_entries`` and the ``skipped_files`` branch
of ``_compute_total_checksum`` in :mod:`src.core.phases.manifest`.

These tests pin the contract that a writer skipping a file (e.g. the
source vanishes after hashing) MUST surface that loss, both by:

1. Removing the entry from ``manifest["files"]`` so verify does not
   forever flag it as "missing".
2. Recording the skipped entry under ``manifest["skipped_files"]`` so
   the verifier and UI can display the data loss.
3. Recomputing the total checksum so the manifest stays internally
   consistent — but in a way that BINDS the ``skipped_files`` list
   into the digest so an attacker cannot strip the list and still
   produce a valid-looking manifest.
"""

from __future__ import annotations

import hashlib


from src.core.phases.manifest import (
    _compute_total_checksum,
    prune_manifest_entries,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manifest(files: dict[str, dict]) -> dict:
    """Build a minimal manifest dict shaped like ``build_integrity_manifest``."""
    return {
        "version": 1,
        "algorithm": "sha256",
        "files": dict(files),
        "total_checksum": _compute_total_checksum(files),
    }


def _entry(hash_hex: str = "a" * 64, size: int = 10) -> dict:
    return {"hash": hash_hex, "size": size}


# ---------------------------------------------------------------------------
# _compute_total_checksum — skipped_files branch
# ---------------------------------------------------------------------------


class TestComputeTotalChecksumSkipped:
    """Skipped entries must alter the digest (anti-strip property)."""

    def test_no_skipped_returns_files_only_digest(self) -> None:
        files = {"a.txt": _entry("a" * 64, 1)}
        digest = _compute_total_checksum(files)
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)

    def test_empty_skipped_list_equals_no_skipped(self) -> None:
        # A truthiness check (``if skipped_files``) treats ``[]`` and
        # ``None`` identically; pin that behaviour so the digest stays
        # stable when callers pass an explicit empty list.
        files = {"a.txt": _entry()}
        assert _compute_total_checksum(files, skipped_files=[]) == _compute_total_checksum(
            files, skipped_files=None
        )

    def test_skipped_entries_change_digest(self) -> None:
        files = {"a.txt": _entry()}
        skipped = [
            {
                "path": "vanished.txt",
                "reason": "vanished_during_write",
                "recorded_hash": "b" * 64,
                "recorded_size": 5,
            }
        ]
        d_no_skip = _compute_total_checksum(files)
        d_with_skip = _compute_total_checksum(files, skipped_files=skipped)
        assert d_no_skip != d_with_skip

    def test_skipped_order_does_not_change_digest(self) -> None:
        # The function sorts ``skipped_files`` by path before hashing
        # so two equivalent lists in different orders produce the same
        # digest. Regression guards against a future refactor that
        # would forget the sort.
        files = {"a.txt": _entry()}
        s1 = [
            {"path": "z.txt", "reason": "x", "recorded_hash": "0" * 64, "recorded_size": 1},
            {"path": "a.txt", "reason": "y", "recorded_hash": "1" * 64, "recorded_size": 2},
        ]
        s2 = list(reversed(s1))
        assert _compute_total_checksum(files, skipped_files=s1) == _compute_total_checksum(
            files, skipped_files=s2
        )

    def test_skipped_with_missing_keys_uses_defaults(self) -> None:
        # ``item.get("path", "")`` etc. defends against malformed skipped
        # entries — make sure that path produces a stable digest rather
        # than crashing.
        files = {"a.txt": _entry()}
        skipped_full = [
            {"path": "x", "reason": "r", "recorded_hash": "h", "recorded_size": 5},
        ]
        skipped_partial = [{"path": "x"}]
        # Different content → different digests, but BOTH must succeed.
        d_full = _compute_total_checksum(files, skipped_files=skipped_full)
        d_partial = _compute_total_checksum(files, skipped_files=skipped_partial)
        assert d_full != d_partial
        assert len(d_full) == len(d_partial) == 64

    def test_digest_matches_explicit_construction(self) -> None:
        """The digest format is documented as the spec — pin it byte-for-byte."""
        files = {"a.txt": _entry("aa" * 32, 3)}
        skipped = [{"path": "v.txt", "reason": "x", "recorded_hash": "bb" * 32, "recorded_size": 7}]
        expected_str = (
            "a.txt\x00" + "aa" * 32 + "\x00" + "3"
            "\n__skipped__"
            "\nv.txt\x00x\x00" + "bb" * 32 + "\x00" + "7"
        )
        expected = hashlib.sha256(expected_str.encode("utf-8")).hexdigest()
        assert _compute_total_checksum(files, skipped_files=skipped) == expected


# ---------------------------------------------------------------------------
# prune_manifest_entries
# ---------------------------------------------------------------------------


class TestPruneManifestEntries:
    """Behaviour of the pruning helper across all early-return guards
    and the happy path that mutates the manifest in place."""

    def test_empty_skipped_set_returns_unchanged(self) -> None:
        manifest = _make_manifest({"a.txt": _entry()})
        before = dict(manifest)
        result = prune_manifest_entries(manifest, set())
        assert result is manifest  # mutated in place / same object
        assert manifest == before

    def test_none_manifest_returns_none(self) -> None:
        # Defensive: a falsy manifest (None) is returned as-is rather
        # than raising. The caller is then responsible for the case.
        assert prune_manifest_entries(None, {"x"}) is None  # type: ignore[arg-type]

    def test_manifest_without_files_key_returns_unchanged(self) -> None:
        bad = {"version": 1}
        result = prune_manifest_entries(bad, {"a.txt"})
        assert result is bad
        assert "skipped_files" not in bad

    def test_skipped_path_not_in_files_does_not_mutate(self) -> None:
        manifest = _make_manifest({"a.txt": _entry()})
        before_checksum = manifest["total_checksum"]
        result = prune_manifest_entries(manifest, {"ghost.txt"})
        assert result is manifest
        assert "a.txt" in manifest["files"]
        assert manifest.get("skipped_files") in (None, [])
        # No removal happened → checksum must NOT be touched.
        assert manifest["total_checksum"] == before_checksum

    def test_single_skipped_path_removed_and_recorded(self) -> None:
        manifest = _make_manifest(
            {
                "a.txt": _entry("aa" * 32, 1),
                "b.txt": _entry("bb" * 32, 2),
            }
        )
        result = prune_manifest_entries(manifest, {"b.txt"})
        assert result is manifest
        # Entry removed
        assert "b.txt" not in manifest["files"]
        assert "a.txt" in manifest["files"]
        # Skipped list populated with full provenance
        assert len(manifest["skipped_files"]) == 1
        rec = manifest["skipped_files"][0]
        assert rec["path"] == "b.txt"
        assert rec["reason"] == "vanished_during_write"
        assert rec["recorded_hash"] == "bb" * 32
        assert rec["recorded_size"] == 2

    def test_checksum_recomputed_after_pruning(self) -> None:
        manifest = _make_manifest({"a.txt": _entry("aa" * 32, 1), "b.txt": _entry("bb" * 32, 2)})
        before = manifest["total_checksum"]
        prune_manifest_entries(manifest, {"b.txt"})
        after = manifest["total_checksum"]
        assert after != before
        # The recomputed checksum BINDS the skipped_files list, so we
        # can verify it equals the explicit recompute.
        expected = _compute_total_checksum(
            manifest["files"], skipped_files=manifest["skipped_files"]
        )
        assert after == expected

    def test_multiple_skipped_paths_all_removed(self) -> None:
        manifest = _make_manifest(
            {
                "a.txt": _entry(),
                "b.txt": _entry(),
                "c.txt": _entry(),
            }
        )
        prune_manifest_entries(manifest, {"a.txt", "c.txt"})
        assert set(manifest["files"].keys()) == {"b.txt"}
        recorded_paths = {e["path"] for e in manifest["skipped_files"]}
        assert recorded_paths == {"a.txt", "c.txt"}

    def test_pre_existing_skipped_entries_are_preserved(self) -> None:
        # If the manifest was already pruned earlier in the pipeline,
        # a second pass must APPEND new entries, not overwrite the list.
        manifest = _make_manifest({"a.txt": _entry(), "b.txt": _entry()})
        manifest["skipped_files"] = [
            {
                "path": "earlier.txt",
                "reason": "earlier_reason",
                "recorded_hash": "ff" * 32,
                "recorded_size": 99,
            }
        ]
        prune_manifest_entries(manifest, {"b.txt"})
        recorded_paths = [e["path"] for e in manifest["skipped_files"]]
        assert recorded_paths == ["earlier.txt", "b.txt"]

    def test_skipped_entry_missing_hash_or_size_uses_defaults(self) -> None:
        # The manifest's ``files`` dict MAY contain incomplete entries
        # (e.g. an older format). Pruning must not crash; the recorded
        # entry simply uses empty/zero defaults.
        manifest = _make_manifest({"oddly_shaped.txt": {}})
        prune_manifest_entries(manifest, {"oddly_shaped.txt"})
        rec = manifest["skipped_files"][0]
        assert rec["recorded_hash"] == ""
        assert rec["recorded_size"] == 0

    def test_returns_same_object_for_chaining(self) -> None:
        # The docstring promises the same dict (mutated in place) is
        # returned — pin that contract because callers chain on it.
        manifest = _make_manifest({"a.txt": _entry()})
        assert prune_manifest_entries(manifest, set()) is manifest
        assert prune_manifest_entries(manifest, {"a.txt"}) is manifest

    def test_skipped_paths_not_in_files_with_one_match_still_recorded(self) -> None:
        # Mixed input: one path matches, one doesn't — the matching
        # one is removed and recorded, the other is silently ignored.
        manifest = _make_manifest({"a.txt": _entry()})
        prune_manifest_entries(manifest, {"a.txt", "ghost.txt"})
        recorded_paths = {e["path"] for e in manifest["skipped_files"]}
        assert recorded_paths == {"a.txt"}
