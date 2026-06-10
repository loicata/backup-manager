"""Regression tests for audit L11:

- #15 ConfigManager.delete_verify_hash prunes the encrypted-archive
  reference store when a backup is rotated (the rotator removed the
  archive + sidecars but never the verify_hashes entry).
- #16 RunHistoryStore.load opportunistically compacts the on-disk JSONL
  when it exceeds 2× the load cap (the cap was load-only before).
"""

import json

from src.core.config import ConfigManager
from src.core.run_history import RunHistoryStore


class TestDeleteVerifyHash:
    def test_prunes_existing_entry(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_verify_hash("crypter_FULL_2026-06-10.tar.wbenc", "abc", 100)
        assert "crypter_FULL_2026-06-10.tar.wbenc" in mgr.load_verify_hashes()

        mgr.delete_verify_hash("crypter_FULL_2026-06-10.tar.wbenc")

        assert mgr.load_verify_hashes() == {}

    def test_matches_bare_name_against_wbenc_key(self, tmp_config_dir):
        """The rotator knows the backup as the bare name; the store keys
        it with .tar.wbenc. delete_verify_hash must bridge the two."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_verify_hash("crypter_FULL_2026-06-10.tar.wbenc", "abc", 100)

        mgr.delete_verify_hash("crypter_FULL_2026-06-10")  # bare, no suffix

        assert mgr.load_verify_hashes() == {}

    def test_absent_entry_is_noop(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_verify_hash("keep.tar.wbenc", "abc", 100)
        mgr.delete_verify_hash("not-there")
        assert "keep.tar.wbenc" in mgr.load_verify_hashes()

    def test_remaining_entries_still_verify_after_prune(self, tmp_config_dir):
        """Pruning one entry must keep the HMAC envelope valid for the
        rest (the store stays loadable, not silently wiped)."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_verify_hash("a.tar.wbenc", "h1", 10)
        mgr.save_verify_hash("b.tar.wbenc", "h2", 20)

        mgr.delete_verify_hash("a.tar.wbenc")

        remaining = mgr.load_verify_hashes()
        assert set(remaining) == {"b.tar.wbenc"}
        assert remaining["b.tar.wbenc"]["sha256"] == "h2"


class TestRunHistoryCompaction:
    def test_oversized_file_compacted_on_load(self, tmp_path):
        store = RunHistoryStore(tmp_path)
        from src.core.run_history import _MAX_ENTRIES_PER_PROFILE

        pid = "profile-x"
        path = tmp_path / f"{pid}.jsonl"
        # Write 2× cap + 10 lines so load() trips the compaction threshold.
        n = 2 * _MAX_ENTRIES_PER_PROFILE + 10
        with open(path, "w", encoding="utf-8") as f:
            for i in range(n):
                f.write(json.dumps({"message": f"line {i}", "level": "info"}) + "\n")

        with open(path, encoding="utf-8") as f:
            before = sum(1 for _ in f)
        assert before == n

        entries = store.load(pid)

        # Returned tail is capped, and the file was rewritten to the cap.
        assert len(entries) == _MAX_ENTRIES_PER_PROFILE
        with open(path, encoding="utf-8") as f:
            after = sum(1 for _ in f)
        assert after == _MAX_ENTRIES_PER_PROFILE
        # Most-recent entries are retained (tail, not head).
        assert entries[-1]["message"] == f"line {n - 1}"

    def test_small_file_not_rewritten(self, tmp_path):
        store = RunHistoryStore(tmp_path)
        pid = "profile-y"
        path = tmp_path / f"{pid}.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for i in range(5):
                f.write(json.dumps({"message": f"m{i}", "level": "info"}) + "\n")
        mtime_before = path.stat().st_mtime_ns

        store.load(pid)

        # Untouched: under the threshold, no rewrite.
        assert path.stat().st_mtime_ns == mtime_before
