"""Regression tests for stale .partial cleanup (audit 2026-06-10).

A hard kill mid-upload leaves a ``<name>.partial`` that the sidecar
filter hides from list_backups AND list_orphan_backups — so the orphan
scan never removed it and a 47 GB encrypted run cut by a shutdown
leaked its full size forever. ``LocalStorage.purge_stale_partials``
sweeps them, age-gated so a concurrent run's in-flight partial is safe.
"""

import os
import time

from src.storage.local import LocalStorage


def _make_partial(dest, name: str, age_seconds: float) -> None:
    p = dest / name
    p.write_bytes(b"x" * 16)
    old = time.time() - age_seconds
    os.utime(p, (old, old))


class TestPurgeStalePartials:
    def test_removes_old_partial_for_profile(self, tmp_path):
        dest = tmp_path / "backups"
        dest.mkdir()
        _make_partial(dest, "My_Backup_FULL_2026-06-10_100000.tar.wbenc.partial", age_seconds=7200)

        storage = LocalStorage(str(dest))
        removed = storage.purge_stale_partials("My_Backup_", grace_seconds=3600)

        assert removed == ["My_Backup_FULL_2026-06-10_100000.tar.wbenc.partial"]
        assert list(dest.glob("*.partial")) == []

    def test_keeps_recent_partial_in_flight(self, tmp_path):
        dest = tmp_path / "backups"
        dest.mkdir()
        _make_partial(dest, "My_Backup_FULL_2026-06-10_120000.tar.wbenc.partial", age_seconds=10)

        storage = LocalStorage(str(dest))
        removed = storage.purge_stale_partials("My_Backup_", grace_seconds=3600)

        assert removed == []  # too recent → still being written
        assert len(list(dest.glob("*.partial"))) == 1

    def test_never_touches_other_profiles_partial(self, tmp_path):
        dest = tmp_path / "backups"
        dest.mkdir()
        _make_partial(dest, "Other_Profile_FULL_2026-06-10_100000.partial", age_seconds=99999)

        storage = LocalStorage(str(dest))
        removed = storage.purge_stale_partials("My_Backup_", grace_seconds=3600)

        assert removed == []
        assert len(list(dest.glob("*.partial"))) == 1

    def test_leaves_committed_backups_and_sidecars_alone(self, tmp_path):
        dest = tmp_path / "backups"
        dest.mkdir()
        (dest / "My_Backup_FULL_2026-06-10_100000.tar.wbenc").write_bytes(b"real backup")
        (dest / "My_Backup_FULL_2026-06-10_100000.wbcommit").write_bytes(b"marker")
        _make_partial(dest, "My_Backup_FULL_2026-06-09_100000.tar.wbenc.partial", age_seconds=7200)

        storage = LocalStorage(str(dest))
        removed = storage.purge_stale_partials("My_Backup_", grace_seconds=3600)

        assert removed == ["My_Backup_FULL_2026-06-09_100000.tar.wbenc.partial"]
        assert (dest / "My_Backup_FULL_2026-06-10_100000.tar.wbenc").exists()
        assert (dest / "My_Backup_FULL_2026-06-10_100000.wbcommit").exists()

    def test_empty_prefix_is_a_noop(self, tmp_path):
        dest = tmp_path / "backups"
        dest.mkdir()
        _make_partial(dest, "anything.partial", age_seconds=99999)

        storage = LocalStorage(str(dest))
        assert storage.purge_stale_partials("", grace_seconds=3600) == []
        assert len(list(dest.glob("*.partial"))) == 1

    def test_missing_dest_is_a_noop(self, tmp_path):
        storage = LocalStorage(str(tmp_path / "does_not_exist"))
        assert storage.purge_stale_partials("My_Backup_", grace_seconds=3600) == []
