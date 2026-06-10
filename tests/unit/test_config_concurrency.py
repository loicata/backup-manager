"""Regression tests for ConfigManager thread-safety and .bak recovery.

Covers two audit findings (2026-06-10, medium):
    - save_profile raced itself from three threads through ONE
      deterministic .json.tmp name → torn file / PermissionError.
      Fixed by the manager-wide ``_io_lock`` in ``_atomic_write``.
    - .bak recovery clobbered the live profile file with a non-atomic
      ``shutil.copy2`` and never re-checked whether a concurrent save
      had already fixed the file. Fixed by
      ``ConfigManager._recover_profile_from_bak`` (.tmp + os.replace,
      TOCTOU re-parse).
"""

import json
import threading

from src.core.config import BackupProfile, ConfigManager


def _profile_path(mgr: ConfigManager, profile: BackupProfile):
    return mgr.profiles_dir / f"{profile.id}.json"


class TestConcurrentSaves:
    def test_concurrent_saves_same_profile_no_corruption(self, tmp_config_dir):
        """Hammer one profile from 4 threads: the final file must be
        complete valid JSON and no saver may raise (pre-fix, the shared
        .json.tmp produced PermissionError / torn payloads)."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Hammered")
        mgr.save_profile(profile)

        errors: list[BaseException] = []

        def _save_many(tag: int) -> None:
            try:
                for i in range(25):
                    profile.name = f"Hammered-{tag}-{i}"
                    mgr.save_profile(profile)
            except BaseException as exc:  # noqa: BLE001 — collected for assert
                errors.append(exc)

        threads = [threading.Thread(target=_save_many, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent saves raised: {errors!r}"

        raw = _profile_path(mgr, profile).read_text(encoding="utf-8")
        data = json.loads(raw)  # must parse — no torn write
        assert data["name"].startswith("Hammered-")
        assert list(mgr.profiles_dir.glob("*.json.tmp")) == []

    def test_concurrent_saves_distinct_profiles(self, tmp_config_dir):
        """Different profiles saved in parallel all survive intact."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profiles = [BackupProfile(name=f"P{i}") for i in range(4)]
        errors: list[BaseException] = []

        def _save(p: BackupProfile) -> None:
            try:
                for _ in range(10):
                    mgr.save_profile(p)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_save, args=(p,)) for p in profiles]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        loaded = mgr.get_all_profiles()
        assert {p.name for p in loaded} == {"P0", "P1", "P2", "P3"}


class TestRecoverFromBak:
    def _make_profile_with_bak(self, tmp_config_dir):
        """Save twice so the .bak holds 'Old' and the live file 'New'."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Old")
        mgr.save_profile(profile)
        profile.name = "New"
        mgr.save_profile(profile)  # copies 'Old' → .bak, writes 'New'
        return mgr, profile

    def test_recover_prefers_concurrently_fixed_live_file(self, tmp_config_dir):
        """If the live file parses by the time recovery runs (a
        concurrent save fixed it), the stale .bak must NOT clobber it."""
        mgr, profile = self._make_profile_with_bak(tmp_config_dir)
        path = _profile_path(mgr, profile)

        recovered = mgr._recover_profile_from_bak(path)

        assert recovered is not None
        assert recovered.name == "New"  # live file won, not the 'Old' bak
        on_disk = json.loads(path.read_text(encoding="utf-8"))
        assert on_disk["name"] == "New"  # file untouched

    def test_recover_restores_bak_atomically(self, tmp_config_dir):
        """Corrupted live file + valid .bak → profile recovered, live
        file rewritten with the .bak payload, no .tmp leftover."""
        mgr, profile = self._make_profile_with_bak(tmp_config_dir)
        path = _profile_path(mgr, profile)
        path.write_text("{corrupted", encoding="utf-8")

        recovered = mgr._recover_profile_from_bak(path)

        assert recovered is not None
        assert recovered.name == "Old"
        on_disk = json.loads(path.read_text(encoding="utf-8"))
        assert on_disk["name"] == "Old"
        assert list(mgr.profiles_dir.glob("*.json.tmp")) == []

    def test_recover_returns_none_when_bak_unusable(self, tmp_config_dir):
        """Corrupted live file + corrupted .bak → None, no crash."""
        mgr, profile = self._make_profile_with_bak(tmp_config_dir)
        path = _profile_path(mgr, profile)
        bak = path.with_suffix(".json.bak")
        path.write_text("{corrupted", encoding="utf-8")
        bak.write_text("{also corrupted", encoding="utf-8")

        assert mgr._recover_profile_from_bak(path) is None

    def test_recover_returns_none_without_bak(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Lonely")
        mgr.save_profile(profile)  # single save → no .bak yet
        path = _profile_path(mgr, profile)
        path.write_text("{corrupted", encoding="utf-8")

        assert mgr._recover_profile_from_bak(path) is None

    def test_get_all_profiles_uses_recovery(self, tmp_config_dir):
        """End-to-end: corrupted live file is recovered transparently."""
        mgr, profile = self._make_profile_with_bak(tmp_config_dir)
        _profile_path(mgr, profile).write_text("XXX", encoding="utf-8")

        profiles = mgr.get_all_profiles()

        assert [p.name for p in profiles] == ["Old"]


class TestBakNotRefreshedFromCorruptMain:
    """Audit L4/#5: a corrupt main file must never overwrite a good .bak."""

    def test_save_with_corrupt_main_keeps_good_bak(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="V1")
        mgr.save_profile(profile)
        profile.name = "V2"
        mgr.save_profile(profile)  # .bak now holds the good 'V1'

        path = _profile_path(mgr, profile)
        bak = path.with_suffix(".json.bak")
        # Corrupt the live file, then save again.
        path.write_text("{ corrupt main", encoding="utf-8")
        profile.name = "V3"
        mgr.save_profile(profile)

        # .bak must still be valid JSON (NOT the corrupt main).
        data = json.loads(bak.read_text(encoding="utf-8"))
        assert data["name"] == "V1"
        # And the live file is the new good payload.
        assert json.loads(path.read_text(encoding="utf-8"))["name"] == "V3"

    def test_file_parses_helper(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        good = tmp_config_dir / "good.json"
        good.write_text('{"a": 1}', encoding="utf-8")
        bad = tmp_config_dir / "bad.json"
        bad.write_text("{ nope", encoding="utf-8")
        assert mgr._file_parses_as_json(good) is True
        assert mgr._file_parses_as_json(bad) is False
        assert mgr._file_parses_as_json(tmp_config_dir / "absent.json") is False


class TestQuarantineCorruptProfile:
    """Audit L4/#4: when BOTH main and .bak are unparseable, the file is
    quarantined to .json.broken (preserved, removed from the active set,
    stops re-erroring) instead of silently re-failing every load."""

    def test_double_corruption_quarantines(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Doomed")
        mgr.save_profile(profile)
        mgr.save_profile(profile)  # create .bak
        path = _profile_path(mgr, profile)
        bak = path.with_suffix(".json.bak")
        path.write_text("{ corrupt", encoding="utf-8")
        bak.write_text("{ also corrupt", encoding="utf-8")

        profiles = mgr.get_all_profiles()

        assert profiles == []
        assert not path.exists()  # moved aside
        broken = path.with_suffix(".json.broken")
        assert broken.exists()
        assert broken.read_text(encoding="utf-8") == "{ corrupt"

    def test_quarantined_file_not_reloaded_next_time(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Doomed")
        mgr.save_profile(profile)
        mgr.save_profile(profile)
        path = _profile_path(mgr, profile)
        path.with_suffix(".json.bak").write_text("{ bad", encoding="utf-8")
        path.write_text("{ bad", encoding="utf-8")

        mgr.get_all_profiles()  # quarantines
        # Second load sees no .json at all → clean empty, no error loop.
        assert mgr.get_all_profiles() == []
        assert list(mgr.profiles_dir.glob("*.json")) == []
