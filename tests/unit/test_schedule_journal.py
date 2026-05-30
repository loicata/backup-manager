"""ScheduleJournal: per-profile update_last + terminal-only get_last_run.

Regression for the 30/05/2026 "crypter shows Failed but the backup
actually succeeded" bug. ``update_last`` updated the GLOBAL last entry,
so when two profiles ran back-to-back (crypter → My Backup), crypter's
``success`` (and its 2356 file count) landed on the My Backup row and
crypter's own entry stayed stuck on ``started`` — which the dashboard
card then painted as "Failed".
"""

from src.core.scheduler import ScheduleJournal, ScheduleLogEntry


def _entry(profile_id: str, name: str, status: str, trigger: str = "manual") -> ScheduleLogEntry:
    return ScheduleLogEntry(
        profile_id=profile_id,
        profile_name=name,
        trigger=trigger,
        status=status,
    )


class TestUpdateLastTargetsProfile:
    def test_updates_matching_profile_not_global_last(self, tmp_path):
        """The exact bug: crypter finishes after a My Backup row exists."""
        j = ScheduleJournal(tmp_path)
        j.add(_entry("crypter", "crypter", "started"))
        j.add(_entry("mybackup", "My Backup", "started"))

        j.update_last(profile_id="crypter", status="success", files_count=2356)

        entries = j.get_entries(limit=10)
        crypter = [e for e in entries if e["profile_id"] == "crypter"][-1]
        mybackup = [e for e in entries if e["profile_id"] == "mybackup"][-1]
        # crypter's own row got the success + file count.
        assert crypter["status"] == "success"
        assert crypter["files_count"] == 2356
        # My Backup must be untouched — NOT given crypter's success/counts.
        assert mybackup["status"] == "started"
        assert mybackup.get("files_count", 0) == 0

    def test_unknown_profile_is_noop(self, tmp_path):
        j = ScheduleJournal(tmp_path)
        j.add(_entry("a", "A", "started"))
        j.update_last(profile_id="ghost", status="success")
        assert j.get_entries()[-1]["status"] == "started"  # unchanged

    def test_legacy_no_profile_id_updates_global_last(self, tmp_path):
        j = ScheduleJournal(tmp_path)
        j.add(_entry("a", "A", "started"))
        j.update_last(status="success")  # legacy global fallback
        assert j.get_entries()[-1]["status"] == "success"

    def test_empty_journal_is_safe(self, tmp_path):
        j = ScheduleJournal(tmp_path)
        j.update_last(profile_id="a", status="success")  # no raise
        assert j.get_entries() == []


class TestGetLastRunTerminalOnly:
    def test_skips_orphan_started(self, tmp_path):
        """A trailing 'started' (in-flight or crash orphan) must not hide
        the last finished run behind a "Failed" card."""
        j = ScheduleJournal(tmp_path)
        j.add(_entry("a", "A", "success"))
        j.add(_entry("a", "A", "started"))
        last = j.get_last_run("a")
        assert last is not None
        assert last["status"] == "success"

    def test_skips_verify_and_waiting(self, tmp_path):
        j = ScheduleJournal(tmp_path)
        j.add(_entry("a", "A", "success"))
        j.add(_entry("a", "A", "success", trigger="verify"))  # verify ignored
        j.add(_entry("a", "A", "waiting"))  # retry-waiting ignored
        last = j.get_last_run("a")
        assert last["status"] == "success"
        assert last["trigger"] == "manual"

    def test_returns_none_when_only_started(self, tmp_path):
        j = ScheduleJournal(tmp_path)
        j.add(_entry("a", "A", "started"))
        assert j.get_last_run("a") is None

    def test_returns_failed_terminal(self, tmp_path):
        j = ScheduleJournal(tmp_path)
        j.add(_entry("a", "A", "success"))
        j.add(_entry("a", "A", "failed"))
        assert j.get_last_run("a")["status"] == "failed"

    def test_other_profiles_do_not_leak(self, tmp_path):
        j = ScheduleJournal(tmp_path)
        j.add(_entry("a", "A", "success"))
        j.add(_entry("b", "B", "failed"))
        assert j.get_last_run("a")["status"] == "success"
        assert j.get_last_run("b")["status"] == "failed"
