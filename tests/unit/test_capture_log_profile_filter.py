"""Regression test: per-run log capture must not cross-contaminate
between two engines sharing one EventBus (audit 2026-06-10, medium).

Before the fix, every engine's _capture_log was subscribed to the
shared bus and appended EVERY LOG line — so a concurrent run's
"Backup complete" landed in another run's log file and the History
tab misclassified the failed run as successful.
"""

from src.core.backup_engine import BackupEngine
from src.core.backup_result import BackupResult
from src.core.events import LOG, EventBus, ProfileTaggingEventBus


def _engine_capturing(bus: EventBus, profile_id: str) -> BackupEngine:
    """An engine wired to capture only ``profile_id``'s LOG lines."""
    eng = BackupEngine.__new__(BackupEngine)
    eng._events = bus
    eng._current_result = BackupResult()
    eng._run_profile_id = profile_id
    bus.subscribe(LOG, eng._capture_log)
    return eng


class TestCaptureLogProfileFilter:
    def test_foreign_tagged_lines_are_dropped(self):
        bus = EventBus()
        eng_a = _engine_capturing(bus, "profile-A")
        eng_b = _engine_capturing(bus, "profile-B")

        # Each engine emits through its own tagging bus (as run_backup does).
        bus_a = ProfileTaggingEventBus(bus, "profile-A")
        bus_b = ProfileTaggingEventBus(bus, "profile-B")

        bus_a.emit(LOG, message="A: collecting files")
        bus_b.emit(LOG, message="B: Backup complete")
        bus_a.emit(LOG, message="A: Backup complete")

        assert eng_a._current_result.log_lines == [
            "A: collecting files",
            "A: Backup complete",
        ]
        assert eng_b._current_result.log_lines == ["B: Backup complete"]

    def test_untagged_lines_are_kept(self):
        """Backward compat: a raw emit with no profile_id is captured."""
        bus = EventBus()
        eng = _engine_capturing(bus, "profile-A")

        bus.emit(LOG, message="legacy untagged line")

        assert eng._current_result.log_lines == ["legacy untagged line"]

    def test_capture_before_run_keeps_everything(self):
        """If _run_profile_id is unset (engine idle), nothing is dropped."""
        bus = EventBus()
        eng = _engine_capturing(bus, "profile-A")
        eng._run_profile_id = None

        ProfileTaggingEventBus(bus, "other").emit(LOG, message="x")

        assert eng._current_result.log_lines == ["x"]
