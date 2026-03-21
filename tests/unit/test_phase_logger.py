"""Tests for src.core.phase_logger — unified pipeline phase logging."""

from src.core.events import LOG, PROGRESS, EventBus
from src.core.phase_logger import PhaseLogger


class TestPhaseLoggerInfo:
    """Tests for the info() method."""

    def test_info_emits_log_event_with_info_level(self) -> None:
        """info() emits a LOG event with level='info'."""
        events = EventBus()
        received = []
        events.subscribe(LOG, lambda **kw: received.append(kw))

        logger = PhaseLogger("collector", events)
        logger.info("test message")

        assert len(received) == 1
        assert received[0]["message"] == "test message"
        assert received[0]["level"] == "info"

    def test_info_includes_phase_name_in_event(self) -> None:
        """info() includes the phase name in the emitted event."""
        events = EventBus()
        received = []
        events.subscribe(LOG, lambda **kw: received.append(kw))

        logger = PhaseLogger("filter", events)
        logger.info("filtering done")

        assert received[0]["phase"] == "filter"

    def test_info_without_events_does_not_crash(self) -> None:
        """info() with events=None works without error."""
        logger = PhaseLogger("collector", events=None)
        logger.info("no crash")  # Should not raise


class TestPhaseLoggerWarning:
    """Tests for the warning() method."""

    def test_warning_emits_log_event_with_warning_level(self) -> None:
        """warning() emits a LOG event with level='warning'."""
        events = EventBus()
        received = []
        events.subscribe(LOG, lambda **kw: received.append(kw))

        logger = PhaseLogger("encryptor", events)
        logger.warning("disk almost full")

        assert len(received) == 1
        assert received[0]["message"] == "disk almost full"
        assert received[0]["level"] == "warning"

    def test_warning_includes_phase_name(self) -> None:
        """warning() includes phase name."""
        events = EventBus()
        received = []
        events.subscribe(LOG, lambda **kw: received.append(kw))

        logger = PhaseLogger("rotator", events)
        logger.warning("old backup")

        assert received[0]["phase"] == "rotator"


class TestPhaseLoggerError:
    """Tests for the error() method."""

    def test_error_emits_log_event_with_error_level(self) -> None:
        """error() emits a LOG event with level='error'."""
        events = EventBus()
        received = []
        events.subscribe(LOG, lambda **kw: received.append(kw))

        logger = PhaseLogger("writer", events)
        logger.error("write failed")

        assert len(received) == 1
        assert received[0]["message"] == "write failed"
        assert received[0]["level"] == "error"

    def test_error_without_events_does_not_crash(self) -> None:
        """error() with events=None works without error."""
        logger = PhaseLogger("writer", events=None)
        logger.error("no crash")


class TestPhaseLoggerProgress:
    """Tests for the progress() method."""

    def test_progress_emits_progress_event(self) -> None:
        """progress() emits a PROGRESS event with correct data."""
        events = EventBus()
        received = []
        events.subscribe(PROGRESS, lambda **kw: received.append(kw))

        logger = PhaseLogger("writer", events)
        logger.progress(current=5, total=10, filename="test.txt", phase="backup")

        assert len(received) == 1
        assert received[0]["current"] == 5
        assert received[0]["total"] == 10
        assert received[0]["filename"] == "test.txt"
        assert received[0]["phase"] == "backup"

    def test_progress_without_events_does_not_crash(self) -> None:
        """progress() with events=None works without error."""
        logger = PhaseLogger("writer", events=None)
        logger.progress(current=1, total=1, filename="x.txt", phase="backup")


class TestPhaseLoggerConstruction:
    """Tests for PhaseLogger construction."""

    def test_different_phase_names(self) -> None:
        """PhaseLogger can be created with any phase name."""
        events = EventBus()
        received = []
        events.subscribe(LOG, lambda **kw: received.append(kw))

        for name in [
            "collector",
            "filter",
            "manifest",
            "writer",
            "verifier",
            "encryptor",
            "mirror",
            "rotator",
            "remote_writer",
        ]:
            logger = PhaseLogger(name, events)
            logger.info(f"hello from {name}")
            assert received[-1]["phase"] == name
