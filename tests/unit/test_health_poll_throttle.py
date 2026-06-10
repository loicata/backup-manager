"""Regression tests for health-poll cost controls (audit 2026-06-10).

The 60 s 24/7 full-SSH health poll produced ~91% of the log and risked
Winsock buffer exhaustion. The poll now: pauses while the window is in
the tray, probes remote backends only every Nth tick, and logs
destination state TRANSITIONS instead of every probe.

Duck-typed against the app methods — no real Tk root.
"""

from types import SimpleNamespace
from unittest.mock import Mock

from src.core.config import StorageConfig, StorageType
from src.ui.app import _REMOTE_POLL_EVERY_N_TICKS, BackupManagerApp


class _FakeRoot:
    """Minimal Tk root stub: records after() calls, lets us drive ticks."""

    def __init__(self, state="normal"):
        self._state = state
        self.after_calls = []

    def after(self, ms, func, *args):
        self.after_calls.append((ms, func, args))

    def state(self):
        return self._state


def _local(label="Storage"):
    return (StorageConfig(storage_type=StorageType.LOCAL, destination_path="D:/B"), label)


def _sftp(label="Mirror 1"):
    return (StorageConfig(storage_type=StorageType.SFTP, sftp_host="h"), label)


def _app(state="normal", configs=None):
    root = _FakeRoot(state=state)
    fake = SimpleNamespace(
        root=root,
        _health_poll_generation=1,
        _health_configs=configs if configs is not None else {0: _local(), 1: _sftp()},
        _health_poll_tick=0,
        _spawned=[],
    )

    def _check_single(index, config, label):
        fake._spawned.append(label)

    # Patch the thread spawn to record synchronously instead of starting threads.
    fake._check_single_destination = _check_single
    fake._is_window_hidden = lambda: BackupManagerApp._is_window_hidden(fake)
    # Referenced as the re-arm callback in root.after(...); never invoked here.
    fake._poll_health = lambda *a: None
    return fake


class TestPollThrottle:
    def _run_poll(self, fake):
        # threading.Thread(target=self._check_single_destination, ...) —
        # intercept by monkeypatching the bound method via the class call.
        import src.ui.app as appmod

        real_thread = appmod.threading.Thread

        def _fake_thread(target=None, args=(), **kw):
            t = SimpleNamespace()
            t.start = lambda target=target, args=args: target(*args)
            return t

        appmod.threading.Thread = _fake_thread
        try:
            BackupManagerApp._poll_health(fake, 1)
        finally:
            appmod.threading.Thread = real_thread

    def test_paused_in_tray_spawns_nothing(self):
        fake = _app(state="withdrawn")
        self._run_poll(fake)
        assert fake._spawned == []
        # Timer still re-armed so polling resumes when the window reopens.
        assert any(call[1] for call in fake.root.after_calls)

    def test_local_probed_every_tick_remote_throttled(self):
        fake = _app(state="normal")

        # First tick: tick becomes 1 → remote NOT probed (1 % 5 != 0).
        self._run_poll(fake)
        assert "Storage" in fake._spawned
        assert "Mirror 1" not in fake._spawned

        # Advance to the Nth tick → remote probed.
        fake._spawned.clear()
        fake._health_poll_tick = _REMOTE_POLL_EVERY_N_TICKS - 1
        self._run_poll(fake)
        assert "Storage" in fake._spawned
        assert "Mirror 1" in fake._spawned

    def test_generation_mismatch_stops(self):
        fake = _app(state="normal")
        BackupManagerApp._poll_health(fake, 999)  # stale generation
        assert fake._spawned == []
        assert fake.root.after_calls == []  # not re-armed


class TestStateTransitionLogging:
    def _fake_app(self):
        fake = SimpleNamespace(
            _active_engines={},
            _backup_running=False,
            _launch_in_progress=False,
            _last_health_online={},
            tab_run=SimpleNamespace(after=Mock()),
        )
        fake._a_backup_is_active = lambda: BackupManagerApp._a_backup_is_active(fake)
        return fake

    def test_offline_transition_logs_warning(self, caplog):
        fake = self._fake_app()
        fake._last_health_online = {0: True}
        health = SimpleNamespace(online=False, error="drive unplugged", label="Storage")

        with caplog.at_level("WARNING", logger="src.ui.app"):
            BackupManagerApp._on_health_result(fake, 0, health)

        assert any("went offline" in r.getMessage() for r in caplog.records)
        assert fake._last_health_online[0] is False

    def test_no_log_when_state_unchanged(self, caplog):
        fake = self._fake_app()
        fake._last_health_online = {0: True}
        health = SimpleNamespace(online=True, error="", label="Storage")

        with caplog.at_level("INFO", logger="src.ui.app"):
            BackupManagerApp._on_health_result(fake, 0, health)

        msgs = [r.getMessage() for r in caplog.records]
        assert not any("offline" in m or "back online" in m for m in msgs)

    def test_recovery_transition_logs_info(self, caplog):
        fake = self._fake_app()
        fake._last_health_online = {0: False}
        health = SimpleNamespace(online=True, error="", label="Mirror 1")

        with caplog.at_level("INFO", logger="src.ui.app"):
            BackupManagerApp._on_health_result(fake, 0, health)

        assert any("back online" in r.getMessage() for r in caplog.records)
        assert fake._last_health_online[0] is True
