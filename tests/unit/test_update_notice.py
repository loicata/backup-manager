"""Regression test: the GitHub update checker is wired into the app
(audit L6 — it was complete but had zero callers).

Duck-typed against BackupManagerApp's callback methods; no real Tk root.
"""

from types import SimpleNamespace
from unittest.mock import Mock

from src.ui.app import BackupManagerApp


def _fake_app():
    fake = SimpleNamespace(
        _pending_update_url="",
        root=SimpleNamespace(after=Mock()),
        tray=SimpleNamespace(notify=Mock()),
    )
    # _on_update_available passes self._show_update_notice to root.after;
    # the attribute must exist on the stub (it's never invoked here — the
    # Mock'd after() just records it).
    fake._show_update_notice = Mock()
    return fake


class TestUpdateNoticeWiring:
    def test_callback_marshals_to_tk_thread(self):
        fake = _fake_app()
        BackupManagerApp._on_update_available(fake, "3.9.0", "https://example/rel")
        # Must hand off to the Tk loop, not touch widgets on the daemon thread.
        fake.root.after.assert_called_once()
        args = fake.root.after.call_args.args
        # root.after(0, self._show_update_notice, latest, url)
        assert args[0] == 0
        assert args[2] == "3.9.0"
        assert args[3] == "https://example/rel"

    def test_show_notice_records_url_and_notifies(self):
        fake = _fake_app()
        BackupManagerApp._show_update_notice(fake, "3.9.0", "https://example/rel")
        assert fake._pending_update_url == "https://example/rel"
        fake.tray.notify.assert_called_once()
        title, body = fake.tray.notify.call_args.args
        assert "Update" in title
        assert "3.9.0" in body

    def test_show_notice_survives_tray_failure(self):
        fake = _fake_app()
        fake.tray.notify.side_effect = RuntimeError("tray dead")
        # Must not raise even if the tray notification fails.
        BackupManagerApp._show_update_notice(fake, "3.9.0", "https://example/rel")
        assert fake._pending_update_url == "https://example/rel"
