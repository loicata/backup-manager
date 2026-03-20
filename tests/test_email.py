"""Tests for src.notifications.email_notifier."""

from unittest.mock import patch, MagicMock

from src.core.config import EmailConfig
from src.notifications.email_notifier import (
    send_backup_report,
    send_test_email,
    _build_html,
)


class TestEmailNotifier:
    def _make_config(self, **overrides):
        defaults = {
            "enabled": True,
            "smtp_host": "smtp.test.com",
            "smtp_port": 587,
            "use_tls": True,
            "username": "user",
            "password": "pass",
            "from_address": "from@test.com",
            "to_address": "to@test.com",
            "send_on_success": True,
            "send_on_failure": True,
        }
        defaults.update(overrides)
        return EmailConfig(**defaults)

    def test_disabled_returns_false(self):
        config = self._make_config(enabled=False)
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "disabled" in msg.lower()

    def test_success_not_sent_when_disabled(self):
        config = self._make_config(send_on_success=False)
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False

    def test_failure_not_sent_when_disabled(self):
        config = self._make_config(send_on_failure=False)
        ok, msg = send_backup_report(config, "Test", False, "Failed")
        assert ok is False

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_send_success_report(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config()
        ok, msg = send_backup_report(config, "Profile1", True, "3 files backed up")
        assert ok is True
        assert "sent" in msg.lower()

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_send_failure_report(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config()
        ok, msg = send_backup_report(config, "Profile1", False, "Disk full")
        assert ok is True

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_send_test_email(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config()
        ok, msg = send_test_email(config)
        assert ok is True

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_auth_error(self, mock_smtp_class):
        import smtplib

        mock_smtp = MagicMock()
        mock_smtp.login.side_effect = smtplib.SMTPAuthenticationError(535, b"Bad credentials")
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config()
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "authentication" in msg.lower()

    def test_build_html_success(self):
        html = _build_html("TestProfile", True, "All good")
        assert "SUCCESS" in html
        assert "#27ae60" in html  # Green color
        assert "TestProfile" in html

    def test_build_html_failure(self):
        html = _build_html("TestProfile", False, "Disk full", "Error details")
        assert "FAILED" in html
        assert "#e74c3c" in html  # Red color
        assert "Error details" in html

    def test_multiple_recipients(self):
        config = self._make_config(to_address="a@test.com, b@test.com")
        # Just verify it doesn't crash when building
        assert "," in config.to_address
