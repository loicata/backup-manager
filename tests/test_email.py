"""Tests for src.notifications.email_notifier."""

from unittest.mock import MagicMock, patch

from src.core.config import EmailConfig
from src.notifications.email_notifier import (
    SMTP_AUTH_HINTS,
    SMTP_PRESETS,
    _build_html,
    send_backup_report,
    send_test_email,
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

    def test_build_html_cancelled(self):
        html = _build_html("TestProfile", False, "Cancelled", cancelled=True)
        assert "CANCELLED" in html
        assert "#f39c12" in html  # Orange color
        assert "TestProfile" in html

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_send_cancelled_report(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config()
        ok, msg = send_backup_report(
            config,
            "Profile1",
            False,
            "Backup cancelled by user",
            cancelled=True,
        )
        assert ok is True
        assert "sent" in msg.lower()

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_cancelled_subject_contains_cancelled(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config()
        send_backup_report(
            config,
            "Profile1",
            False,
            "Backup cancelled by user",
            cancelled=True,
        )
        # Extract the email and decode the MIME subject
        from email import message_from_string
        from email.header import decode_header

        call_args = mock_smtp.sendmail.call_args
        raw_msg = call_args[0][2]
        msg = message_from_string(raw_msg)
        decoded_parts = decode_header(msg["Subject"])
        subject = "".join(
            part.decode(enc or "utf-8") if isinstance(part, bytes) else part
            for part, enc in decoded_parts
        )
        assert "CANCELLED" in subject

    def test_cancelled_not_sent_when_failure_disabled(self):
        config = self._make_config(send_on_failure=False)
        ok, msg = send_backup_report(
            config,
            "Test",
            False,
            "Cancelled",
            cancelled=True,
        )
        assert ok is False
        assert "disabled" in msg.lower()

    def test_cancelled_sent_when_failure_enabled(self):
        config = self._make_config(send_on_failure=True, send_on_success=False)
        # Should not be blocked — cancelled follows failure trigger
        with patch("src.notifications.email_notifier.smtplib.SMTP") as mock_smtp_class:
            mock_smtp = MagicMock()
            mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
            mock_smtp_class.return_value.__exit__ = lambda s, *a: None
            ok, msg = send_backup_report(
                config,
                "Test",
                False,
                "Cancelled",
                cancelled=True,
            )
            assert ok is True

    def test_auth_hints_exist_for_all_presets(self):
        """Every SMTP preset must have a corresponding auth hint."""
        for provider in SMTP_PRESETS:
            assert provider in SMTP_AUTH_HINTS, f"Missing auth hint for {provider}"
            assert len(SMTP_AUTH_HINTS[provider]) > 0

    def test_gmail_hint_mentions_app_password(self):
        """Gmail hint must mention App Password."""
        assert "App Password" in SMTP_AUTH_HINTS["gmail"]

    def test_protonmail_hint_mentions_bridge(self):
        """ProtonMail hint must mention Bridge."""
        assert "Bridge" in SMTP_AUTH_HINTS["protonmail"]
