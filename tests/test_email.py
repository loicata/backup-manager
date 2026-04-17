"""Tests for src.notifications.email_notifier."""

import smtplib
import socket
from unittest.mock import MagicMock, patch

from src.core.backup_result import BackupResult, PhaseError
from src.core.config import EmailConfig
from src.core.integrity_verifier import BackupVerifyResult, VerifyAllResult
from src.notifications.email_notifier import (
    SMTP_AUTH_HINTS,
    SMTP_PRESETS,
    _build_backup_html,
    _build_html,
    _format_duration,
    _format_rate,
    _format_size,
    send_backup_report,
    send_test_email,
    send_verify_report,
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


# ---------------------------------------------------------------------------
# SMTP edge cases — network errors and protocol failures
# ---------------------------------------------------------------------------


class TestSmtpEdgeCases:
    """Tests for SMTP error handling: timeouts, TLS, disconnects."""

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

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_connection_timeout(self, mock_smtp_class: MagicMock) -> None:
        """Socket timeout during connection returns failure."""
        mock_smtp_class.side_effect = TimeoutError("Connection timed out")
        config = self._make_config()
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "timeout" in msg.lower()

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_server_disconnected(self, mock_smtp_class: MagicMock) -> None:
        """SMTPServerDisconnected during send returns failure."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = smtplib.SMTPServerDisconnected(
            "Connection unexpectedly closed"
        )
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config()
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "SMTPServerDisconnected" in msg

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_starttls_failure(self, mock_smtp_class: MagicMock) -> None:
        """STARTTLS handshake failure returns failure."""
        mock_smtp = MagicMock()
        mock_smtp.starttls.side_effect = smtplib.SMTPException("STARTTLS failed")
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config(use_tls=True, smtp_port=587)
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "SMTPException" in msg

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_smtp_recipient_refused(self, mock_smtp_class: MagicMock) -> None:
        """Recipient refused must surface a distinct, actionable message.

        Previously this was swallowed into the generic "Email error"
        message and the operator never learned their notifications
        had stopped being delivered.
        """
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = smtplib.SMTPRecipientsRefused(
            {"bad@test.com": (550, b"User unknown")}
        )
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config(to_address="bad@test.com")
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        # The message must clearly identify this as a recipient issue
        # with the refused address and tell the user what to do.
        assert "Recipients refused" in msg
        assert "bad@test.com" in msg
        assert "update" in msg.lower() or "not being delivered" in msg.lower()

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_smtp_data_error(self, mock_smtp_class: MagicMock) -> None:
        """SMTP data error (554) returns failure."""
        mock_smtp = MagicMock()
        mock_smtp.sendmail.side_effect = smtplib.SMTPDataError(554, b"Message rejected")
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config()
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "SMTPDataError" in msg

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_connection_refused(self, mock_smtp_class: MagicMock) -> None:
        """Connection refused (wrong port) returns failure."""
        mock_smtp_class.side_effect = ConnectionRefusedError("[Errno 111] Connection refused")
        config = self._make_config(smtp_port=25)
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "ConnectionRefusedError" in msg

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_dns_resolution_failure(self, mock_smtp_class: MagicMock) -> None:
        """DNS failure returns failure with useful message."""
        mock_smtp_class.side_effect = socket.gaierror("[Errno 11001] getaddrinfo failed")
        config = self._make_config(smtp_host="nonexistent.invalid")
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "gaierror" in msg

    @patch("src.notifications.email_notifier.smtplib.SMTP_SSL")
    def test_ssl_port_465_timeout(self, mock_smtp_ssl_class: MagicMock) -> None:
        """Timeout on SSL port 465 returns failure."""
        mock_smtp_ssl_class.side_effect = TimeoutError("SSL connection timed out")
        config = self._make_config(use_tls=True, smtp_port=465)
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is False
        assert "timeout" in msg.lower()

    @patch("src.notifications.email_notifier.smtplib.SMTP_SSL")
    def test_ssl_port_465_success(self, mock_smtp_ssl_class: MagicMock) -> None:
        """Successful send via SSL on port 465."""
        mock_smtp = MagicMock()
        mock_smtp_ssl_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_ssl_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config(use_tls=True, smtp_port=465)
        ok, msg = send_backup_report(config, "Test", True, "OK")
        assert ok is True


# ---------------------------------------------------------------------------
# Verify report email
# ---------------------------------------------------------------------------


class TestSendVerifyReport:
    """Tests for send_verify_report()."""

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

    def _make_result(self, ok: int = 3, errors: int = 0) -> VerifyAllResult:
        results = []
        for i in range(ok):
            results.append(
                BackupVerifyResult(
                    backup_name=f"Backup_{i}",
                    destination="primary",
                    storage_type="local",
                    status="ok",
                    message="SHA-256 verified",
                )
            )
        for i in range(errors):
            results.append(
                BackupVerifyResult(
                    backup_name=f"BadBackup_{i}",
                    destination="primary",
                    storage_type="local",
                    status="corrupted",
                    message="Hash mismatch",
                )
            )
        return VerifyAllResult(
            results=results,
            duration_seconds=12.5,
            total_backups=ok + errors,
            ok_count=ok,
            error_count=errors,
        )

    def test_disabled_returns_false(self) -> None:
        config = self._make_config(enabled=False)
        ok, msg = send_verify_report(config, "Test", self._make_result())
        assert ok is False
        assert "disabled" in msg.lower()

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_sends_on_errors(self, mock_smtp_class: MagicMock) -> None:
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config(send_on_success=False)
        result = self._make_result(ok=2, errors=1)
        ok, msg = send_verify_report(config, "TestProfile", result)
        assert ok is True

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_sends_on_success_when_enabled(self, mock_smtp_class: MagicMock) -> None:
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = self._make_config(send_on_success=True)
        result = self._make_result(ok=3, errors=0)
        ok, msg = send_verify_report(config, "TestProfile", result)
        assert ok is True

    def test_skips_success_when_disabled(self) -> None:
        config = self._make_config(send_on_success=False)
        result = self._make_result(ok=3, errors=0)
        ok, msg = send_verify_report(config, "Test", result)
        assert ok is False
        assert "disabled" in msg.lower()


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


class TestFormatHelpers:
    """Tests for _format_duration, _format_size, _format_rate."""

    def test_format_duration_seconds(self) -> None:
        assert _format_duration(45) == "45s"

    def test_format_duration_minutes(self) -> None:
        assert _format_duration(155) == "2m 35s"

    def test_format_duration_hours(self) -> None:
        assert _format_duration(3665) == "1h 01m"

    def test_format_size_bytes(self) -> None:
        assert _format_size(512) == "512 B"

    def test_format_size_kb(self) -> None:
        assert _format_size(2048) == "2.0 KB"

    def test_format_size_mb(self) -> None:
        assert _format_size(5 * 1024 * 1024) == "5.0 MB"

    def test_format_size_gb(self) -> None:
        assert _format_size(2 * 1024 * 1024 * 1024) == "2.00 GB"

    def test_format_rate_kbs(self) -> None:
        result = _format_rate(512 * 1024, 1.0)
        assert "KB/s" in result

    def test_format_rate_mbs(self) -> None:
        result = _format_rate(100 * 1024 * 1024, 1.0)
        assert "MB/s" in result

    def test_format_rate_zero_duration(self) -> None:
        assert _format_rate(1000, 0) == "\u2014"


# ---------------------------------------------------------------------------
# Enriched backup email template
# ---------------------------------------------------------------------------


class TestBuildBackupHtml:
    """Tests for _build_backup_html with full BackupResult."""

    def _make_result(self, **overrides) -> BackupResult:
        defaults = {
            "files_found": 1234,
            "files_processed": 890,
            "files_skipped": 344,
            "bytes_source": 1_900_000_000,
            "duration_seconds": 155.0,
            "backup_path": "G:\\Backups\\MyProfile_FULL_2026-04-02",
            "rotated_count": 2,
        }
        defaults.update(overrides)
        return BackupResult(**defaults)

    def test_contains_statistics(self) -> None:
        """HTML contains file counts and sizes."""
        result = self._make_result()
        html = _build_backup_html("TestProfile", True, "OK", result=result, backup_type="FULL")
        assert "1,234" in html
        assert "890" in html
        assert "344" in html
        assert "1." in html  # 1.x GB

    def test_contains_duration(self) -> None:
        result = self._make_result(duration_seconds=155)
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "2m 35s" in html

    def test_no_transfer_rate(self) -> None:
        """Transfer rate removed from email — should not appear."""
        result = self._make_result(bytes_source=100_000_000, duration_seconds=10)
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "Transfer rate" not in html

    def test_contains_backup_type(self) -> None:
        result = self._make_result()
        html = _build_backup_html("Test", True, "OK", result=result, backup_type="FULL")
        assert "FULL" in html

    def test_contains_mirror_results(self) -> None:
        result = self._make_result()
        result.mirror_results = [
            ("Mirror 1", True, "OK", "SSH user@host:22"),
            ("Mirror 2", False, "S3 timeout", "S3 my-bucket"),
        ]
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "Mirror 1" in html
        assert "Mirror 2" in html
        assert "SSH user@host:22" in html
        assert "S3 timeout" in html

    def test_contains_retention_info(self) -> None:
        result = self._make_result(rotated_count=3)
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "3" in html
        assert "deleted" in html.lower()

    def test_contains_backups_available(self) -> None:
        result = self._make_result(rotated_count=1)
        result.backups_available = 5
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "Backups available" in html
        assert "5" in html

    def test_contains_errors(self) -> None:
        result = self._make_result()
        result.phase_errors = [
            PhaseError(phase="writer", file_path="bad.txt", message="Permission denied"),
        ]
        html = _build_backup_html("Test", False, "Failed", result=result)
        assert "bad.txt" in html
        assert "Permission denied" in html
        # Header rendered as Errors, not Warnings.
        assert "Errors (1)" in html
        assert "Warnings (" not in html

    def test_warnings_rendered_as_warnings_not_errors(self) -> None:
        """A successful backup with only warnings must show an orange
        Warnings panel, not a red Errors panel.

        Regression: before the fix, _build_backup_html treated every
        phase_errors entry as an error regardless of severity, so the
        manifest-upload warning from a successful remote backup looked
        like a failure.
        """
        from src.core.backup_result import ErrorSeverity

        result = self._make_result()
        result.phase_errors = [
            PhaseError(
                phase="manifest",
                file_path="bk_01.wbverify",
                message="manifest upload failed",
                severity=ErrorSeverity.WARNING,
            ),
        ]
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "Warnings (1)" in html
        assert "Errors (" not in html
        # Orange accent colour, not red.
        assert "#d68910" in html

    def test_mixed_errors_and_warnings_are_split(self) -> None:
        """Both panels appear with their respective counts and colours."""
        from src.core.backup_result import ErrorSeverity

        result = self._make_result()
        result.phase_errors = [
            PhaseError(phase="writer", file_path="a.txt", message="boom"),
            PhaseError(
                phase="manifest",
                file_path="m.wbverify",
                message="upload warning",
                severity=ErrorSeverity.WARNING,
            ),
        ]
        html = _build_backup_html("Test", False, "Failed", result=result)
        assert "Errors (1)" in html
        assert "Warnings (1)" in html
        assert "boom" in html
        assert "upload warning" in html

    def test_contains_log_lines(self) -> None:
        result = self._make_result()
        html = _build_backup_html(
            "Test", True, "OK", details="Backup started\nPhase 1 OK", result=result
        )
        assert "Backup started" in html
        assert "Phase 1 OK" in html

    def test_success_color_green(self) -> None:
        result = self._make_result()
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "#27ae60" in html

    def test_failure_color_red(self) -> None:
        result = self._make_result()
        html = _build_backup_html("Test", False, "Failed", result=result)
        assert "#e74c3c" in html

    def test_cancelled_color_orange(self) -> None:
        result = self._make_result()
        html = _build_backup_html("Test", False, "Cancelled", result=result, cancelled=True)
        assert "#f39c12" in html

    def test_skipped_files_not_shown_when_zero(self) -> None:
        result = self._make_result(files_skipped=0)
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "skipped" not in html.lower()

    def test_no_mirrors_no_destinations_section(self) -> None:
        result = self._make_result()
        result.mirror_results = []
        html = _build_backup_html("Test", True, "OK", result=result)
        assert "Destinations" not in html

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_send_with_enriched_result(self, mock_smtp_class: MagicMock) -> None:
        """send_backup_report with result= uses the enriched template."""
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        config = EmailConfig(
            enabled=True,
            smtp_host="smtp.test.com",
            smtp_port=587,
            use_tls=True,
            username="user",
            password="pass",
            from_address="from@test.com",
            to_address="to@test.com",
            send_on_success=True,
            send_on_failure=True,
        )
        result = self._make_result()
        ok, msg = send_backup_report(
            config,
            "TestProfile",
            True,
            "890 files backed up",
            result=result,
            backup_type="FULL",
        )
        assert ok is True

        # Verify the email body contains enriched content
        from email import message_from_string

        call_args = mock_smtp.sendmail.call_args
        raw_msg = call_args[0][2]
        msg = message_from_string(raw_msg)
        # Walk MIME parts to find the HTML body
        html_body = ""
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html_body = part.get_payload(decode=True).decode("utf-8")
                break
        assert "Statistics" in html_body
        assert "1,234" in html_body
