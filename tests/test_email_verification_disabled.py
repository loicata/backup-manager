"""Tests for the v3.7.0 case 3 ``verification_disabled`` email tag.

Case 3 = scheduled backup that completed in Fast mode (post-copy
verify off) AND no periodic verification is armed. The email
notifier promotes a warning to the subject line and adds an amber
block to the body so the operator notices that no automatic
integrity check confirms this backup.

Contracts pinned here:

1. ``send_backup_report`` adds "verification disabled" to the subject
   only when ``success=True``, ``cancelled=False``, and the flag is on.
2. A FAILED or CANCELLED run never gets the tag (the existing
   failure/cancel marker already screams loudly).
3. ``_build_backup_html`` and ``_build_html`` both insert the amber
   block at the right spot when the flag is on; emit nothing when off.
4. The default value of the kwarg is False (backwards-compat: every
   existing caller continues to produce the v3.6.x subject/body).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.core.backup_result import BackupResult
from src.core.config import EmailConfig
from src.notifications.email_notifier import (
    _build_backup_html,
    _build_html,
    send_backup_report,
)


def _make_config(**overrides) -> EmailConfig:
    """Build a minimal EmailConfig for notifier tests."""
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


def _make_result() -> BackupResult:
    """Build a minimal successful BackupResult for the enriched body."""
    r = BackupResult()
    r.files_processed = 42
    r.files_found = 42
    r.bytes_source = 1_234_567
    r.duration_seconds = 12.3
    r.backup_path = "C:/backups/bk_2026-05-17"
    return r


class TestVerificationDisabledSubject:
    """Subject line acquires the warning tag only in case 3 success."""

    @patch("src.notifications.email_notifier._send_email")
    def test_subject_has_tag_on_success_when_flag_on(self, mock_send):
        mock_send.return_value = (True, "sent")
        send_backup_report(
            _make_config(),
            profile_name="MyProfile",
            success=True,
            summary="42 files backed up",
            verification_disabled=True,
        )
        # Inspect the subject passed to _send_email (positional arg 1).
        subject = mock_send.call_args.args[1]
        assert "verification disabled" in subject.lower()
        assert "MyProfile" in subject
        assert "SUCCESS" in subject

    @patch("src.notifications.email_notifier._send_email")
    def test_subject_unchanged_when_flag_off(self, mock_send):
        mock_send.return_value = (True, "sent")
        send_backup_report(
            _make_config(),
            profile_name="MyProfile",
            success=True,
            summary="42 files backed up",
            verification_disabled=False,
        )
        subject = mock_send.call_args.args[1]
        assert "verification disabled" not in subject.lower()
        assert "SUCCESS" in subject

    @patch("src.notifications.email_notifier._send_email")
    def test_subject_no_tag_on_failure(self, mock_send):
        mock_send.return_value = (True, "sent")
        send_backup_report(
            _make_config(),
            profile_name="MyProfile",
            success=False,
            summary="Disk full",
            verification_disabled=True,
        )
        subject = mock_send.call_args.args[1]
        # Failure already screams loudly — no extra "verification disabled".
        assert "verification disabled" not in subject.lower()
        assert "FAILED" in subject

    @patch("src.notifications.email_notifier._send_email")
    def test_subject_no_tag_on_cancelled(self, mock_send):
        mock_send.return_value = (True, "sent")
        send_backup_report(
            _make_config(),
            profile_name="MyProfile",
            success=False,  # cancelled is reported as not-success
            summary="cancel",
            cancelled=True,
            verification_disabled=True,
        )
        subject = mock_send.call_args.args[1]
        assert "verification disabled" not in subject.lower()
        assert "CANCELLED" in subject


class TestVerificationDisabledBody:
    """The amber block sits in the body when (and only when) the flag is on."""

    def test_simple_html_has_block_when_flag_on(self):
        body = _build_html(
            profile_name="MyProfile",
            success=True,
            summary="ok",
            verification_disabled=True,
        )
        assert "Verification disabled" in body
        assert "Verify tab" in body  # variant A wording landmark
        # The HTML template wraps the sentence over several lines, so
        # asserting on the exact run "no periodic verification" misses
        # the inter-word newline. Match the two halves independently.
        lowered = body.lower()
        assert "no periodic" in lowered
        assert "no automatic integrity check" in lowered

    def test_simple_html_no_block_when_flag_off(self):
        body = _build_html(
            profile_name="MyProfile",
            success=True,
            summary="ok",
            verification_disabled=False,
        )
        assert "Verification disabled" not in body

    def test_enriched_html_has_block_when_flag_on(self):
        body = _build_backup_html(
            profile_name="MyProfile",
            success=True,
            summary="ok",
            result=_make_result(),
            verification_disabled=True,
        )
        assert "Verification disabled" in body
        assert "Verify tab" in body

    def test_enriched_html_no_block_when_flag_off(self):
        body = _build_backup_html(
            profile_name="MyProfile",
            success=True,
            summary="ok",
            result=_make_result(),
            verification_disabled=False,
        )
        assert "Verification disabled" not in body

    def test_block_suppressed_on_failure_even_with_flag(self):
        """A FAILED enriched body must not display the warning block —
        the failure marker is already the dominant signal."""
        body = _build_backup_html(
            profile_name="MyProfile",
            success=False,
            summary="boom",
            result=_make_result(),
            verification_disabled=True,
        )
        assert "Verification disabled" not in body

    def test_block_suppressed_on_cancelled_even_with_flag(self):
        body = _build_backup_html(
            profile_name="MyProfile",
            success=False,
            summary="cancel",
            cancelled=True,
            result=_make_result(),
            verification_disabled=True,
        )
        assert "Verification disabled" not in body


class TestBackwardCompat:
    """Existing callers that do not pass the kwarg keep their old output."""

    @patch("src.notifications.email_notifier._send_email")
    def test_default_kwarg_is_false(self, mock_send):
        mock_send.return_value = (True, "sent")
        send_backup_report(
            _make_config(),
            profile_name="MyProfile",
            success=True,
            summary="ok",
        )
        subject = mock_send.call_args.args[1]
        assert "verification disabled" not in subject.lower()

    def test_simple_html_default_omits_block(self):
        body = _build_html("p", True, "ok")
        assert "Verification disabled" not in body

    def test_enriched_html_default_omits_block(self):
        body = _build_backup_html("p", True, "ok", result=_make_result())
        assert "Verification disabled" not in body
