"""Regression tests for audit L8 (findings 7 & 8):

- SMTP: opportunistic STARTTLS upgrade on a cleartext-login send to a
  remote server, so the password is not sent in the clear. Local
  bridges (127.0.0.1) are exempt.
- IAM: build_iam_policy() scopes the data/retention actions to the
  chosen buckets instead of arn:aws:s3:::*.
"""

import json
from unittest.mock import MagicMock, patch

from src.core.config import EmailConfig
from src.notifications.email_notifier import (
    _send_email,
    _should_opportunistic_starttls,
)
from src.storage.s3_setup import REQUIRED_IAM_POLICY, build_iam_policy


def _config(**overrides) -> EmailConfig:
    defaults = {
        "enabled": True,
        "smtp_host": "smtp.example.com",
        "smtp_port": 587,
        "use_tls": False,
        "username": "user",
        "password": "secret",
        "from_address": "from@test.com",
        "to_address": "to@test.com",
    }
    defaults.update(overrides)
    return EmailConfig(**defaults)


class TestOpportunisticStartTLS:
    def test_predicate_remote_cleartext_login(self):
        assert _should_opportunistic_starttls(_config()) is True

    def test_predicate_skips_local_bridge(self):
        assert _should_opportunistic_starttls(_config(smtp_host="127.0.0.1")) is False

    def test_predicate_skips_when_no_username(self):
        assert _should_opportunistic_starttls(_config(username="")) is False

    def test_predicate_skips_when_tls_already_on(self):
        assert _should_opportunistic_starttls(_config(use_tls=True)) is False

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_starttls_upgraded_on_remote_cleartext(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp.has_extn.return_value = True
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        ok, _ = _send_email(_config(), "subj", "<p>x</p>")

        assert ok is True
        mock_smtp.starttls.assert_called_once()  # upgraded before login
        mock_smtp.login.assert_called_once()

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_no_starttls_for_local_bridge(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp.has_extn.return_value = True
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        ok, _ = _send_email(_config(smtp_host="127.0.0.1", username=""), "subj", "<p>x</p>")

        assert ok is True
        mock_smtp.starttls.assert_not_called()  # loopback bridge exempt

    @patch("src.notifications.email_notifier.smtplib.SMTP")
    def test_no_starttls_when_server_lacks_extension(self, mock_smtp_class):
        mock_smtp = MagicMock()
        mock_smtp.has_extn.return_value = False  # server does not advertise it
        mock_smtp_class.return_value.__enter__ = lambda s: mock_smtp
        mock_smtp_class.return_value.__exit__ = lambda s, *a: None

        ok, _ = _send_email(_config(), "subj", "<p>x</p>")

        assert ok is True
        mock_smtp.starttls.assert_not_called()


class TestBuildIamPolicy:
    def test_scopes_object_actions_to_buckets(self):
        doc = json.loads(build_iam_policy(["my-backup", "my-mirror"]))
        statements = {s["Sid"]: s for s in doc["Statement"]}

        # Account-level actions stay broad (CreateBucket has no bucket yet).
        assert statements["AccountLevelSetup"]["Resource"] == "*"
        assert "s3:CreateBucket" in statements["AccountLevelSetup"]["Action"]

        # Object actions are scoped to the named buckets — NOT arn:::*.
        obj = statements["ObjectScoped"]
        assert obj["Resource"] == ["arn:aws:s3:::my-backup/*", "arn:aws:s3:::my-mirror/*"]
        assert "s3:DeleteObject" in obj["Action"]
        assert "arn:aws:s3:::*" not in obj["Resource"]
        assert "arn:aws:s3:::*/*" not in obj["Resource"]

    def test_empty_falls_back_to_bootstrap(self):
        assert build_iam_policy([]) == REQUIRED_IAM_POLICY
        assert build_iam_policy(["", "  "]) == REQUIRED_IAM_POLICY

    def test_output_is_valid_json(self):
        json.loads(build_iam_policy(["b1"]))  # must not raise
