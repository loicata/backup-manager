"""Tests for src.core.update_checker — version comparison, hash
verification, network timeout handling, and security checks.
"""

import hashlib
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from unittest.mock import MagicMock, patch

import pytest

from src.core.update_checker import (
    _version_tuple,
    check_for_update,
    verify_update_hash,
    CHECK_TIMEOUT,
)


# ---------------------------------------------------------------------------
# Version comparison
# ---------------------------------------------------------------------------


class TestVersionTuple:
    def test_simple_version(self):
        assert _version_tuple("3.0") == (3, 0)

    def test_three_part_version(self):
        assert _version_tuple("3.1.2") == (3, 1, 2)

    def test_single_number(self):
        assert _version_tuple("5") == (5,)

    def test_version_with_prefix(self):
        assert _version_tuple("v2.1.0") == (2, 1, 0)

    def test_empty_string(self):
        assert _version_tuple("") == ()


class TestVersionComparison:
    def test_newer_version(self):
        assert _version_tuple("3.1") > _version_tuple("3.0")

    def test_same_version(self):
        assert _version_tuple("3.0") == _version_tuple("3.0")

    def test_older_version(self):
        assert _version_tuple("2.9") < _version_tuple("3.0")

    def test_major_upgrade(self):
        assert _version_tuple("4.0") > _version_tuple("3.9.9")

    def test_patch_upgrade(self):
        assert _version_tuple("3.0.1") > _version_tuple("3.0.0")


# ---------------------------------------------------------------------------
# Hash verification
# ---------------------------------------------------------------------------


class TestVerifyUpdateHash:
    def test_valid_hash_matches(self):
        data = b"hello world"
        expected = hashlib.sha256(data).hexdigest()
        assert verify_update_hash(data, expected) is True

    def test_invalid_hash_rejected(self):
        data = b"hello world"
        assert verify_update_hash(data, "0" * 64) is False

    def test_missing_hash_allowed_backward_compat(self):
        """When no hash is provided, allow the update (backward compat)."""
        assert verify_update_hash(b"anything", None) is True
        assert verify_update_hash(b"anything", "") is True

    def test_case_insensitive_hash(self):
        data = b"test payload"
        expected = hashlib.sha256(data).hexdigest().upper()
        assert verify_update_hash(data, expected) is True

    def test_empty_data_valid_hash(self):
        data = b""
        expected = hashlib.sha256(data).hexdigest()
        assert verify_update_hash(data, expected) is True

    def test_wrong_hash_for_different_data(self):
        data_a = b"version 3.1"
        data_b = b"version 3.2"
        hash_a = hashlib.sha256(data_a).hexdigest()
        assert verify_update_hash(data_b, hash_a) is False


# ---------------------------------------------------------------------------
# check_for_update integration (mocked network)
# ---------------------------------------------------------------------------


class TestCheckForUpdate:
    def _make_response(self, body: dict, status: int = 200) -> MagicMock:
        """Create a mock urllib response."""
        resp = MagicMock()
        resp.read.return_value = json.dumps(body).encode("utf-8")
        resp.status = status
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_newer_version_triggers_callback(self, mock_urlopen):
        mock_urlopen.return_value = self._make_response({
            "latest": "4.0",
            "url": "https://example.com/update.exe",
        })
        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_called_once_with("4.0", "https://example.com/update.exe")

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_same_version_no_callback(self, mock_urlopen):
        mock_urlopen.return_value = self._make_response({
            "latest": "3.0",
            "url": "https://example.com/update.exe",
        })
        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_older_version_no_callback(self, mock_urlopen):
        mock_urlopen.return_value = self._make_response({
            "latest": "2.0",
            "url": "https://example.com/update.exe",
        })
        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_network_timeout_no_crash(self, mock_urlopen):
        mock_urlopen.side_effect = TimeoutError("Connection timed out")
        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_invalid_json_no_crash(self, mock_urlopen):
        resp = MagicMock()
        resp.read.return_value = b"NOT JSON {{{}"
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = resp

        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    def test_non_https_url_rejected(self):
        """HTTP URLs for the check endpoint should be rejected."""
        callback = MagicMock()
        thread = check_for_update(
            "3.0", callback, url="http://insecure.example.com/version.json"
        )
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_non_https_download_url_rejected(self, mock_urlopen):
        """HTTP download URLs in the response should be rejected."""
        mock_urlopen.return_value = self._make_response({
            "latest": "4.0",
            "url": "http://insecure.example.com/update.exe",
        })
        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_missing_latest_field(self, mock_urlopen):
        mock_urlopen.return_value = self._make_response({
            "url": "https://example.com/update.exe",
        })
        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_missing_url_field(self, mock_urlopen):
        mock_urlopen.return_value = self._make_response({
            "latest": "4.0",
        })
        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_connection_error_no_crash(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("Network unreachable")
        callback = MagicMock()
        thread = check_for_update("3.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()
