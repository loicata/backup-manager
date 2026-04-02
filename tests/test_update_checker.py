"""Tests for src.core.update_checker — GitHub Releases API integration,
version comparison, network error handling, and security checks.
"""

import json
from unittest.mock import MagicMock, patch

from src.core.update_checker import (
    _version_tuple,
    check_for_update,
)

# ---------------------------------------------------------------------------
# Version comparison
# ---------------------------------------------------------------------------


class TestVersionTuple:
    def test_simple_version(self) -> None:
        assert _version_tuple("3.0") == (3, 0)

    def test_three_part_version(self) -> None:
        assert _version_tuple("3.1.2") == (3, 1, 2)

    def test_single_number(self) -> None:
        assert _version_tuple("5") == (5,)

    def test_version_with_prefix(self) -> None:
        assert _version_tuple("v2.1.0") == (2, 1, 0)

    def test_empty_string(self) -> None:
        assert _version_tuple("") == ()


class TestVersionComparison:
    def test_newer_version(self) -> None:
        assert _version_tuple("3.1") > _version_tuple("3.0")

    def test_same_version(self) -> None:
        assert _version_tuple("3.0") == _version_tuple("3.0")

    def test_older_version(self) -> None:
        assert _version_tuple("2.9") < _version_tuple("3.0")

    def test_major_upgrade(self) -> None:
        assert _version_tuple("4.0") > _version_tuple("3.9.9")

    def test_patch_upgrade(self) -> None:
        assert _version_tuple("3.0.1") > _version_tuple("3.0.0")


# ---------------------------------------------------------------------------
# check_for_update — GitHub Releases API (mocked network)
# ---------------------------------------------------------------------------


class TestCheckForUpdate:
    """Tests for the GitHub Releases API integration."""

    def _make_response(self, body: dict) -> MagicMock:
        """Create a mock urllib response wrapping a JSON body."""
        resp = MagicMock()
        resp.read.return_value = json.dumps(body).encode("utf-8")
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_newer_version_triggers_callback(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = self._make_response(
            {
                "tag_name": "v4.0.0",
                "html_url": "https://github.com/loicata/backup-manager/releases/tag/v4.0.0",
            }
        )
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_called_once_with(
            "4.0.0",
            "https://github.com/loicata/backup-manager/releases/tag/v4.0.0",
        )

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_same_version_no_callback(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = self._make_response(
            {
                "tag_name": "v3.0.0",
                "html_url": "https://github.com/loicata/backup-manager/releases/tag/v3.0.0",
            }
        )
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_older_version_no_callback(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = self._make_response(
            {
                "tag_name": "v2.0.0",
                "html_url": "https://github.com/loicata/backup-manager/releases/tag/v2.0.0",
            }
        )
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_tag_without_v_prefix(self, mock_urlopen: MagicMock) -> None:
        """Tags without 'v' prefix (e.g. '4.0.0') should work."""
        mock_urlopen.return_value = self._make_response(
            {
                "tag_name": "4.0.0",
                "html_url": "https://github.com/loicata/backup-manager/releases/tag/4.0.0",
            }
        )
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_called_once()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_network_timeout_no_crash(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = TimeoutError("Connection timed out")
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_invalid_json_no_crash(self, mock_urlopen: MagicMock) -> None:
        resp = MagicMock()
        resp.read.return_value = b"NOT JSON {{{}"
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = resp

        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    def test_non_https_url_rejected(self) -> None:
        """HTTP URLs for the check endpoint should be rejected."""
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback, url="http://insecure.example.com/api")
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_missing_tag_name_field(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = self._make_response(
            {
                "html_url": "https://github.com/loicata/backup-manager/releases/tag/v4.0.0",
            }
        )
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_missing_html_url_field(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = self._make_response(
            {
                "tag_name": "v4.0.0",
            }
        )
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_connection_error_no_crash(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = ConnectionError("Network unreachable")
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_not_called()

    @patch("src.core.update_checker.urllib.request.urlopen")
    def test_prerelease_tag_still_compared(self, mock_urlopen: MagicMock) -> None:
        """Tags like 'v4.0.0-beta' should still extract version digits."""
        mock_urlopen.return_value = self._make_response(
            {
                "tag_name": "v4.0.0-beta",
                "html_url": "https://github.com/loicata/backup-manager/releases/tag/v4.0.0-beta",
            }
        )
        callback = MagicMock()
        thread = check_for_update("3.0.0", callback)
        thread.join(timeout=5)
        callback.assert_called_once()
