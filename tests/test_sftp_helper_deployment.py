"""Tests for Stage 2 of the Option-C PoC — server helper deployment.

Pins three contracts on ``SFTPStorage._ensure_helper_script`` and its
support trio (``_has_gnu_tar``, ``_remote_file_hash_matches``,
``_upload_helper``):

1. **Detection-then-no-op**: when the helper is already on the server
   with the expected hash, ``_ensure_helper_script`` skips the
   upload entirely and just returns the cached path.

2. **Deployment**: when the helper is missing or stale, the bytes are
   pushed via SFTP, chmod 0755 is applied, and the remote path is
   returned.

3. **Graceful degradation**: when GNU tar is unavailable, the asset
   is missing, or any SSH operation throws, the method returns
   ``None`` so the caller can fall back to sequential verify.

All paramiko I/O is mocked. Real connection logic is exercised by the
integration suite once Stage 4 wires the helper into the upload path.
"""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock, patch

from src.storage.sftp import (
    SFTPStorage,
    _get_helper_bytes_and_hash,
    _resolve_helper_path,
)


def _make_backend() -> SFTPStorage:
    """Construct an SFTPStorage without touching paramiko or real config."""
    backend = SFTPStorage.__new__(SFTPStorage)
    backend._persistent_transport = None
    backend._cancel_check = None
    backend._remote_path = "/home/u/backups"
    return backend


class _FakeExecChannel:
    """Stand-in for a paramiko exec channel.

    Mimics the recv-until-empty + recv_exit_status protocol the
    helper-deployment code relies on. Tests construct one with a
    pre-baked output and exit status.
    """

    def __init__(self, output: bytes, exit_status: int = 0):
        self._output = output
        self._exit_status = exit_status
        self._drained = False
        self.last_cmd: str | None = None
        self.closed = False

    def settimeout(self, _t: float) -> None:
        pass

    def exec_command(self, cmd: str) -> None:
        self.last_cmd = cmd

    def recv(self, _n: int) -> bytes:
        if self._drained:
            return b""
        self._drained = True
        return self._output

    def recv_exit_status(self) -> int:
        return self._exit_status

    def close(self) -> None:
        self.closed = True


class _FakeTransport:
    """Returns scripted ``_FakeExecChannel`` instances on ``open_session``.

    Pass ``channel_outputs`` as a list of (output_bytes, exit_status)
    tuples — they're popped in order so tests can stage multiple
    SSH calls (GNU tar probe → hash check → optional upload).
    """

    def __init__(self, channel_outputs: list[tuple[bytes, int]]):
        self._outputs = list(channel_outputs)
        self.opened_channels: list[_FakeExecChannel] = []

    def open_session(self) -> _FakeExecChannel:
        if not self._outputs:
            raise RuntimeError("Test stub ran out of canned channel outputs")
        output, exit_status = self._outputs.pop(0)
        channel = _FakeExecChannel(output, exit_status)
        self.opened_channels.append(channel)
        return channel


class _FakeSFTPFile:
    """Mimics paramiko's SFTPClient.open return value (a context-manager
    file). The fake pushes the assembled bytes into the parent
    ``_FakeSFTP.written`` dict on ``__exit__`` so tests can assert on
    what landed at each path.
    """

    def __init__(self, sftp: _FakeSFTP, path: str):
        self._sftp = sftp
        self._path = path
        self._chunks: list[bytes] = []

    def write(self, data: bytes) -> None:
        self._chunks.append(data)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._sftp.written[self._path] = b"".join(self._chunks)
        return False


class _FakeSFTP:
    def __init__(self):
        self.written: dict[str, bytes] = {}
        self.chmoded: dict[str, int] = {}
        self.closed = False

    def open(self, path: str, mode: str) -> _FakeSFTPFile:
        assert mode == "wb"
        return _FakeSFTPFile(self, path)

    def chmod(self, path: str, mode: int) -> None:
        self.chmoded[path] = mode

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------
# Asset resolution
# ---------------------------------------------------------------------


class TestAssetResolution:
    """``_resolve_helper_path`` finds the bash file in dev mode."""

    def test_helper_is_present_in_repo(self):
        path = _resolve_helper_path()
        assert path is not None, "server_helper.sh should ship in assets/"
        assert path.is_file()
        assert path.name == "server_helper.sh"

    def test_helper_bytes_and_hash_consistency(self):
        first = _get_helper_bytes_and_hash()
        second = _get_helper_bytes_and_hash()
        assert first is not None
        assert first is second, "module-level cache should return the same tuple"
        content, digest = first
        assert len(digest) == 64
        assert hashlib.sha256(content).hexdigest() == digest

    def test_helper_asset_is_lf_on_disk(self):
        """The shipped asset must have LF line endings.

        Regression guard for the 2026-05-15 incident: git's
        `autocrlf=true` on Windows had converted the helper to CRLF
        between checkout and PyInstaller bundling. The resulting
        `#!/bin/bash\r\n` shebang made the Linux kernel look up an
        interpreter literally named `bash\r`, execve failed with
        ENOENT, and the SSH channel was closed before any tar byte
        could be sent. The fix combines `.gitattributes` (*.sh
        text eol=lf) with this static check so a future regression
        — editor saving CRLF, autocrlf rule lost, etc. — fails
        loudly in CI instead of silently shipping a broken helper.
        """
        path = _resolve_helper_path()
        assert path is not None
        content = path.read_bytes()
        assert b"\r" not in content, (
            f"{path} contains CR bytes — check .gitattributes "
            f"(*.sh text eol=lf) and your editor's line-ending setting"
        )

    def test_get_helper_bytes_normalises_crlf_to_lf(self, tmp_path, monkeypatch):
        """Defence in depth — the loader rewrites CRLF to LF.

        If the static check above fails to catch a CRLF helper (older
        checkout, third-party asset, manual edit), this layer ensures
        the bytes pushed to the server are still a valid POSIX script.
        Without it, the CRLF helper would deploy under a hash that
        does not match the LF version on the server, triggering a
        fresh upload of unrunnable bytes.
        """
        import src.storage.sftp as sftp_mod

        crlf_helper = tmp_path / "server_helper.sh"
        crlf_helper.write_bytes(b"#!/bin/bash\r\nset -e\r\necho ok\r\n")

        monkeypatch.setattr(sftp_mod, "_HELPER_CACHE", None)
        monkeypatch.setattr(sftp_mod, "_resolve_helper_path", lambda: crlf_helper)

        result = _get_helper_bytes_and_hash()
        assert result is not None
        content, digest = result
        assert b"\r" not in content
        assert content == b"#!/bin/bash\nset -e\necho ok\n"
        assert digest == hashlib.sha256(content).hexdigest()


# ---------------------------------------------------------------------
# Deployment paths
# ---------------------------------------------------------------------


class TestEnsureHelperScript:
    """End-to-end behaviour of ``_ensure_helper_script``."""

    def test_returns_cached_path_when_helper_already_correct(self):
        """Server reports the helper exists with the expected hash → skip upload."""
        helper = _get_helper_bytes_and_hash()
        assert helper is not None
        _, expected_hash = helper
        expected_path = f"/tmp/bm-helper-{expected_hash[:8]}.sh"

        backend = _make_backend()
        transport = _FakeTransport(
            [
                (b"tar (GNU tar) 1.34\n", 0),  # GNU tar probe
                (expected_hash.encode() + b"\n", 0),  # remote hash matches
            ]
        )

        with patch.object(backend, "_get_transport", return_value=transport):
            result = backend._ensure_helper_script()

        assert result == expected_path
        # Both channels closed exactly once.
        assert len(transport.opened_channels) == 2
        assert all(c.closed for c in transport.opened_channels)

    def test_uploads_helper_when_remote_hash_does_not_match(self):
        """Remote hash mismatch → push the bytes + chmod 0755."""
        helper = _get_helper_bytes_and_hash()
        assert helper is not None
        content, expected_hash = helper
        expected_path = f"/tmp/bm-helper-{expected_hash[:8]}.sh"

        backend = _make_backend()
        transport = _FakeTransport(
            [
                (b"tar (GNU tar) 1.34\n", 0),
                (b"deadbeef\n", 0),  # wrong hash
            ]
        )
        sftp = _FakeSFTP()

        with (
            patch.object(backend, "_get_transport", return_value=transport),
            patch.object(backend, "_get_sftp", return_value=sftp),
        ):
            result = backend._ensure_helper_script()

        assert result == expected_path
        assert sftp.written.get(expected_path) == content
        assert sftp.chmoded.get(expected_path) == 0o755
        assert sftp.closed is True

    def test_returns_none_when_server_has_bsd_tar(self):
        """Non-GNU tar → degrade. No further probes, no upload."""
        backend = _make_backend()
        transport = _FakeTransport(
            [
                (b"bsdtar 3.5.3 - libarchive 3.5.3\n", 0),
            ]
        )

        with patch.object(backend, "_get_transport", return_value=transport):
            result = backend._ensure_helper_script()

        assert result is None
        # Only the GNU tar probe ran — no hash check, no upload.
        assert len(transport.opened_channels) == 1

    def test_returns_none_when_upload_raises(self):
        """SFTP failure during chmod / write → degrade, no exception leaks."""
        helper = _get_helper_bytes_and_hash()
        assert helper is not None

        backend = _make_backend()
        transport = _FakeTransport(
            [
                (b"tar (GNU tar) 1.34\n", 0),
                (b"wronghash\n", 0),
            ]
        )

        sftp = MagicMock()
        sftp.open.side_effect = OSError("disk full")

        with (
            patch.object(backend, "_get_transport", return_value=transport),
            patch.object(backend, "_get_sftp", return_value=sftp),
        ):
            result = backend._ensure_helper_script()

        assert result is None

    def test_returns_none_when_transport_raises(self):
        """Connection failure → degrade silently."""
        backend = _make_backend()
        with patch.object(
            backend, "_get_transport", side_effect=OSError("connection refused")
        ):
            result = backend._ensure_helper_script()
        assert result is None

    def test_cache_short_circuits_second_call(self):
        """Second call must not hit SSH at all.

        Saves ~3 round-trips per backup on the steady state where
        the helper is already deployed.
        """
        helper = _get_helper_bytes_and_hash()
        assert helper is not None
        _, expected_hash = helper

        backend = _make_backend()
        transport = _FakeTransport(
            [
                (b"tar (GNU tar) 1.34\n", 0),
                (expected_hash.encode() + b"\n", 0),
            ]
        )

        with patch.object(backend, "_get_transport", return_value=transport):
            backend._ensure_helper_script()
            second = backend._ensure_helper_script()

        assert second is not None
        # Cache hit means no additional channels were opened on the
        # second call — the stub list is now exhausted from the first.
        assert len(transport.opened_channels) == 2

    def test_cache_pins_none_when_first_call_failed(self):
        """A failed probe must not be retried on every upload.

        Cached ``None`` is the right thing to do: re-running the GNU
        tar probe every backup would burn an SSH round-trip for
        zero new information on a server that never gets reconfigured.
        Users who change server tooling can reconnect to refresh the
        cache (new SFTPStorage instance → new cache).
        """
        backend = _make_backend()
        # Only one canned output — a second SSH probe would raise
        # RuntimeError ("ran out of canned"), which is exactly what
        # the cache exists to prevent.
        transport = _FakeTransport(
            [
                (b"bsdtar 3.5.3\n", 0),
            ]
        )

        with patch.object(backend, "_get_transport", return_value=transport):
            first = backend._ensure_helper_script()
            second = backend._ensure_helper_script()

        assert first is None
        assert second is None
        # Confirm the cache short-circuited: only the BSD-tar probe ran.
        assert len(transport.opened_channels) == 1


# ---------------------------------------------------------------------
# Support method behaviour
# ---------------------------------------------------------------------


class TestHasGnuTar:
    """``_has_gnu_tar`` recognises GNU tar via the version string."""

    def test_recognises_gnu_tar(self):
        backend = _make_backend()
        transport = _FakeTransport([(b"tar (GNU tar) 1.34\nCopyright ...\n", 0)])
        assert backend._has_gnu_tar(transport) is True

    def test_rejects_bsdtar(self):
        backend = _make_backend()
        transport = _FakeTransport([(b"bsdtar 3.5.3 - libarchive 3.5.3\n", 0)])
        assert backend._has_gnu_tar(transport) is False

    def test_returns_false_on_exception(self):
        backend = _make_backend()
        transport = MagicMock()
        transport.open_session.side_effect = OSError("broken pipe")
        assert backend._has_gnu_tar(transport) is False

    def test_unbounded_recv_does_not_oom(self):
        """Regression for the 2026-05-15 OOM freeze.

        Stage 4 of PoC C wired ``_ensure_helper_script`` into
        ``upload_tar_stream``. Existing tar-upload tests had been
        written for the pre-stage-4 code path and configured their
        channel mocks with only ``recv_exit_status`` — leaving
        ``recv`` as a default ``MagicMock`` that returns truthy
        ``MagicMock`` instances forever. The old recv loop
        ``while True: chunk = recv(); if not chunk: break; output
        += chunk`` then ran indefinitely, accumulating MagicMock
        call metadata until pytest committed >2 GB of private bytes
        and Windows froze the desktop (no BSOD).

        This test pins the bound: a channel whose recv returns
        anything other than bytes must make ``_has_gnu_tar`` bail
        out promptly (False), not loop.
        """
        backend = _make_backend()
        channel = MagicMock()
        channel.settimeout = MagicMock()
        channel.exec_command = MagicMock()
        channel.recv.return_value = MagicMock()  # truthy non-bytes
        channel.recv_exit_status.return_value = 0
        channel.close = MagicMock()
        transport = MagicMock()
        transport.open_session.return_value = channel

        assert backend._has_gnu_tar(transport) is False
        # The guard must trigger on the first non-bytes chunk; recv
        # is called at most a handful of times, not millions.
        assert channel.recv.call_count <= 3


class TestRemoteFileHashMatches:
    """``_remote_file_hash_matches`` compares cleanly trimmed hashes."""

    def test_matches_when_hashes_equal(self):
        backend = _make_backend()
        transport = _FakeTransport([(b"abcd1234\n", 0)])
        assert backend._remote_file_hash_matches(transport, "/tmp/x", "abcd1234") is True

    def test_mismatch_returns_false(self):
        backend = _make_backend()
        transport = _FakeTransport([(b"deadbeef\n", 0)])
        assert backend._remote_file_hash_matches(transport, "/tmp/x", "abcd1234") is False

    def test_missing_file_returns_false(self):
        """``sha256sum`` of a non-existent file yields empty stdout."""
        backend = _make_backend()
        transport = _FakeTransport([(b"", 1)])
        assert backend._remote_file_hash_matches(transport, "/tmp/x", "abcd1234") is False

    def test_returns_false_on_exception(self):
        backend = _make_backend()
        transport = MagicMock()
        transport.open_session.side_effect = OSError("transport gone")
        assert backend._remote_file_hash_matches(transport, "/tmp/x", "abcd1234") is False

    def test_unbounded_recv_does_not_oom(self):
        """Regression for the 2026-05-15 OOM freeze (mirror of
        ``TestHasGnuTar.test_unbounded_recv_does_not_oom``).

        The same recv loop pattern lives in
        ``_remote_file_hash_matches``; same guard, same test.
        """
        backend = _make_backend()
        channel = MagicMock()
        channel.settimeout = MagicMock()
        channel.exec_command = MagicMock()
        channel.recv.return_value = MagicMock()  # truthy non-bytes
        channel.recv_exit_status.return_value = 0
        channel.close = MagicMock()
        transport = MagicMock()
        transport.open_session.return_value = channel

        assert backend._remote_file_hash_matches(transport, "/tmp/x", "abcd") is False
        assert channel.recv.call_count <= 3
