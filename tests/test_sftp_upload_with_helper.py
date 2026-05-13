"""Tests for Stage 4c of the Option-C PoC — upload-with-helper.

The bash helper consumes the tar stream on its stdin while emitting
``<sha256>  <path>`` lines on stdout. ``_upload_tar_stream_with_helper``
must:

1. Build the tar stream and write it to the helper's stdin in the
   main thread (same as the legacy path).
2. Drain stdout (the hash lines) AND stderr (warnings, errors)
   concurrently in a reader thread. Without this, the helper blocks
   when the SSH receive window fills with hashes nobody is reading.
3. After ``channel.shutdown_write()``, wait for the reader to exit,
   then check the exit code.
4. On success, persist the captured stdout to ``<dest>.wbserverhashes``
   via SFTP so the verify phase can short-circuit.

These tests stub paramiko transport + channel + SFTP — no actual
SSH. The concurrency contract is exercised by timing-sensitive
assertions on the fake channel.
"""

from __future__ import annotations

import tarfile
import threading
import time
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.storage.sftp import SFTPStorage, _HelperEarlyFailure


def _make_backend() -> SFTPStorage:
    backend = SFTPStorage.__new__(SFTPStorage)
    backend._persistent_transport = None
    backend._cancel_check = None
    backend._remote_path = "/home/u/backups"
    backend._bandwidth_limit_kbps = 0
    return backend


def _make_fixture_files(tmp_path: Path, names_and_contents: list[tuple[str, bytes]]):
    """Create files under tmp_path and return the (local_path, rel, size) tuples."""
    files: list[tuple[Path, str, int]] = []
    for rel, content in names_and_contents:
        local = tmp_path / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(content)
        files.append((local, rel, len(content)))
    return files


class _FakeHelperChannel:
    """Mimics a paramiko channel attached to a running helper.

    Records the bytes written to stdin so tests can confirm the tar
    stream was actually sent. Exposes ``stdout_chunks`` and
    ``stderr_chunks`` to script the helper's "responses" — the
    reader thread pulls these as if they were arriving from the
    server.

    Concurrency:
        ``send`` / ``shutdown_write`` happen on the main thread;
        ``recv`` / ``recv_stderr`` / ``recv_*_ready`` are exercised
        by the reader thread. We protect the lists with a lock so
        the readers see a consistent view even under timing variance.
    """

    def __init__(
        self,
        stdout_chunks: list[bytes],
        stderr_chunks: list[bytes] | None = None,
        exit_status: int = 0,
        delay_before_stdout: float = 0.0,
    ):
        self._stdout = list(stdout_chunks)
        self._stderr = list(stderr_chunks or [])
        self._exit_status = exit_status
        self._delay = delay_before_stdout
        self._lock = threading.Lock()
        self._stdin_received = bytearray()
        self._write_shutdown = False
        self._exit_ready = False
        self._stdin_eof_event = threading.Event()
        self._delay_done_event = threading.Event()
        self.closed = False

        # Kick the delay off in a background thread so the reader
        # can poll recv_ready immediately.
        if self._delay > 0:

            def _wait_then_unlock():
                time.sleep(self._delay)
                self._delay_done_event.set()

            threading.Thread(target=_wait_then_unlock, daemon=True).start()
        else:
            self._delay_done_event.set()

    # ----- main thread API (send tar stream) -----

    def settimeout(self, _t: float) -> None:
        pass

    def exec_command(self, cmd: str) -> None:
        self.last_cmd = cmd

    def send(self, data: bytes) -> int:
        with self._lock:
            self._stdin_received.extend(data)
        return len(data)

    def sendall(self, data: bytes) -> None:
        self.send(data)

    def shutdown_write(self) -> None:
        self._write_shutdown = True
        self._stdin_eof_event.set()
        # After stdin is fully written, the helper finishes — make
        # exit status available so the drain loop can terminate.
        self._exit_ready = True

    def close(self) -> None:
        self.closed = True

    # ----- reader thread API (drain stdout / stderr) -----

    def recv_ready(self) -> bool:
        if not self._delay_done_event.is_set():
            return False
        with self._lock:
            return bool(self._stdout)

    def recv_stderr_ready(self) -> bool:
        if not self._delay_done_event.is_set():
            return False
        with self._lock:
            return bool(self._stderr)

    def recv(self, _n: int) -> bytes:
        if not self._delay_done_event.is_set():
            return b""
        with self._lock:
            if not self._stdout:
                return b""
            return self._stdout.pop(0)

    def recv_stderr(self, _n: int) -> bytes:
        if not self._delay_done_event.is_set():
            return b""
        with self._lock:
            if not self._stderr:
                return b""
            return self._stderr.pop(0)

    def exit_status_ready(self) -> bool:
        return self._exit_ready

    def recv_exit_status(self) -> int:
        # Real paramiko blocks until the remote process exits. Our
        # contract: the main thread already called shutdown_write
        # so the helper is "done" — just return the canned status.
        self._stdin_eof_event.wait(timeout=10)
        return self._exit_status


class _FakeTransport:
    def __init__(self, channel: _FakeHelperChannel):
        self.channel = channel
        self.opened = 0

    def open_session(self) -> _FakeHelperChannel:
        self.opened += 1
        return self.channel

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------


class TestHappyPath:
    """Helper succeeds, tar stream lands intact, sidecar is persisted."""

    def test_writes_sidecar_to_remote_when_helper_succeeds(self, tmp_path: Path):
        backend = _make_backend()
        files = _make_fixture_files(
            tmp_path,
            [("foo.txt", b"foo content"), ("dir/bar.bin", b"bar bytes" * 100)],
        )

        stdout_lines = [
            b"a" * 64 + b"  foo.txt\n",
            b"b" * 64 + b"  dir/bar.bin\n",
        ]
        channel = _FakeHelperChannel(stdout_lines, exit_status=0)
        transport = _FakeTransport(channel)

        # ``_upload_helper`` is the SFTP write path — mock the sidecar write.
        with patch.object(backend, "_upload_helper") as upload_helper:
            backend._upload_tar_stream_with_helper(
                transport,
                files,
                "/home/u/backups/MyBackup",
                "/tmp/bm-helper-abc.sh",
                None,
                None,
            )

        # Helper command was invoked with the right args.
        assert channel.last_cmd == "'/tmp/bm-helper-abc.sh' '/home/u/backups/MyBackup'"

        # Tar stream actually flowed through stdin (not zero bytes).
        # We can't trivially parse the tar from a memory buffer here
        # because tarfile in stream mode emits padding, but >0 bytes
        # and at least the size of the two payloads means the data
        # reached the channel.
        assert len(channel._stdin_received) >= len(b"foo content") + len(b"bar bytes" * 100)

        # Sidecar persisted at <dest>.wbserverhashes via _upload_helper.
        upload_helper.assert_called_once()
        call_args = upload_helper.call_args
        # Positional: (transport, content, remote_path)
        # _upload_helper(transport, content, remote_path)
        passed_transport, passed_content, passed_path = call_args[0]
        assert passed_path == "/home/u/backups/MyBackup.wbserverhashes"
        # Content is exactly what the helper emitted on stdout.
        assert passed_content == b"".join(stdout_lines)

        # Channel was closed.
        assert channel.closed is True

    def test_tar_stream_contains_all_input_files(self, tmp_path: Path):
        """The tar stream on stdin is well-formed and round-trips."""
        backend = _make_backend()
        contents = {
            "a.txt": b"alpha",
            "b/c.txt": b"beta",
            "d/e/f.txt": b"gamma",
        }
        files = _make_fixture_files(tmp_path, list(contents.items()))

        stdout = [b"f" * 64 + b"  a.txt\n"]
        channel = _FakeHelperChannel(stdout, exit_status=0)
        transport = _FakeTransport(channel)

        with patch.object(backend, "_upload_helper"):
            backend._upload_tar_stream_with_helper(
                transport, files, "/dest", "/tmp/helper.sh", None, None
            )

        # Reconstruct files from the captured tar stream.
        tar_bytes = bytes(channel._stdin_received)
        recovered: dict[str, bytes] = {}
        with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r|") as tar:
            for member in tar:
                if member.isfile():
                    extracted = tar.extractfile(member)
                    assert extracted is not None
                    recovered[member.name] = extracted.read()
        assert recovered == contents


# ---------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------


class TestFailureModes:
    """Stage 4c failure semantics."""

    def test_open_session_failure_raises_helper_early_failure(self, tmp_path: Path):
        """Helper-side error BEFORE any byte sent → caller can retry classic."""
        backend = _make_backend()
        files = _make_fixture_files(tmp_path, [("a.txt", b"data")])

        transport = MagicMock()
        transport.open_session.side_effect = OSError("broken pipe")

        with pytest.raises(_HelperEarlyFailure, match="open_session failed"):
            backend._upload_tar_stream_with_helper(
                transport, files, "/dest", "/tmp/helper.sh", None, None
            )

    def test_exec_command_failure_raises_helper_early_failure(self, tmp_path: Path):
        """exec_command throws → also early failure (no tar bytes sent)."""
        backend = _make_backend()
        files = _make_fixture_files(tmp_path, [("a.txt", b"data")])

        channel = _FakeHelperChannel([], exit_status=0)
        channel.exec_command = MagicMock(side_effect=OSError("exec denied"))  # type: ignore[method-assign]
        transport = _FakeTransport(channel)

        with pytest.raises(_HelperEarlyFailure, match="exec_command failed"):
            backend._upload_tar_stream_with_helper(
                transport, files, "/dest", "/tmp/helper.sh", None, None
            )

    def test_nonzero_exit_raises_oserror_with_stderr_tail(self, tmp_path: Path):
        """Helper crashed mid-upload → OSError carrying the stderr message."""
        backend = _make_backend()
        files = _make_fixture_files(tmp_path, [("a.txt", b"data")])

        channel = _FakeHelperChannel(
            stdout_chunks=[],
            stderr_chunks=[b"error: out of disk space\n"],
            exit_status=2,
        )
        transport = _FakeTransport(channel)

        with patch.object(backend, "_upload_helper") as upload_helper:
            with pytest.raises(OSError, match="exit 2.*out of disk space"):
                backend._upload_tar_stream_with_helper(
                    transport, files, "/dest", "/tmp/helper.sh", None, None
                )
        # On failure we must NOT have attempted to write a sidecar
        # — that would have masked the failure as success.
        upload_helper.assert_not_called()

    def test_empty_stdout_skips_sidecar_write_with_warning(self, tmp_path: Path):
        """Helper succeeded but emitted no hashes (e.g. empty file list).

        The upload still counts as successful — the verify will
        gracefully fall back to ``_verify_backup_files_sequential``
        because the sidecar will be absent.
        """
        backend = _make_backend()
        files = _make_fixture_files(tmp_path, [("a.txt", b"data")])

        channel = _FakeHelperChannel(stdout_chunks=[], exit_status=0)
        transport = _FakeTransport(channel)

        with patch.object(backend, "_upload_helper") as upload_helper:
            backend._upload_tar_stream_with_helper(
                transport, files, "/dest", "/tmp/helper.sh", None, None
            )

        upload_helper.assert_not_called()

    def test_sidecar_write_failure_is_non_fatal(self, tmp_path: Path):
        """The backup itself succeeded — losing the sidecar should
        not retroactively fail the run. Verify just falls back.
        """
        backend = _make_backend()
        files = _make_fixture_files(tmp_path, [("a.txt", b"data")])

        channel = _FakeHelperChannel([b"1" * 64 + b"  a.txt\n"], exit_status=0)
        transport = _FakeTransport(channel)

        with patch.object(
            backend, "_upload_helper", side_effect=OSError("sftp write failed")
        ):
            # Should not raise.
            backend._upload_tar_stream_with_helper(
                transport, files, "/dest", "/tmp/helper.sh", None, None
            )
