"""Tests for SFTP tar-stream upload and directory cache.

Covers:
- Option A: _created_dirs cache skips redundant mkdir
- Option D: upload_tar_stream sends files as tar archive
- Fallback: encrypted uploads use file-by-file mode
- Integration: tar extraction produces correct files on disk
"""

import io
import sys
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.exceptions import WriteError
from src.core.phases.collector import FileInfo

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_mock_paramiko():
    """Inject a mock paramiko into sys.modules and return it."""
    mp = MagicMock()
    mp.Ed25519Key = MagicMock()
    mp.Ed25519Key.__name__ = "Ed25519Key"
    mp.ECDSAKey = MagicMock()
    mp.ECDSAKey.__name__ = "ECDSAKey"
    mp.RSAKey = MagicMock()
    mp.RSAKey.__name__ = "RSAKey"
    mp.Transport = MagicMock()
    mp.SFTPClient = MagicMock()
    mp.HostKeys = MagicMock
    mp.AuthenticationException = type("AuthenticationException", (Exception,), {})
    mp.SSHException = type("SSHException", (Exception,), {})
    sys.modules["paramiko"] = mp
    sys.modules["paramiko.hostkeys"] = MagicMock()
    return mp


def _cleanup_paramiko():
    sys.modules.pop("paramiko", None)
    sys.modules.pop("paramiko.hostkeys", None)


def _make_storage():
    from src.storage.sftp import SFTPStorage

    return SFTPStorage(
        host="10.0.0.1",
        port=22,
        username="user",
        password="pass",
        remote_path="/home/user/backups",
    )


def _make_file_infos(tmp_path, count=5, size=64):
    """Create sample files and return FileInfo list."""
    files = []
    for i in range(count):
        subdir = tmp_path / f"sub{i % 3}"
        subdir.mkdir(exist_ok=True)
        p = subdir / f"file{i}.txt"
        content = f"content-{i}" * max(1, size // 10)
        p.write_text(content, encoding="utf-8")
        stat = p.stat()
        files.append(
            FileInfo(
                source_path=p,
                relative_path=f"sub{i % 3}/file{i}.txt",
                size=stat.st_size,
                mtime=stat.st_mtime,
                source_root=tmp_path,
            )
        )
    return files


# ===========================================================================
# Option A — Directory cache tests
# ===========================================================================


class TestDirectoryCache:
    """Verify that _created_dirs skips redundant mkdir calls."""

    def test_mkdir_exec_called_once_per_dir(self):
        """Same directory should only trigger one mkdir -p exec."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 0
            mock_transport.open_session.return_value = mock_channel

            # Call twice with same dir
            storage._ensure_remote_dir_exec(mock_transport, "/home/user/backups/subdir")
            storage._ensure_remote_dir_exec(mock_transport, "/home/user/backups/subdir")

            # open_session should be called only once
            assert mock_transport.open_session.call_count == 1
        finally:
            _cleanup_paramiko()

    def test_mkdir_exec_different_dirs(self):
        """Different directories should each trigger a mkdir -p."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 0
            mock_transport.open_session.return_value = mock_channel

            storage._ensure_remote_dir_exec(mock_transport, "/home/user/dir1")
            storage._ensure_remote_dir_exec(mock_transport, "/home/user/dir2")

            assert mock_transport.open_session.call_count == 2
        finally:
            _cleanup_paramiko()

    def test_connect_clears_cache(self):
        """connect() should reset the directory cache."""
        mp = _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._created_dirs.add("/some/old/dir")

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True
            mp.Transport.return_value = mock_transport

            mock_sock = MagicMock()
            with (
                patch("src.storage.sftp.socket.socket", return_value=mock_sock),
                patch.object(storage, "_verify_host_key"),
            ):
                storage.connect()

            assert len(storage._created_dirs) == 0
        finally:
            _cleanup_paramiko()

    def test_mkdir_sftp_cached(self):
        """SFTP mkdir should also use the directory cache."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_sftp = MagicMock()

            # First call creates dirs
            storage._ensure_remote_dir_sftp(mock_sftp, "/home/user/backups")
            first_stat_count = mock_sftp.stat.call_count

            # Second call with same dir should skip entirely
            storage._ensure_remote_dir_sftp(mock_sftp, "/home/user/backups")
            assert mock_sftp.stat.call_count == first_stat_count
        finally:
            _cleanup_paramiko()

    def test_mkdir_sftp_sub_components_cached(self):
        """After creating /a/b/c, creating /a/b/d should skip /a and /a/b."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_sftp = MagicMock()

            storage._ensure_remote_dir_sftp(mock_sftp, "/a/b/c")

            # /a/b/d only needs to stat /a/b/d, not /a or /a/b
            mock_sftp.stat.reset_mock()
            storage._ensure_remote_dir_sftp(mock_sftp, "/a/b/d")
            # Only /a/b/d should be checked (1 stat), not /a or /a/b
            assert mock_sftp.stat.call_count == 1
        finally:
            _cleanup_paramiko()


# ===========================================================================
# Option D — Tar stream upload tests
# ===========================================================================


class TestTarStreamUpload:
    """Verify upload_tar_stream sends a valid tar archive."""

    def test_tar_stream_sends_data(self):
        """upload_tar_stream should send tar data via channel."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = True

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True
            storage._persistent_transport = mock_transport

            # Capture data sent to channel
            sent_data = bytearray()
            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 0

            def capture_sendall(data):
                sent_data.extend(data)

            mock_channel.sendall.side_effect = capture_sendall
            mock_transport.open_session.return_value = mock_channel

            # Create a temp file
            import tempfile

            with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w") as f:
                f.write("hello world")
                tmp_path = Path(f.name)

            try:
                files = [(tmp_path, "test/hello.txt", tmp_path.stat().st_size)]
                storage.upload_tar_stream(files, "backup_2026")

                # Verify tar was sent
                assert len(sent_data) > 0

                # Verify it's a valid tar
                tar_io = io.BytesIO(bytes(sent_data))
                with tarfile.open(fileobj=tar_io, mode="r|") as tar:
                    members = list(tar)
                    assert len(members) == 1
                    assert members[0].name == "test/hello.txt"
            finally:
                tmp_path.unlink()

            # Verify channel lifecycle (mkdir channel + tar channel)
            mock_channel.shutdown_write.assert_called_once()
            # recv_exit_status called twice: once for mkdir, once for tar
            assert mock_channel.recv_exit_status.call_count == 2
        finally:
            _cleanup_paramiko()

    def test_tar_stream_closes_adhoc_transport(self, tmp_path):
        """Transport created just for a tar upload must be closed afterwards.

        When no connect() has been issued, ``upload_tar_stream`` obtains
        a fresh transport through ``_get_transport()``.  The previous
        implementation never closed it on any code path, leaking an SSH
        session per tar upload.
        """
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = True
            storage._persistent_transport = None  # Ad-hoc path.

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True

            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 0
            mock_transport.open_session.return_value = mock_channel

            src = tmp_path / "f.txt"
            src.write_text("x", encoding="utf-8")
            files = [(src, "f.txt", 1)]

            with patch.object(storage, "_create_transport", return_value=mock_transport):
                storage.upload_tar_stream(files, "backup_adhoc")

            mock_transport.close.assert_called_once()
        finally:
            _cleanup_paramiko()

    def test_tar_stream_preserves_persistent_transport(self, tmp_path):
        """When using a persistent transport, upload_tar_stream must NOT close it."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = True

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True
            storage._persistent_transport = mock_transport

            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 0
            mock_transport.open_session.return_value = mock_channel

            src = tmp_path / "f.txt"
            src.write_text("x", encoding="utf-8")
            files = [(src, "f.txt", 1)]

            storage.upload_tar_stream(files, "backup_persistent")

            mock_transport.close.assert_not_called()
        finally:
            _cleanup_paramiko()

    def test_tar_stream_multiple_files(self, tmp_path):
        """Multiple files should all appear in the tar archive."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = True

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True
            storage._persistent_transport = mock_transport

            sent_data = bytearray()
            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 0
            mock_channel.sendall.side_effect = lambda d: sent_data.extend(d)
            mock_transport.open_session.return_value = mock_channel

            # Create files
            files = []
            for i in range(10):
                p = tmp_path / f"file{i}.txt"
                p.write_text(f"data-{i}", encoding="utf-8")
                files.append((p, f"dir/file{i}.txt", p.stat().st_size))

            storage.upload_tar_stream(files, "backup_2026")

            # Verify all 10 files in tar
            tar_io = io.BytesIO(bytes(sent_data))
            with tarfile.open(fileobj=tar_io, mode="r|") as tar:
                members = list(tar)
            assert len(members) == 10
        finally:
            _cleanup_paramiko()

    def test_tar_stream_exit_failure_raises(self, tmp_path):
        """Non-zero exit status from tar should raise OSError."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = True

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True
            storage._persistent_transport = mock_transport

            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 1  # tar failed
            mock_channel.sendall.return_value = None
            mock_transport.open_session.return_value = mock_channel

            p = tmp_path / "file.txt"
            p.write_text("data", encoding="utf-8")
            files = [(p, "file.txt", p.stat().st_size)]

            with pytest.raises(OSError, match="tar extraction failed"):
                storage.upload_tar_stream(files, "backup")
        finally:
            _cleanup_paramiko()

    def test_tar_stream_progress_callback(self, tmp_path):
        """Progress callback should be called during tar streaming."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = True

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True
            storage._persistent_transport = mock_transport

            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 0
            mock_channel.sendall.return_value = None
            mock_transport.open_session.return_value = mock_channel

            p = tmp_path / "file.txt"
            p.write_text("x" * 1000, encoding="utf-8")
            files = [(p, "file.txt", p.stat().st_size)]

            progress_calls = []
            storage.upload_tar_stream(
                files,
                "backup",
                progress_callback=lambda sent, total: progress_calls.append((sent, total)),
            )

            assert len(progress_calls) > 0
            # Last call should have sent > 0
            assert progress_calls[-1][0] > 0
        finally:
            _cleanup_paramiko()

    def test_tar_fallback_when_exec_unavailable(self, tmp_path):
        """When exec is not available, falls back to file-by-file."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = False

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True
            storage._persistent_transport = mock_transport

            p = tmp_path / "file.txt"
            p.write_text("data", encoding="utf-8")
            files = [(p, "file.txt", p.stat().st_size)]

            with patch.object(storage, "upload_file") as mock_upload:
                storage.upload_tar_stream(files, "backup")
                mock_upload.assert_called_once()
        finally:
            _cleanup_paramiko()

    def test_tar_creates_remote_dir(self):
        """upload_tar_stream should create remote dir before tar."""
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = True

            mock_transport = MagicMock()
            mock_transport.is_active.return_value = True
            storage._persistent_transport = mock_transport

            sent_data = bytearray()
            mock_channel = MagicMock()
            mock_channel.recv_exit_status.return_value = 0
            mock_channel.sendall.side_effect = lambda d: sent_data.extend(d)
            mock_transport.open_session.return_value = mock_channel

            import tempfile

            with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w") as f:
                f.write("test")
                tmp_path = Path(f.name)

            try:
                files = [(tmp_path, "file.txt", tmp_path.stat().st_size)]
                storage.upload_tar_stream(files, "backup_2026")
            finally:
                tmp_path.unlink()

            # open_session called at least twice: mkdir + tar
            assert mock_transport.open_session.call_count >= 2
            # First exec_command should be mkdir
            first_exec = mock_channel.exec_command.call_args_list[0]
            assert "mkdir -p" in first_exec[0][0]
        finally:
            _cleanup_paramiko()


# ===========================================================================
# Integration: tar extraction produces correct files
# ===========================================================================


class TestTarIntegration:
    """Verify tar stream produces correct files when extracted."""

    def test_tar_extraction_matches_source(self, tmp_path):
        """Files extracted from tar stream should match originals."""
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        # Create source files with known content
        expected = {}
        for i in range(20):
            subdir = source_dir / f"level1/level2_{i % 4}"
            subdir.mkdir(parents=True, exist_ok=True)
            p = subdir / f"file{i}.dat"
            content = f"data-{i}-{'x' * (i * 10)}".encode()
            p.write_bytes(content)
            rel_path = str(p.relative_to(source_dir)).replace("\\", "/")
            expected[rel_path] = content

        # Build tar in memory (simulates what upload_tar_stream does)
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w|") as tar:
            for rel_path, content in expected.items():
                info = tarfile.TarInfo(name=rel_path)
                info.size = len(content)
                tar.addfile(info, fileobj=io.BytesIO(content))

        # Extract (simulates what remote tar xf does)
        tar_buffer.seek(0)
        with tarfile.open(fileobj=tar_buffer, mode="r|") as tar:
            tar.extractall(path=extract_dir, filter="data")

        # Verify all files match
        for rel_path, content in expected.items():
            extracted = extract_dir / rel_path
            assert extracted.exists(), f"Missing: {rel_path}"
            assert extracted.read_bytes() == content, f"Mismatch: {rel_path}"

    def test_tar_preserves_file_sizes(self, tmp_path):
        """File sizes in extracted tar should match originals."""
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        extract_dir = tmp_path / "extract"
        extract_dir.mkdir()

        # Create files of various sizes (including empty and large)
        sizes = [0, 1, 100, 1024, 10240, 65536]
        files_info = []
        for _i, size in enumerate(sizes):
            p = source_dir / f"file_{size}b.bin"
            p.write_bytes(b"\x42" * size)
            files_info.append((p, f"file_{size}b.bin", size))

        # Build and extract tar
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w|") as tar:
            for local_path, rel_path, size in files_info:
                info = tarfile.TarInfo(name=rel_path)
                info.size = size
                with open(local_path, "rb") as f:
                    tar.addfile(info, fileobj=f)

        tar_buffer.seek(0)
        with tarfile.open(fileobj=tar_buffer, mode="r|") as tar:
            tar.extractall(path=extract_dir, filter="data")

        for _, rel_path, expected_size in files_info:
            extracted = extract_dir / rel_path
            assert extracted.stat().st_size == expected_size


# ===========================================================================
# Remote writer integration: tar vs file-by-file
# ===========================================================================


class TestRemoteWriterTarIntegration:
    """Verify write_remote correctly dispatches tar vs file-by-file."""

    def test_write_remote_uses_tar_when_available(self, tmp_path):
        """write_remote should use tar stream when backend supports it."""
        from src.core.phases.remote_writer import write_remote

        files = _make_file_infos(tmp_path, count=5)

        backend = MagicMock(
            spec=["connect", "disconnect", "upload_tar_stream", "supports_tar_stream"]
        )
        backend.supports_tar_stream = True
        backend.upload_tar_stream.return_value = None

        write_remote(files, backend, "backup_2026")

        backend.connect.assert_called_once()
        backend.upload_tar_stream.assert_called_once()
        backend.disconnect.assert_called_once()

    def test_write_remote_falls_back_without_tar(self, tmp_path):
        """write_remote should upload file-by-file without tar support."""
        from src.core.phases.remote_writer import write_remote

        files = _make_file_infos(tmp_path, count=3)

        backend = MagicMock(spec=["connect", "disconnect", "upload_file"])
        backend.upload_file.return_value = None

        write_remote(files, backend, "backup_2026")

        backend.connect.assert_called_once()
        assert backend.upload_file.call_count == 3
        backend.disconnect.assert_called_once()

    def test_write_remote_encrypted_uses_tar_wbenc(self, tmp_path):
        """Encrypted uploads produce a single .tar.wbenc via upload_file."""
        from src.core.phases.remote_writer import write_remote

        files = _make_file_infos(tmp_path, count=2)

        backend = MagicMock(
            spec=[
                "connect",
                "disconnect",
                "upload_file",
                "upload_tar_stream",
                "supports_tar_stream",
            ]
        )
        backend.supports_tar_stream = True

        def _drain(fileobj, remote_path, size=0):
            while fileobj.read(65536):
                pass

        backend.upload_file.side_effect = _drain

        write_remote(files, backend, "backup_2026", encrypt_password="password12345678")

        # Should NOT use unencrypted tar stream
        backend.upload_tar_stream.assert_not_called()
        # Should upload single .tar.wbenc file
        backend.upload_file.assert_called_once()
        remote_path = backend.upload_file.call_args[0][1]
        assert remote_path == "backup_2026.tar.wbenc"

    def test_write_remote_encrypted_upload_failure_joins_producer(self, tmp_path):
        """Producer thread must terminate cleanly when upload raises.

        Before the unconditional ``thread.join()`` was adopted, a
        failing upload returned from ``write_remote`` while leaving
        the producer alive as a daemon thread still trying to push
        bytes into a freshly closed pipe.  We now block until the
        producer has observed ``BrokenPipeError`` and exited, so no
        orphan threads linger.
        """
        import threading

        from src.core.phases.remote_writer import write_remote

        files = _make_file_infos(tmp_path, count=6, size=4096)

        backend = MagicMock(
            spec=[
                "connect",
                "disconnect",
                "upload_file",
                "upload_tar_stream",
                "supports_tar_stream",
            ]
        )
        backend.supports_tar_stream = True

        def _drain_then_fail(fileobj, remote_path, size=0):
            # Read just enough to unblock the producer once, then fail.
            fileobj.read(1024)
            raise OSError("network dropped")

        backend.upload_file.side_effect = _drain_then_fail

        threads_before = {t.ident for t in threading.enumerate()}

        with pytest.raises(WriteError):
            write_remote(files, backend, "backup_2026", encrypt_password="password12345678")

        # No producer thread should remain alive after write_remote
        # returns — the finally unconditionally joins it.
        leaked = [
            t for t in threading.enumerate() if t.ident not in threads_before and t.is_alive()
        ]
        assert leaked == [], f"Producer thread leaked: {leaked!r}"

    def test_write_remote_disconnect_on_error(self, tmp_path):
        """Backend is disconnected even if upload fails."""
        from src.core.phases.remote_writer import write_remote

        files = _make_file_infos(tmp_path, count=1)

        backend = MagicMock(
            spec=["connect", "disconnect", "upload_tar_stream", "supports_tar_stream"]
        )
        backend.supports_tar_stream = True
        backend.upload_tar_stream.side_effect = OSError("network down")

        with pytest.raises(WriteError):
            write_remote(files, backend, "backup_2026")

        backend.disconnect.assert_called_once()
