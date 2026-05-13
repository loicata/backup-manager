"""Tests for the v3.6 PoC C verify dispatch (sidecar-first path).

Two layers under test:

1. ``_try_get_server_hashes_sidecar`` — pulls and parses the
   ``<backup>.wbserverhashes`` file the server helper emits during
   upload. Must return None gracefully on every failure mode so the
   caller falls back to the v3.5.9 sequential ``sha256sum`` loop
   without raising.

2. ``verify_backup_files`` (refactored) — dispatches to the sidecar
   path when available, otherwise to ``_verify_backup_files_sequential``.
   The legacy path is exercised by ``test_sftp_verify_parallel.py``
   only at the algorithmic level; here we only need to confirm the
   dispatch picks the right branch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.storage.sftp import SFTPStorage


def _make_backend() -> SFTPStorage:
    backend = SFTPStorage.__new__(SFTPStorage)
    backend._persistent_transport = None
    backend._cancel_check = None
    backend._remote_path = "/home/u/backups"
    return backend


class _FakeSFTPReadFile:
    def __init__(self, payload: bytes | None):
        self._payload = payload

    def read(self) -> bytes:
        if self._payload is None:
            raise FileNotFoundError("sidecar missing")
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeSFTP:
    def __init__(self, sidecars: dict[str, bytes | None]):
        self._sidecars = sidecars
        self.opened: list[str] = []
        self.closed = False

    def open(self, path: str, mode: str):
        assert mode == "rb"
        self.opened.append(path)
        if path not in self._sidecars:
            raise FileNotFoundError(f"no entry for {path}")
        return _FakeSFTPReadFile(self._sidecars[path])

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------
# Sidecar parser
# ---------------------------------------------------------------------


class TestTryGetServerHashesSidecar:
    """``_try_get_server_hashes_sidecar`` is purely a parser + downloader.

    Every error mode (file missing, empty, malformed) returns None
    so the caller can degrade to the sequential path without raising.
    """

    def test_returns_dict_when_sidecar_well_formed(self):
        backend = _make_backend()
        # Build line by line so Python's adjacent-string-literal
        # concatenation doesn't glue tokens together.
        h1 = "abc" + "0" * 61
        h2 = "def" + "0" * 61
        h3 = "1" * 64
        payload = (f"{h1}  rel/path1.txt\n{h2}  rel/path2.txt\n{h3}  deep/dir/file.bin\n").encode()
        sftp = _FakeSFTP({"/home/u/backups/MyBackup.wbserverhashes": payload})

        with (
            patch.object(backend, "_get_transport", return_value=MagicMock()),
            patch.object(backend, "_get_sftp", return_value=sftp),
        ):
            result = backend._try_get_server_hashes_sidecar("MyBackup")

        assert result == {
            "rel/path1.txt": h1,
            "rel/path2.txt": h2,
            "deep/dir/file.bin": h3,
        }
        assert sftp.closed is True

    def test_returns_none_when_sidecar_missing(self):
        backend = _make_backend()
        sftp = _FakeSFTP({})  # path absent → FileNotFoundError on open

        with (
            patch.object(backend, "_get_transport", return_value=MagicMock()),
            patch.object(backend, "_get_sftp", return_value=sftp),
        ):
            result = backend._try_get_server_hashes_sidecar("MyBackup")

        assert result is None

    def test_returns_none_when_sidecar_empty(self):
        backend = _make_backend()
        sftp = _FakeSFTP({"/home/u/backups/MyBackup.wbserverhashes": b""})

        with (
            patch.object(backend, "_get_transport", return_value=MagicMock()),
            patch.object(backend, "_get_sftp", return_value=sftp),
        ):
            result = backend._try_get_server_hashes_sidecar("MyBackup")

        assert result is None

    def test_returns_none_when_sidecar_has_no_valid_lines(self):
        """Truncated or garbage content → None, not partial dict.

        Returning partial data would silently accept missing hashes.
        Better to fall through to the sequential path which re-hashes
        every file from disk.
        """
        backend = _make_backend()
        payload = b"not a hash\nstill not\n# comment\n"
        sftp = _FakeSFTP({"/home/u/backups/MyBackup.wbserverhashes": payload})

        with (
            patch.object(backend, "_get_transport", return_value=MagicMock()),
            patch.object(backend, "_get_sftp", return_value=sftp),
        ):
            result = backend._try_get_server_hashes_sidecar("MyBackup")

        assert result is None

    def test_tolerates_malformed_lines_mixed_with_valid_lines(self):
        """Mixed quality → keep the valid lines, log debug on the rest."""
        backend = _make_backend()
        h_a = "1" * 64
        h_b = "2" * 64
        payload = (
            f"{h_a}  valid.txt\ngarbage line should be skipped\n\n{h_b}  another.txt\n"
        ).encode()
        sftp = _FakeSFTP({"/home/u/backups/MyBackup.wbserverhashes": payload})

        with (
            patch.object(backend, "_get_transport", return_value=MagicMock()),
            patch.object(backend, "_get_sftp", return_value=sftp),
        ):
            result = backend._try_get_server_hashes_sidecar("MyBackup")

        assert result == {
            "valid.txt": h_a,
            "another.txt": h_b,
        }

    def test_returns_none_when_sftp_open_fails_unexpectedly(self):
        """Connection errors mid-fetch → degrade silently."""
        backend = _make_backend()
        sftp = MagicMock()
        sftp.open.side_effect = OSError("connection reset")

        with (
            patch.object(backend, "_get_transport", return_value=MagicMock()),
            patch.object(backend, "_get_sftp", return_value=sftp),
        ):
            result = backend._try_get_server_hashes_sidecar("MyBackup")

        assert result is None


# ---------------------------------------------------------------------
# Dispatch (verify_backup_files)
# ---------------------------------------------------------------------


class TestVerifyBackupFilesDispatch:
    """``verify_backup_files`` picks sidecar-first, falls back on miss."""

    def test_uses_sidecar_when_available(self):
        backend = _make_backend()
        file_list = [("a.txt", 100), ("b.txt", 200), ("c.txt", 300)]
        sidecar = {
            "a.txt": "1" * 64,
            "b.txt": "2" * 64,
            # c.txt deliberately missing from sidecar — should map to ""
        }

        with (
            patch.object(backend, "list_backup_files", return_value=file_list),
            patch.object(backend, "_try_get_server_hashes_sidecar", return_value=sidecar),
            patch.object(backend, "_verify_backup_files_sequential") as seq,
        ):
            result = backend.verify_backup_files("MyBackup")

        # Sequential path must NOT have been hit.
        seq.assert_not_called()
        assert result == [
            ("a.txt", 100, "1" * 64),
            ("b.txt", 200, "2" * 64),
            ("c.txt", 300, ""),
        ]

    def test_falls_back_to_sequential_when_sidecar_missing(self):
        backend = _make_backend()
        file_list = [("a.txt", 100)]
        expected_fallback = [("a.txt", 100, "deadbeef" + "0" * 56)]

        with (
            patch.object(backend, "list_backup_files", return_value=file_list),
            patch.object(backend, "_try_get_server_hashes_sidecar", return_value=None),
            patch.object(
                backend,
                "_verify_backup_files_sequential",
                return_value=expected_fallback,
            ) as seq,
        ):
            result = backend.verify_backup_files("MyBackup")

        seq.assert_called_once_with("MyBackup", file_list)
        assert result == expected_fallback

    def test_empty_file_list_short_circuits_before_sidecar_lookup(self):
        """No files to verify → return [] without any SSH activity."""
        backend = _make_backend()

        with (
            patch.object(backend, "list_backup_files", return_value=[]),
            patch.object(backend, "_try_get_server_hashes_sidecar") as side,
            patch.object(backend, "_verify_backup_files_sequential") as seq,
        ):
            result = backend.verify_backup_files("MyBackup")

        assert result == []
        side.assert_not_called()
        seq.assert_not_called()
