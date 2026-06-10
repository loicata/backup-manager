"""Tests for src.security.encryption.verify_encrypted_archive.

Real authentication of a .tar.wbenc archive (per-chunk AES-256-GCM +
trailing HMAC), without extracting. This is what backs the post-backup
"Verification OK" for encrypted local backups — the previous code only
stat-ed the file size and logged "GCM-authenticated" while decrypting
nothing (audit M06/M33), so a corrupt archive passed verification.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.core.phases.collector import FileInfo
from src.core.phases.local_writer import write_encrypted_tar
from src.security.encryption import verify_encrypted_archive

PW = "password12345678"


def _make_archive(tmp_path: Path) -> Path:
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("alpha " * 2000, encoding="utf-8")
    (src / "b.txt").write_text("beta " * 4000, encoding="utf-8")

    def fi(name: str) -> FileInfo:
        p = src / name
        return FileInfo(
            source_path=p,
            relative_path=name,
            size=p.stat().st_size,
            mtime=p.stat().st_mtime,
            source_root=str(src),
        )

    dest = tmp_path / "dest"
    dest.mkdir()
    return write_encrypted_tar([fi("a.txt"), fi("b.txt")], dest, "Bk_FULL", PW)


class TestVerifyEncryptedArchive:
    def test_valid_archive_passes(self, tmp_path):
        archive = _make_archive(tmp_path)
        verify_encrypted_archive(archive, PW)  # must not raise

    def test_wrong_password_raises(self, tmp_path):
        archive = _make_archive(tmp_path)
        with pytest.raises(Exception):
            verify_encrypted_archive(archive, "the-wrong-password")

    def test_truncated_archive_raises(self, tmp_path):
        archive = _make_archive(tmp_path)
        data = archive.read_bytes()
        archive.write_bytes(data[: len(data) // 2])  # lose the HMAC trailer
        with pytest.raises(Exception):
            verify_encrypted_archive(archive, PW)

    def test_bitflip_in_body_raises(self, tmp_path):
        archive = _make_archive(tmp_path)
        data = bytearray(archive.read_bytes())
        # Flip a byte well past the salt/nonce header — corrupts a GCM chunk.
        data[len(data) // 2] ^= 0xFF
        archive.write_bytes(bytes(data))
        with pytest.raises(Exception):
            verify_encrypted_archive(archive, PW)
