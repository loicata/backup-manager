"""Regression test for the v3.7.41 multi-source restoration bug.

User-reported symptom (3.7.40 install, profile ``tes_crypter`` with
two sources ``F:/Documents/Divers/Economie`` and
``F:/Documents/Divers/BFM``):

    On restore, the contents of the FIRST source landed at the
    restore-directory root (flat, no enclosing folder), while the
    SECOND source was correctly nested under its own folder. The
    user lost the per-source separation and had to manually
    re-create the ``Economie/`` folder by moving files around.

Root cause in ``RecoveryTab._decrypt_and_extract``: the pre-3.7.41
code learned a ``strip_prefix`` from the first tar member it saw
(e.g. ``"Economie/"``) and unconditionally stripped that prefix
from every subsequent member's name. ``Economie/*`` got stripped
flat; ``BFM/*`` did NOT match the strip prefix and survived intact
— hence the asymmetry.

The tar is correctly built by ``collector.py::add_file`` (line ~336)
with ``rel = f"{source_root.name}/{inner_rel}"``, so the fix is
simply to use ``member.name`` verbatim during extraction.

These tests reproduce a 2-source tar in-memory + on disk via the
real ``EncryptingWriter`` / ``DecryptingReader`` pipeline (no
mock — the failure mode was integration-shaped, mocks would miss
it again). They run in <0.5 s because the payload is a few bytes
per file.
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from src.security.encryption import EncryptingWriter
from src.ui.tabs.recovery_tab import RecoveryTab


# ---------------------------------------------------------------------
# Helpers — build a .tar.wbenc the same way the backup pipeline does.
# ---------------------------------------------------------------------


def _add_str_member(tar: tarfile.TarFile, arcname: str, payload: bytes) -> None:
    """Add a single file-from-bytes member to an open tarfile."""
    info = tarfile.TarInfo(name=arcname)
    info.size = len(payload)
    tar.addfile(info, io.BytesIO(payload))


def _write_encrypted_tar(
    tar_path: Path,
    password: str,
    members: list[tuple[str, bytes]],
) -> None:
    """Write a ``.tar.wbenc`` archive at ``tar_path``.

    ``members`` is the list of ``(arcname, payload)`` to add, in order.
    Uses the real ``EncryptingWriter`` so the on-disk format is the
    same shape that ``_decrypt_and_extract`` expects in production.
    """
    with open(tar_path, "wb") as raw:
        writer = EncryptingWriter(raw, password)
        try:
            with tarfile.open(fileobj=writer, mode="w|") as tar:
                for arcname, payload in members:
                    _add_str_member(tar, arcname, payload)
        finally:
            writer.close()


# ---------------------------------------------------------------------
# Regression: the bug shows up only with >=2 top-level folders.
# ---------------------------------------------------------------------


class TestMultiSourceRestorePreservesFolders:
    """The user's ``tes_crypter`` scenario reduced to its essence."""

    PASSWORD = "test-password-1234"

    def test_two_sources_keep_their_top_level_folders(self, tmp_path: Path) -> None:
        """``Economie/`` AND ``BFM/`` must both exist after restore.

        This is the exact failure mode the user reported on v3.7.40.
        """
        tar_path = tmp_path / "tes_crypter_FULL_20260527_220027.tar.wbenc"
        dest = tmp_path / "restore"
        dest.mkdir()

        _write_encrypted_tar(
            tar_path,
            self.PASSWORD,
            [
                ("Economie/budget.txt", b"economie-payload-1"),
                ("Economie/subdir/notes.md", b"economie-payload-2"),
                ("BFM/article.txt", b"bfm-payload-1"),
                ("BFM/subdir/index.html", b"bfm-payload-2"),
            ],
        )

        count = RecoveryTab._decrypt_and_extract(tar_path, dest, self.PASSWORD)

        restore_root = dest / "tes_crypter_FULL_20260527_220027"
        assert count == 4
        # The critical assertion: BOTH top-level folders survive.
        assert (restore_root / "Economie").is_dir(), (
            "Economie/ folder is missing — the v3.7.40 strip_prefix bug "
            "extracted Economie's contents flat at the root"
        )
        assert (restore_root / "BFM").is_dir(), "BFM/ folder is missing"
        # And the files end up in the right place — no flat extraction
        # at the restore root for any of them.
        assert (restore_root / "Economie" / "budget.txt").is_file()
        assert (restore_root / "Economie" / "subdir" / "notes.md").is_file()
        assert (restore_root / "BFM" / "article.txt").is_file()
        assert (restore_root / "BFM" / "subdir" / "index.html").is_file()
        # And payloads round-trip intact.
        assert (restore_root / "Economie" / "budget.txt").read_bytes() == b"economie-payload-1"
        assert (restore_root / "BFM" / "article.txt").read_bytes() == b"bfm-payload-1"

    def test_no_flat_files_at_restore_root_for_first_source(self, tmp_path: Path) -> None:
        """Explicit negative assertion: the first source's files must
        NOT appear at the restore root. The 3.7.40 bug placed every
        Economie/* file directly under restore_dir.
        """
        tar_path = tmp_path / "demo_FULL_20260527_120000.tar.wbenc"
        dest = tmp_path / "restore"
        dest.mkdir()

        _write_encrypted_tar(
            tar_path,
            self.PASSWORD,
            [
                ("Alpha/one.txt", b"a1"),
                ("Beta/two.txt", b"b1"),
            ],
        )

        RecoveryTab._decrypt_and_extract(tar_path, dest, self.PASSWORD)

        restore_root = dest / "demo_FULL_20260527_120000"
        # The bug would have placed ``one.txt`` directly here.
        assert not (restore_root / "one.txt").exists(), (
            "one.txt must be inside Alpha/, not at restore root — "
            "v3.7.40 strip_prefix regression"
        )

    def test_three_sources_all_preserve_their_folders(self, tmp_path: Path) -> None:
        """Generalisation to N sources — every top-level folder must
        survive. The bug strips the FIRST one only; the second and
        third would have survived even on the bugged code, but the
        first is the canary.
        """
        tar_path = tmp_path / "triple_FULL_20260527.tar.wbenc"
        dest = tmp_path / "restore"
        dest.mkdir()

        _write_encrypted_tar(
            tar_path,
            self.PASSWORD,
            [
                ("First/a.txt", b"A"),
                ("Second/b.txt", b"B"),
                ("Third/c.txt", b"C"),
            ],
        )

        RecoveryTab._decrypt_and_extract(tar_path, dest, self.PASSWORD)

        restore_root = dest / "triple_FULL_20260527"
        for folder, leaf, payload in (
            ("First", "a.txt", b"A"),
            ("Second", "b.txt", b"B"),
            ("Third", "c.txt", b"C"),
        ):
            target = restore_root / folder / leaf
            assert target.is_file(), f"{folder}/{leaf} missing — multi-source extraction broken"
            assert target.read_bytes() == payload


# ---------------------------------------------------------------------
# Non-regression: the single-source case must keep working — it was
# the original (correct) case before multi-source profiles became
# common. The fix removes a strip, but the strip was never needed for
# single-source either (collector.py always wraps with source folder).
# ---------------------------------------------------------------------


class TestSingleSourceStillRestoresCorrectly:
    PASSWORD = "another-test-password"

    def test_single_source_keeps_its_wrapping_folder(self, tmp_path: Path) -> None:
        """A single-source profile ``F:/MyFolder`` produces a tar with
        every entry under ``MyFolder/...``. Post-fix, the extraction
        creates ``restore_dir/MyFolder/...`` (matching the LOCAL
        non-encrypted path at ``_do_local_restore`` line ~1615 which
        also preserves source folder names via ``relative_to``).
        """
        tar_path = tmp_path / "solo_FULL_20260527.tar.wbenc"
        dest = tmp_path / "restore"
        dest.mkdir()

        _write_encrypted_tar(
            tar_path,
            self.PASSWORD,
            [
                ("MyFolder/doc.txt", b"hello"),
                ("MyFolder/sub/inner.txt", b"world"),
            ],
        )

        count = RecoveryTab._decrypt_and_extract(tar_path, dest, self.PASSWORD)

        restore_root = dest / "solo_FULL_20260527"
        assert count == 2
        assert (restore_root / "MyFolder" / "doc.txt").read_bytes() == b"hello"
        assert (restore_root / "MyFolder" / "sub" / "inner.txt").read_bytes() == b"world"
        # And NO flat extraction at the root.
        assert not (restore_root / "doc.txt").exists()


# ---------------------------------------------------------------------
# Defence: a wrong password must still raise the friendly RuntimeError.
# (Touches the same code path; the fix shouldn't break this.)
# ---------------------------------------------------------------------


class TestWrongPasswordStillRejected:
    def test_wrong_password_raises_runtime_error(self, tmp_path: Path) -> None:
        tar_path = tmp_path / "pwd_FULL_20260527.tar.wbenc"
        dest = tmp_path / "restore"
        dest.mkdir()

        _write_encrypted_tar(
            tar_path,
            "correct-password",
            [("X/a.txt", b"data")],
        )

        with pytest.raises(RuntimeError, match="password"):
            RecoveryTab._decrypt_and_extract(tar_path, dest, "wrong-password")
