"""Tests for RecoveryTab path-traversal defense during tar extraction."""

import os
import subprocess
from pathlib import Path

import pytest

from src.ui.tabs.recovery_tab import _is_within_restore_dir


class TestIsWithinRestoreDir:
    """Tar archive members must not be allowed to escape the restore dir."""

    def test_normal_member_accepted(self, tmp_path: Path) -> None:
        """A member that extracts to a subpath of restore_dir is allowed."""
        restore_dir = tmp_path / "restore"
        restore_dir.mkdir()
        target = restore_dir / "sub" / "file.txt"
        assert _is_within_restore_dir(target, restore_dir) is True

    def test_member_at_root_accepted(self, tmp_path: Path) -> None:
        """A member at the root of restore_dir is allowed."""
        restore_dir = tmp_path / "restore"
        restore_dir.mkdir()
        target = restore_dir / "file.txt"
        assert _is_within_restore_dir(target, restore_dir) is True

    def test_parent_traversal_rejected(self, tmp_path: Path) -> None:
        """A member with '..' segments that escape restore_dir is rejected."""
        restore_dir = tmp_path / "restore"
        restore_dir.mkdir()
        # restore_dir / "../../etc/passwd" resolves above restore_dir
        target = restore_dir / ".." / ".." / "etc" / "passwd"
        assert _is_within_restore_dir(target, restore_dir) is False

    def test_parent_traversal_that_stays_inside_accepted(self, tmp_path: Path) -> None:
        """A '..' that still resolves inside restore_dir is allowed."""
        restore_dir = tmp_path / "restore"
        (restore_dir / "a").mkdir(parents=True)
        # restore_dir / "a/../file.txt" resolves to restore_dir / "file.txt"
        target = restore_dir / "a" / ".." / "file.txt"
        assert _is_within_restore_dir(target, restore_dir) is True

    def test_absolute_path_outside_rejected(self, tmp_path: Path) -> None:
        """An absolute sibling path outside restore_dir is rejected."""
        restore_dir = tmp_path / "restore"
        restore_dir.mkdir()
        sibling = tmp_path / "other" / "file.txt"
        assert _is_within_restore_dir(sibling, restore_dir) is False

    def test_resolve_failure_rejected(self, tmp_path: Path, monkeypatch) -> None:
        """If path resolution raises, the member is conservatively rejected."""
        restore_dir = tmp_path / "restore"
        restore_dir.mkdir()
        target = restore_dir / "file.txt"

        def fail_resolve(self, strict: bool = False):
            raise OSError("simulated filesystem failure")

        monkeypatch.setattr(Path, "resolve", fail_resolve)
        assert _is_within_restore_dir(target, restore_dir) is False

    def test_hostile_absolute_system_path_rejected(self, tmp_path: Path) -> None:
        """A tar member with an absolute OS path must be rejected.

        Reproduces the concrete attack: a malicious archive contains a
        member named ``C:\\Windows\\System32\\evil.dll`` (or
        ``/etc/passwd`` on POSIX).  ``Path.__truediv__`` with an
        absolute right-hand side ignores ``restore_dir`` entirely, so
        ``target = restore_dir / name`` resolves to the absolute path.
        The extraction loop must refuse to write there.
        """
        restore_dir = tmp_path / "restore"
        restore_dir.mkdir()

        hostile_name = "C:\\Windows\\System32\\evil.dll" if os.name == "nt" else "/etc/passwd"

        # This is exactly how the extraction loop builds the target:
        # Path / absolute drops the left operand on Windows and POSIX.
        simulated_target = restore_dir / hostile_name
        assert simulated_target.is_absolute()
        assert _is_within_restore_dir(simulated_target, restore_dir) is False

    @pytest.mark.skipif(os.name != "nt", reason="Windows-specific junction test")
    def test_windows_junction_escape_rejected(self, tmp_path: Path) -> None:
        """A Windows junction inside restore_dir pointing outside is rejected.

        ``Path.resolve()`` follows junctions since Python 3.8 (via
        ``GetFinalPathNameByHandle``), so a junction planted inside
        the restore directory that redirects writes to, e.g.,
        ``C:\\Windows`` is caught by the ``relative_to`` check.
        """
        restore_dir = tmp_path / "restore"
        restore_dir.mkdir()
        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()

        junction_path = restore_dir / "evil_junction"
        # ``mklink /J`` creates a directory junction and does not
        # require elevation on modern Windows (unlike ``/D`` symlinks).
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junction_path), str(outside_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip(
                f"Cannot create junction on this host: " f"{result.stdout or result.stderr}"
            )

        # A tar member name like "evil_junction/payload.txt" would
        # land inside restore_dir on the filesystem, but resolve()
        # follows the junction and the canonical target is outside.
        target_via_junction = junction_path / "payload.txt"
        assert _is_within_restore_dir(target_via_junction, restore_dir) is False

    @pytest.mark.skipif(os.name != "nt", reason="Windows-specific junction test")
    def test_windows_junction_inside_restore_still_accepted(self, tmp_path: Path) -> None:
        """A junction that points to another subdir of restore_dir is allowed.

        Paranoia check: the defense must only block junctions that
        escape, not the pathological but benign case of a junction
        pointing to a sibling inside the same restore_dir.
        """
        restore_dir = tmp_path / "restore"
        (restore_dir / "real").mkdir(parents=True)

        junction_path = restore_dir / "alias"
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junction_path), str(restore_dir / "real")],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip(
                f"Cannot create junction on this host: " f"{result.stdout or result.stderr}"
            )

        target_via_junction = junction_path / "payload.txt"
        assert _is_within_restore_dir(target_via_junction, restore_dir) is True
