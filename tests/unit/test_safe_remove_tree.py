r"""Tests for src.storage._fs_utils.safe_remove_tree.

Covers the robust tree-removal helper that replaces the silent
``shutil.rmtree(onerror=_force_remove_readonly)`` pattern.

Scenarios covered:
- Empty / nonexistent paths return success with zero residuals.
- A regular tree is fully removed (counts files vs dirs).
- Read-only files are removed after attribute clearing (no manual
  chmod by the caller).
- Long Windows paths (>260 chars) are reachable thanks to the
  ``\\?\`` extended-length prefix; the legacy code silently failed.
- Transient ``PermissionError`` triggers exponential-backoff retries
  before falling back to a residual entry.
- Unrecoverable failures land in the residuals list, not as exceptions.
- The result object exposes ``success`` and structured residuals
  for callers (rotator, delete_backup) to act on.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest

# Skip readonly-attribute test on POSIX where chmod 0o444 leaves the
# parent directory writable so unlink succeeds anyway — the scenario
# we care about is Windows-specific.
WINDOWS_ONLY = pytest.mark.skipif(os.name != "nt", reason="Windows-specific attribute behavior")


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """Three-level tree with files at each depth.

    Layout::

        root/
            a.txt
            sub1/
                b.txt
                sub2/
                    c.txt
    """
    root = tmp_path / "root"
    root.mkdir()
    (root / "a.txt").write_text("a", encoding="utf-8")
    sub1 = root / "sub1"
    sub1.mkdir()
    (sub1 / "b.txt").write_text("b", encoding="utf-8")
    sub2 = sub1 / "sub2"
    sub2.mkdir()
    (sub2 / "c.txt").write_text("c", encoding="utf-8")
    return root


class TestSafeRemoveTreeBasics:
    """Happy-path and trivial cases."""

    def test_nonexistent_path_returns_success(self, tmp_path: Path) -> None:
        from src.storage._fs_utils import safe_remove_tree

        result = safe_remove_tree(tmp_path / "does_not_exist")
        assert result.success is True
        assert result.removed_files == 0
        assert result.removed_dirs == 0
        assert result.residuals == []

    def test_empty_directory(self, tmp_path: Path) -> None:
        from src.storage._fs_utils import safe_remove_tree

        target = tmp_path / "empty"
        target.mkdir()

        result = safe_remove_tree(target)
        assert result.success is True
        assert result.removed_files == 0
        assert result.removed_dirs == 1
        assert not target.exists()

    def test_single_file(self, tmp_path: Path) -> None:
        from src.storage._fs_utils import safe_remove_tree

        target = tmp_path / "lonely.txt"
        target.write_text("data", encoding="utf-8")

        result = safe_remove_tree(target)
        assert result.success is True
        assert result.removed_files == 1
        assert result.removed_dirs == 0
        assert not target.exists()

    def test_full_tree_removed_with_correct_counts(self, tree: Path) -> None:
        from src.storage._fs_utils import safe_remove_tree

        result = safe_remove_tree(tree)
        assert result.success is True
        assert result.removed_files == 3  # a.txt, b.txt, c.txt
        assert result.removed_dirs == 3  # root, sub1, sub2
        assert result.residuals == []
        assert not tree.exists()


class TestSafeRemoveTreeReadOnly:
    """Files with READONLY attribute must be removed transparently."""

    @WINDOWS_ONLY
    def test_readonly_file_inside_tree(self, tree: Path) -> None:
        from src.storage._fs_utils import safe_remove_tree

        locked = tree / "sub1" / "b.txt"
        locked.chmod(stat.S_IREAD)

        result = safe_remove_tree(tree)
        assert result.success is True, f"Residuals: {result.residuals}"
        assert not tree.exists()

    @WINDOWS_ONLY
    def test_readonly_root_directory(self, tmp_path: Path) -> None:
        from src.storage._fs_utils import safe_remove_tree

        target = tmp_path / "ro_root"
        target.mkdir()
        (target / "child.txt").write_text("c", encoding="utf-8")
        (target / "child.txt").chmod(stat.S_IREAD)

        result = safe_remove_tree(target)
        assert result.success is True
        assert not target.exists()


class TestSafeRemoveTreeLongPath:
    """Paths exceeding the legacy 260-char MAX_PATH must work on Windows."""

    @WINDOWS_ONLY
    def test_long_path_tree_is_fully_removed(self, tmp_path: Path) -> None:
        from src.storage._fs_utils import safe_remove_tree

        # Build a tree whose deepest absolute path exceeds 260 chars.
        # Creating the tree itself requires the long-path prefix —
        # plain ``Path.mkdir`` hits the same MAX_PATH wall we are
        # testing against, so we use os.makedirs on a ``\\?\`` path.
        root = tmp_path / "long_root"
        root.mkdir()
        segments = [f"segment_{i:02d}_" + "x" * 22 for i in range(8)]
        deep_rel = Path(*segments)
        deep_abs = root / deep_rel
        deep_long = "\\\\?\\" + str(deep_abs)
        os.makedirs(deep_long, exist_ok=True)

        leaf_name = "leaf_" + "y" * 200 + ".txt"
        leaf_long = deep_long + "\\" + leaf_name
        with open(leaf_long, "w", encoding="utf-8") as f:
            f.write("data")

        # Sanity: the un-prefixed leaf path must exceed MAX_PATH.
        leaf_unprefixed = str(deep_abs / leaf_name)
        assert (
            len(leaf_unprefixed) > 260
        ), f"Test fixture must produce >260-char path, got {len(leaf_unprefixed)}"

        result = safe_remove_tree(root)
        assert result.success is True, f"Residuals: {result.residuals}"
        # Use long-path-aware probe to confirm removal — Path.exists()
        # would return False on a still-present long-path tree.
        assert not os.path.exists("\\\\?\\" + str(root))


class TestSafeRemoveTreeRetry:
    """Transient ``PermissionError`` should be retried before giving up."""

    def test_retries_on_transient_permission_error(self, tmp_path: Path) -> None:
        from src.storage import _fs_utils
        from src.storage._fs_utils import safe_remove_tree

        target = tmp_path / "flaky.txt"
        target.write_text("x", encoding="utf-8")

        original_unlink = os.unlink
        call_count = {"n": 0}

        def flaky_unlink(path: str) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                # First call fails as if antivirus held the file briefly
                raise PermissionError(13, "Access denied (transient)")
            return original_unlink(path)

        with patch.object(_fs_utils.os, "unlink", side_effect=flaky_unlink):
            result = safe_remove_tree(target, base_delay=0.01)

        assert result.success is True, f"Residuals: {result.residuals}"
        assert call_count["n"] >= 2, "unlink must be retried after transient failure"
        assert not target.exists()

    def test_hard_failure_recorded_as_residual(self, tmp_path: Path) -> None:
        """A non-recoverable error becomes a residual, not an exception."""
        from src.storage import _fs_utils
        from src.storage._fs_utils import safe_remove_tree

        target = tmp_path / "doomed.txt"
        target.write_text("x", encoding="utf-8")

        def always_fail(path: str) -> None:
            raise PermissionError(13, "Access denied (permanent)")

        with patch.object(_fs_utils.os, "unlink", side_effect=always_fail):
            result = safe_remove_tree(target, max_retries=2, base_delay=0.0)

        assert result.success is False
        assert len(result.residuals) == 1
        assert "doomed.txt" in result.residuals[0].path
        assert "PermissionError" in result.residuals[0].error

    def test_residual_propagates_to_parent(self, tree: Path) -> None:
        """If a child file cannot be removed, the parent dir stays as residual."""
        from src.storage import _fs_utils
        from src.storage._fs_utils import safe_remove_tree

        original_unlink = os.unlink
        target_file = str(tree / "sub1" / "sub2" / "c.txt")

        def selective_fail(path: str) -> None:
            # The Windows long-path code path may add a \\?\ prefix
            # before calling unlink; match on the suffix to stay
            # path-encoding agnostic.
            if path.endswith("c.txt"):
                raise PermissionError(13, "Access denied")
            return original_unlink(path)

        with patch.object(_fs_utils.os, "unlink", side_effect=selective_fail):
            result = safe_remove_tree(tree, max_retries=1, base_delay=0.0)

        assert result.success is False
        residual_paths = [r.path for r in result.residuals]
        # Both the leaf file and every dir on its way up should appear
        assert any("c.txt" in p for p in residual_paths)
        assert any("sub2" in p for p in residual_paths)
        # And the original target_file should still exist on disk
        assert Path(target_file).exists()


class TestRemoveResultDataclass:
    """Result object surface — callers rely on these attributes."""

    def test_success_property_reflects_residuals(self, tmp_path: Path) -> None:
        from src.storage._fs_utils import RemoveResult, Residual

        empty = RemoveResult()
        assert empty.success is True

        with_residual = RemoveResult(
            residuals=[Residual(path=str(tmp_path), error="OSError: boom")]
        )
        assert with_residual.success is False

    def test_residual_str_repr(self) -> None:
        from src.storage._fs_utils import Residual

        r = Residual(path=r"C:\foo\bar", error="PermissionError: [WinError 5]")
        # Must be safely string-able (used in log formatting and exception messages)
        s = str(r)
        assert "foo" in s
        assert "PermissionError" in s
