"""Regression test pinning the v3.7.8 fix for the 6 s profile-switch freeze.

Root cause (v3.7.7 instrumentation, 2026-05-17 evidence):
``RecoveryTab._update_post_source_sections`` ran
``src.rglob("*.wbenc")`` on the storage destination path. The path
typically points at a USB HDD root holding 268 k+ files across all
profiles' backups. Walking the entire tree took 2.9-6.6 s per call,
and the call fired on every profile switch via the trace chain
``load_profile → source_type_var.set → _on_source_type_changed →
_update_post_source_sections``.

Fix: ``rglob`` → ``glob``. Encrypted backups are always written as
``{backup_name}.tar.wbenc`` at the storage root (see
``local_writer.py::write_encrypted_tar`` and
``backup_engine.py::_phase_write``), never nested inside a backup
directory. A shallow glob is functionally equivalent and runs in
microseconds.

This test pins the contract at the source level — any future
refactor that re-introduces ``rglob`` (or any other recursive walk
in this code path) trips immediately.
"""

from __future__ import annotations

import ast
import inspect
import re
import textwrap

from src.ui.tabs.recovery_tab import RecoveryTab


def _executable_source(method) -> str:
    """Return the method source with comment lines stripped.

    Naive ``"rglob" in source`` matches the rationale comment that
    documents the very regression we are blocking. AST round-trip
    drops all comments and docstrings, so the remaining text is the
    executable code only.
    """
    src = textwrap.dedent(inspect.getsource(method))
    tree = ast.parse(src)
    # ast.unparse rebuilds source from the tree without comments. The
    # function docstring survives but is irrelevant for the substring
    # checks below.
    return ast.unparse(tree)


def test_update_post_source_sections_uses_shallow_glob_only() -> None:
    """The local-encrypted-detection branch must not walk recursively.

    The 17/05/2026 instrumentation showed this single call dominating
    99.8 % of the profile-switch wall-clock on a USB HDD destination.
    A regression here is silent in unit tests (tmp_path is NVMe) so
    the contract is pinned by source inspection, not by a runtime
    timing assertion.
    """
    code = _executable_source(RecoveryTab._update_post_source_sections)
    # Match a real attribute call ``.rglob(`` rather than the bare word,
    # so the explanatory docstring (which mentions rglob by name) does
    # not trip the assertion.
    assert not re.search(r"\.rglob\s*\(", code), (
        "_update_post_source_sections must not call rglob — encrypted "
        "backups are at the storage root, so a shallow glob is enough. "
        "rglob walks the entire USB tree (~3-6 s on a 268 k-file dest)."
    )
    # Positive check: the shallow glob is still there. Without this we
    # would also pass on a refactor that drops the detection entirely
    # — which would silently hide the password field for legitimately
    # encrypted local profiles.
    assert re.search(r"\.glob\s*\(\s*['\"]\*\.wbenc['\"]\s*\)", code), (
        "the shallow .wbenc detection on local destinations is gone — "
        "if intentional, update the password-field logic accordingly."
    )
