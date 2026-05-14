"""Unit tests for ``src.ui._status_text.truncate_status_text``.

The helper backs both Run-tab and Verify-tab status lines. Tk Labels
do not clip their own text, so an unbounded filename pushes the
right-side percent counter off-screen. These tests pin the truncation
contract so any future tweak (max width, ellipsis style, phase
formatting) stays consistent across tabs.
"""

from __future__ import annotations

import pytest

from src.ui._status_text import STATUS_MAX_CHARS, truncate_status_text


class TestTruncateStatusText:
    """Truncation rules + boundary cases."""

    def test_short_path_passes_through_unchanged(self) -> None:
        assert truncate_status_text("hashing", "a.txt") == "hashing: a.txt"

    def test_short_without_phase_is_plain_filename(self) -> None:
        assert truncate_status_text("", "a.txt") == "a.txt"

    def test_empty_filename_returns_phase_only(self) -> None:
        assert truncate_status_text("hashing", "") == "hashing"

    def test_empty_filename_and_empty_phase_returns_empty(self) -> None:
        assert truncate_status_text("", "") == ""

    def test_long_path_keeps_the_end(self) -> None:
        """The basename + a few parents must survive truncation."""
        long_path = "Loic Perso/Lilian/Divorce/Avocat/Liz/Claude/cipango56@pm.me/mail_20260502.eml"
        out = truncate_status_text("Verifying", long_path, max_chars=40)
        assert len(out) <= 40
        assert out.startswith("Verifying: ...")
        # The very end of the path stays visible -- that's the point.
        assert out.endswith("mail_20260502.eml")

    def test_truncated_output_never_exceeds_max_chars(self) -> None:
        long_path = "x" * 500
        for max_chars in (20, 30, 50, 80, 120):
            out = truncate_status_text("hashing", long_path, max_chars=max_chars)
            assert len(out) <= max_chars, (
                f"max_chars={max_chars}: produced {len(out)} chars: {out!r}"
            )

    def test_default_max_is_exported_constant(self) -> None:
        """STATUS_MAX_CHARS is the public knob both tabs share."""
        assert STATUS_MAX_CHARS == 80
        long_path = "x" * 500
        out = truncate_status_text("hashing", long_path)
        assert len(out) <= STATUS_MAX_CHARS

    def test_degenerate_phase_longer_than_max(self) -> None:
        """When the phase name alone is wider than max_chars, the
        output still cannot exceed max_chars -- we hard-clip rather
        than crash the layout."""
        out = truncate_status_text("a" * 100, "file.txt", max_chars=20)
        assert len(out) <= 20

    @pytest.mark.parametrize("phase", ["hashing", "Verifying", "Uploading", ""])
    def test_phase_prefix_is_preserved_when_truncating(self, phase: str) -> None:
        long_path = "deep/nested/path/" * 20 + "file.bin"
        out = truncate_status_text(phase, long_path, max_chars=80)
        if phase:
            assert out.startswith(f"{phase}: "), out
        assert out.endswith("file.bin"), out
