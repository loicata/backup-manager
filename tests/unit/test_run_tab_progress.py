"""Tests for RunTab progress bar calculation.

Verifies that the weighted progress bar correctly computes percentages
when phases report progress at different times.

Note: _on_progress() schedules via Tk.after(0, ...) so we must call
update_idletasks() to flush the queue before asserting on _last_pct.
"""

import pytest

from src.core.events import EventBus
from src.ui._status_text import truncate_status_text
from src.ui.tabs.run_tab import RunTab


@pytest.fixture()
def run_tab(tk_root):
    """Create a fresh RunTab for each test."""
    events = EventBus()
    tab = RunTab(tk_root, events=events)
    yield tab
    tab.destroy()


def _progress(run_tab, **kwargs):
    """Call _update_progress directly, bypassing Tk.after() scheduling.

    In production, _on_progress() defers to _update_progress() via
    self.after(0, ...). In tests, the Tk event loop is not running,
    so we call the underlying method directly.
    """
    run_tab._update_progress(
        kwargs.get("current", 0),
        kwargs.get("total", 0),
        kwargs.get("filename", ""),
        kwargs.get("phase", ""),
    )


class TestProgressCalculation:
    """Test the weighted progress bar logic."""

    def test_hashing_only_phase_does_not_reach_99(self, run_tab):
        """Bug regression: hashing alone must NOT reach 99%.

        When phase weights are declared (hashing=1, backup=2, verification=1),
        completing hashing should only fill its share (1/4 = 25%), not 99%.
        """
        # Simulate engine declaring weights before any phase starts
        run_tab._on_phase_count(weights={"hashing": 1, "backup": 2, "verification": 1})

        # Simulate hashing completing all 10 files
        for i in range(1, 11):
            _progress(run_tab, current=i, total=10, filename=f"file{i}", phase="hashing")

        # Hashing weight=1, total_weight=1+2+1=4 → max 25%
        assert run_tab._last_pct == 25

    def test_local_backup_progress_flow(self, run_tab):
        """Full local backup: hashing(1) + backup(2) + verification(1) = 4."""
        run_tab._on_phase_count(weights={"hashing": 1, "backup": 2, "verification": 1})

        # Hashing: 5 files → 1/4 = 25%
        for i in range(1, 6):
            _progress(run_tab, current=i, total=5, filename=f"f{i}", phase="hashing")
        assert run_tab._last_pct == 25

        # Backup: 5 files → 25 + 2/4*100 = 75%
        for i in range(1, 6):
            _progress(run_tab, current=i, total=5, filename=f"f{i}", phase="backup")
        assert run_tab._last_pct == 75

        # Verification: 5 files → 75 + 1/4*100 = 99% (capped)
        for i in range(1, 6):
            _progress(run_tab, current=i, total=5, filename=f"f{i}", phase="verification")
        assert run_tab._last_pct == 99

    def test_remote_backup_progress_flow(self, run_tab):
        """Remote backup: hashing(1) + upload(5) + verification(1) = 7."""
        run_tab._on_phase_count(weights={"hashing": 1, "upload": 5, "verification": 1})

        # Hashing: 100% → 1/7 ≈ 14%
        _progress(run_tab, current=3, total=3, filename="f", phase="hashing")
        assert run_tab._last_pct == 14

        # Upload halfway: 1/7*100 + 5/7*50 = 14.28 + 35.71 = 50%
        _progress(run_tab, current=5, total=10, filename="f", phase="upload")
        assert run_tab._last_pct == 50

    def test_progress_monotonic(self, run_tab):
        """Progress never goes backwards."""
        run_tab._on_phase_count(weights={"hashing": 1, "backup": 2})

        _progress(run_tab, current=5, total=10, filename="f", phase="hashing")
        pct_after_half = run_tab._last_pct

        # Even with weird lower values, monotonic holds
        _progress(run_tab, current=3, total=10, filename="f", phase="hashing")
        assert run_tab._last_pct >= pct_after_half

    def test_no_weights_declared_fallback(self, run_tab):
        """Without PHASE_COUNT, each seen phase gets weight=1."""
        # No _on_phase_count call — simulate missing event

        _progress(run_tab, current=10, total=10, filename="f", phase="hashing")
        # Only one phase known, weight=1/1 → 99% (capped)
        assert run_tab._last_pct == 99

    def test_undeclared_phase_gets_default_weight(self, run_tab):
        """A phase not in weights dict gets default weight=1."""
        run_tab._on_phase_count(weights={"hashing": 1, "backup": 2})

        # "unknown_phase" not declared — should get weight=1
        # total_weight = 1 + 2 + 1 = 4
        _progress(run_tab, current=10, total=10, filename="f", phase="unknown_phase")
        assert run_tab._last_pct == 25  # 1/4 * 100

    def test_mirror_upload_weight(self, run_tab):
        """Backup with mirror: hashing(1) + backup(2) + verification(1) + mirror(5) = 9."""
        run_tab._on_phase_count(
            weights={
                "hashing": 1,
                "backup": 2,
                "verification": 1,
                "mirror_upload": 5,
            }
        )

        # Hashing done → 1/9 ≈ 11%
        _progress(run_tab, current=5, total=5, filename="f", phase="hashing")
        assert run_tab._last_pct == 11

    def test_remote_with_rotation_progress(self, run_tab):
        """Remote backup with rotation: hashing(1)+upload(5)+verification(1)+rotation(1)=8."""
        run_tab._on_phase_count(
            weights={"hashing": 1, "upload": 5, "verification": 1, "rotation": 1}
        )

        # Hashing done → 1/8 = 12%
        _progress(run_tab, current=10, total=10, filename="f", phase="hashing")
        assert run_tab._last_pct == 12

        # Upload done → (1+5)/8 = 75%
        _progress(run_tab, current=100, total=100, filename="f", phase="upload")
        assert run_tab._last_pct == 75

        # Verification done → (1+5+1)/8 = 87%
        _progress(run_tab, current=5, total=5, filename="f", phase="verification")
        assert run_tab._last_pct == 87

        # Rotation halfway → 87 + 1/8*50 = 93%
        _progress(run_tab, current=3, total=6, filename="old", phase="rotation")
        assert run_tab._last_pct == 93

        # Rotation done → 99% (capped)
        _progress(run_tab, current=6, total=6, filename="old", phase="rotation")
        assert run_tab._last_pct == 99

    def test_zero_total_ignored(self, run_tab):
        """Progress with total=0 is silently ignored."""
        run_tab._on_phase_count(weights={"hashing": 1})
        _progress(run_tab, current=0, total=0, filename="f", phase="hashing")
        assert run_tab._last_pct == 0


# ---------------------------------------------------------------------
# Status-line truncation (separate concern from progress arithmetic).
#
# Bug regression: when the file being processed had a long path, the
# "phase: filename" label took the full row width and pushed the
# adjacent percent label off the visible area. The user perceived the
# backup as "stuck" because no % was visible. The fix combines a pack
# reorder (covered by manual UI inspection) with a text truncation
# helper that bounds the label content — the helper is what we test
# here without needing a Tk root.
# ---------------------------------------------------------------------


class TestTruncateStatusText:
    """Pure-Python helper that bounds the progress status line length."""

    def test_short_text_unchanged(self):
        """No truncation when phase + filename fit in budget."""
        assert truncate_status_text("hashing", "small.txt", max_chars=80) == ("hashing: small.txt")

    def test_long_path_truncated_with_leading_ellipsis(self):
        """Long paths must keep the basename visible — start gets clipped."""
        long_path = (
            "Loic Perso/Lilian/Divorce/Avocat/"
            "Liz McKeever Hodgins Mckeever Solicitors/Claude/"
            "Protonmail/cipango56@pm.me/mail_20260502_08.eml"
        )
        result = truncate_status_text("hashing", long_path, max_chars=80)
        assert len(result) <= 80
        assert result.startswith("hashing: ...")
        # The most informative tail (the actual filename) survives.
        assert result.endswith("mail_20260502_08.eml")

    def test_truncation_never_exceeds_max_chars(self):
        """Hard upper bound — the layout reserves a fixed pixel width."""
        for max_chars in (20, 40, 80, 120):
            result = truncate_status_text("phase", "x" * 500, max_chars=max_chars)
            assert (
                len(result) <= max_chars
            ), f"Returned {len(result)} chars for max_chars={max_chars}"

    def test_empty_filename_returns_phase_only(self):
        """No file → just the phase name (e.g. 'Filtering changed files...')."""
        assert truncate_status_text("filtering", "", max_chars=80) == "filtering"

    def test_empty_phase_uses_filename_only(self):
        """Defensive: if phase is missing, still produce a useful label."""
        assert truncate_status_text("", "small.txt", max_chars=80) == "small.txt"

    def test_empty_phase_with_long_filename(self):
        """No phase + long path → leading ellipsis only, no 'phase: ' prefix."""
        long_name = "a/" * 80 + "leaf.txt"
        result = truncate_status_text("", long_name, max_chars=30)
        assert len(result) <= 30
        assert result.startswith("...")
        assert result.endswith("leaf.txt")

    def test_boundary_exact_max_chars(self):
        """Length-equal-to-max must not be truncated (off-by-one guard)."""
        text = "phase: " + "x" * 73  # 80 chars total
        assert len(text) == 80
        result = truncate_status_text("phase", "x" * 73, max_chars=80)
        assert result == text

    def test_phase_name_longer_than_max_falls_back_to_clip(self):
        """Degenerate input — phase alone exceeds budget. Don't crash."""
        long_phase = "a" * 50
        long_name = "b" * 50
        result = truncate_status_text(long_phase, long_name, max_chars=20)
        # Returned string respects the cap even though the prefix alone
        # was bigger than max_chars.
        assert len(result) <= 20
