"""Tests for RunTab progress bar calculation.

Verifies that the weighted progress bar correctly computes percentages
when phases report progress at different times.
"""

import tkinter as tk

import pytest

from src.core.events import EventBus
from src.ui.tabs.run_tab import RunTab


@pytest.fixture(scope="module")
def tk_root():
    """Create a Tk root for the entire module, destroy after."""
    root = tk.Tk()
    root.withdraw()
    yield root
    root.destroy()


@pytest.fixture()
def run_tab(tk_root):
    """Create a fresh RunTab for each test."""
    events = EventBus()
    tab = RunTab(tk_root, events=events)
    yield tab
    tab.destroy()


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
            run_tab._on_progress(current=i, total=10, filename=f"file{i}", phase="hashing")

        # Hashing weight=1, total_weight=1+2+1=4 → max 25%
        assert run_tab._last_pct == 25

    def test_local_backup_progress_flow(self, run_tab):
        """Full local backup: hashing(1) + backup(2) + verification(1) = 4."""
        run_tab._on_phase_count(weights={"hashing": 1, "backup": 2, "verification": 1})

        # Hashing: 5 files → 1/4 = 25%
        for i in range(1, 6):
            run_tab._on_progress(current=i, total=5, filename=f"f{i}", phase="hashing")
        assert run_tab._last_pct == 25

        # Backup: 5 files → 25 + 2/4*100 = 75%
        for i in range(1, 6):
            run_tab._on_progress(current=i, total=5, filename=f"f{i}", phase="backup")
        assert run_tab._last_pct == 75

        # Verification: 5 files → 75 + 1/4*100 = 99% (capped)
        for i in range(1, 6):
            run_tab._on_progress(current=i, total=5, filename=f"f{i}", phase="verification")
        assert run_tab._last_pct == 99

    def test_remote_backup_progress_flow(self, run_tab):
        """Remote backup: hashing(1) + upload(5) + verification(1) = 7."""
        run_tab._on_phase_count(weights={"hashing": 1, "upload": 5, "verification": 1})

        # Hashing: 100% → 1/7 ≈ 14%
        run_tab._on_progress(current=3, total=3, filename="f", phase="hashing")
        assert run_tab._last_pct == 14

        # Upload halfway: 1/7*100 + 5/7*50 = 14.28 + 35.71 = 50%
        run_tab._on_progress(current=5, total=10, filename="f", phase="upload")
        assert run_tab._last_pct == 50

    def test_progress_monotonic(self, run_tab):
        """Progress never goes backwards."""
        run_tab._on_phase_count(weights={"hashing": 1, "backup": 2})

        run_tab._on_progress(current=5, total=10, filename="f", phase="hashing")
        pct_after_half = run_tab._last_pct

        # Even with weird lower values, monotonic holds
        run_tab._on_progress(current=3, total=10, filename="f", phase="hashing")
        assert run_tab._last_pct >= pct_after_half

    def test_no_weights_declared_fallback(self, run_tab):
        """Without PHASE_COUNT, each seen phase gets weight=1."""
        # No _on_phase_count call — simulate missing event

        run_tab._on_progress(current=10, total=10, filename="f", phase="hashing")
        # Only one phase known, weight=1/1 → 99% (capped)
        assert run_tab._last_pct == 99

    def test_undeclared_phase_gets_default_weight(self, run_tab):
        """A phase not in weights dict gets default weight=1."""
        run_tab._on_phase_count(weights={"hashing": 1, "backup": 2})

        # "unknown_phase" not declared — should get weight=1
        # total_weight = 1 + 2 + 1 = 4
        run_tab._on_progress(current=10, total=10, filename="f", phase="unknown_phase")
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
        run_tab._on_progress(current=5, total=5, filename="f", phase="hashing")
        assert run_tab._last_pct == 11

    def test_remote_with_rotation_progress(self, run_tab):
        """Remote backup with rotation: hashing(1)+upload(5)+verification(1)+rotation(1)=8."""
        run_tab._on_phase_count(
            weights={"hashing": 1, "upload": 5, "verification": 1, "rotation": 1}
        )

        # Hashing done → 1/8 = 12%
        run_tab._on_progress(current=10, total=10, filename="f", phase="hashing")
        assert run_tab._last_pct == 12

        # Upload done → (1+5)/8 = 75%
        run_tab._on_progress(current=100, total=100, filename="f", phase="upload")
        assert run_tab._last_pct == 75

        # Verification done → (1+5+1)/8 = 87%
        run_tab._on_progress(current=5, total=5, filename="f", phase="verification")
        assert run_tab._last_pct == 87

        # Rotation halfway → 87 + 1/8*50 = 93%
        run_tab._on_progress(current=3, total=6, filename="old", phase="rotation")
        assert run_tab._last_pct == 93

        # Rotation done → 99% (capped)
        run_tab._on_progress(current=6, total=6, filename="old", phase="rotation")
        assert run_tab._last_pct == 99

    def test_zero_total_ignored(self, run_tab):
        """Progress with total=0 is silently ignored."""
        run_tab._on_phase_count(weights={"hashing": 1})
        run_tab._on_progress(current=0, total=0, filename="f", phase="hashing")
        assert run_tab._last_pct == 0
