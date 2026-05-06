"""Tests for the personal-mode wizard step that picks schedule frequency.

Added in 3.4.0. Before this step existed, every wizard run hard-coded
``ScheduleFrequency.WEEKLY`` — users had to edit the profile from the
Schedule tab afterwards. The step lets them pick Daily / Weekly /
Monthly directly during onboarding.

These tests cover:
* The step is wired into the personal-mode flow as step 4 of 4.
* The default selection is Weekly (matches the historical hard-code).
* ``_create_profile`` reads the chosen value from ``_data``.
* A missing or invalid stored value falls back to Weekly safely.
* The Next button label flips to "Finish" on the last step.
* The 3 cards render without raising under a real Tk root.
"""

from __future__ import annotations

import tkinter as tk

import pytest

from src.core.config import (
    BackupProfile,
    ScheduleFrequency,
    StorageType,
)
from src.ui.wizard import MODE_PERSONAL, SetupWizard


@pytest.fixture()
def wizard(tk_root):
    """Build a fresh SetupWizard against the session Tk root.

    The wizard's ``__init__`` creates a Toplevel and starts the
    wizard-internal data structures without entering ``run()``, so the
    Tk event loop is NOT started — tests can poke at internal methods
    directly.
    """
    w = SetupWizard(tk_root, standalone=True)
    yield w
    # Best-effort teardown — the Toplevel may have been destroyed
    # already by ``_create_profile``; ignore if so.
    try:
        if w._win.winfo_exists():
            w._win.destroy()
    except tk.TclError:
        pass


# ---------------------------------------------------------------------------
# Step wiring
# ---------------------------------------------------------------------------


class TestStepWiring:
    """The new step must be reachable from the personal-mode flow."""

    def test_personal_mode_has_four_steps(self, wizard) -> None:
        """Adding the schedule step must extend ``_total_steps`` from 3 to 4."""
        wizard._select_mode(MODE_PERSONAL)
        assert wizard._total_steps == 4

    def test_step_four_builder_is_schedule_frequency(self, wizard) -> None:
        """``_show_step`` must dispatch step 4 to the new builder method."""
        wizard._select_mode(MODE_PERSONAL)
        wizard._step = 4
        # Capture which builder runs by patching the personal-mode
        # builders. We use a flag set by a wrapper around the real
        # method so the rest of _show_step still works.
        called = {"hit": False}
        original = wizard._step_schedule_frequency

        def wrapper():
            called["hit"] = True
            return original()

        wizard._step_schedule_frequency = wrapper
        wizard._show_step()
        assert called["hit"], "Step 4 did not dispatch to _step_schedule_frequency"


# ---------------------------------------------------------------------------
# Default selection
# ---------------------------------------------------------------------------


class TestDefaultSelection:
    """Pre-selecting Weekly preserves the historical default."""

    def test_default_is_weekly(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._step = 4
        wizard._show_step()
        assert wizard._data["schedule_frequency"] == ScheduleFrequency.WEEKLY.value

    def test_revisit_keeps_user_selection(self, wizard) -> None:
        """If the user picked daily then went Back / Next, the daily
        selection must rehydrate the radio."""
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.DAILY.value
        wizard._step = 4
        wizard._show_step()
        assert wizard._schedule_var.get() == ScheduleFrequency.DAILY.value


# ---------------------------------------------------------------------------
# Variable wiring → _data sync
# ---------------------------------------------------------------------------


class TestVarSyncsToData:
    """Changing the StringVar must update ``_data["schedule_frequency"]``."""

    def test_setting_var_updates_data(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._step = 4
        wizard._show_step()

        wizard._schedule_var.set(ScheduleFrequency.MONTHLY.value)
        wizard._win.update_idletasks()
        assert wizard._data["schedule_frequency"] == ScheduleFrequency.MONTHLY.value


# ---------------------------------------------------------------------------
# _create_profile reads the stored frequency
# ---------------------------------------------------------------------------


def _seed_required_data(wizard, frequency: str | None) -> None:
    """Populate the minimum data needed for ``_create_profile`` to run."""
    wizard._data["name"] = "TestProfile"
    wizard._data["sources"] = ["C:\\does\\not\\matter"]
    # Match the format expected by _build_storage_config_from_key:
    # a "storage" sub-dict with a "type" string and a "vars" dict.
    wizard._data["storage"] = {
        "type": StorageType.LOCAL.value,
        "vars": {"destination_path": "C:\\backups"},
    }
    if frequency is not None:
        wizard._data["schedule_frequency"] = frequency


class TestCreateProfileUsesChoice:
    """The created BackupProfile must reflect the chosen frequency."""

    @pytest.mark.parametrize(
        "choice,expected",
        [
            (ScheduleFrequency.DAILY.value, ScheduleFrequency.DAILY),
            (ScheduleFrequency.WEEKLY.value, ScheduleFrequency.WEEKLY),
            (ScheduleFrequency.MONTHLY.value, ScheduleFrequency.MONTHLY),
        ],
    )
    def test_profile_frequency_matches_choice(
        self, wizard, choice: str, expected: ScheduleFrequency
    ) -> None:
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, choice)

        wizard._create_profile()

        assert isinstance(wizard.result_profile, BackupProfile)
        assert wizard.result_profile.schedule.frequency == expected

    def test_profile_falls_back_to_weekly_when_data_missing(self, wizard) -> None:
        """A code path that bypasses the wizard navigation must still
        get the historical default — never crash with KeyError."""
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, None)

        wizard._create_profile()

        assert wizard.result_profile.schedule.frequency == ScheduleFrequency.WEEKLY

    def test_profile_falls_back_on_corrupted_value(self, wizard) -> None:
        """Defensive path: a corrupted ``_data`` entry must not abort
        profile creation — degrade to WEEKLY."""
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, "bogus_freq_value_xyz")

        wizard._create_profile()

        assert wizard.result_profile.schedule.frequency == ScheduleFrequency.WEEKLY


# ---------------------------------------------------------------------------
# Retention defaults adapt to the chosen frequency
# ---------------------------------------------------------------------------


class TestRetentionAdaptsToFrequency:
    """Daily-frequency profiles get ``gfs_daily=8`` so the Retention
    tab displays "Days of history: 7" (UI value = internal - 1).
    Other frequencies keep the historical ``gfs_daily=1`` since the
    Retention tab hides the Daily row when the schedule isn't daily.
    """

    def test_daily_yields_seven_days_of_history(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, ScheduleFrequency.DAILY.value)

        wizard._create_profile()

        # 8 internal => 7 displayed in the Retention tab.
        assert wizard.result_profile.retention.gfs_daily == 8

    def test_weekly_keeps_historical_daily_default(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, ScheduleFrequency.WEEKLY.value)

        wizard._create_profile()

        # Daily row is hidden for weekly schedule — keep the legacy 1.
        assert wizard.result_profile.retention.gfs_daily == 1

    def test_monthly_keeps_historical_daily_default(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, ScheduleFrequency.MONTHLY.value)

        wizard._create_profile()

        assert wizard.result_profile.retention.gfs_daily == 1

    @pytest.mark.parametrize(
        "freq",
        [
            ScheduleFrequency.DAILY.value,
            ScheduleFrequency.WEEKLY.value,
            ScheduleFrequency.MONTHLY.value,
        ],
    )
    def test_weekly_and_monthly_unchanged_regardless_of_frequency(
        self, wizard, freq: str
    ) -> None:
        """gfs_weekly / gfs_monthly stay at their historical defaults
        for ALL frequencies — only the daily row was in scope for
        this fix.

        Parametrised (rather than looped) so each iteration gets a
        fresh wizard fixture; ``_create_profile`` destroys the
        Toplevel, so a single wizard can only complete one round.
        """
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, freq)
        wizard._create_profile()
        assert wizard.result_profile.retention.gfs_weekly == 4
        assert wizard.result_profile.retention.gfs_monthly == 7

    def test_corrupted_frequency_falls_back_to_weekly_default(
        self, wizard
    ) -> None:
        """Defensive: a bogus stored frequency must NOT trigger the
        Daily branch (which would silently bump retention)."""
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, "bogus_freq_value_xyz")

        wizard._create_profile()

        # Frequency degrades to WEEKLY → daily retention stays at 1.
        assert wizard.result_profile.schedule.frequency == ScheduleFrequency.WEEKLY
        assert wizard.result_profile.retention.gfs_daily == 1

    def test_missing_frequency_falls_back_to_weekly_default(
        self, wizard
    ) -> None:
        """Same defensive guard for a missing key (path that bypasses
        the wizard navigation entirely)."""
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, None)

        wizard._create_profile()

        assert wizard.result_profile.schedule.frequency == ScheduleFrequency.WEEKLY
        assert wizard.result_profile.retention.gfs_daily == 1


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    """Step 4 validation guards an empty / missing selection."""

    def test_validate_passes_with_default(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._step = 4
        wizard._show_step()  # populates _data["schedule_frequency"] with WEEKLY
        assert wizard._validate_personal_step() is None

    def test_validate_fails_without_value(self, wizard) -> None:
        """If a future refactor breaks the default-on-build invariant,
        the validate path surfaces it instead of silently saving."""
        wizard._select_mode(MODE_PERSONAL)
        wizard._step = 4
        # Do NOT call _show_step — leaves _data without the key.
        wizard._data.pop("schedule_frequency", None)
        msg = wizard._validate_personal_step()
        assert msg is not None
        assert "frequency" in msg.lower()


# ---------------------------------------------------------------------------
# Next-button label
# ---------------------------------------------------------------------------


class TestNextButtonLabel:
    """The action button must say 'Finish' on the very last step."""

    def test_next_says_finish_on_last_step(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._step = 4
        wizard._show_step()
        assert str(wizard._next_btn.cget("text")) == "Finish"

    def test_next_says_next_on_intermediate_step(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._step = 2
        wizard._show_step()
        # Encoded with the right-arrow so str(...) compares accurately.
        assert "Next" in str(wizard._next_btn.cget("text"))
        assert "Finish" not in str(wizard._next_btn.cget("text"))
