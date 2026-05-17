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

    def test_personal_mode_has_six_steps(self, wizard) -> None:
        """Personal mode now ends with a Backup-speed step (6/6) since v3.7.0.

        Before v3.7.0 the flow stopped at Retention (5/5). The new step 6
        lets the user pick Fast (verify_after_backup=False, default) or
        Thorough (verify_after_backup=True).
        """
        wizard._select_mode(MODE_PERSONAL)
        assert wizard._total_steps == 6

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
        "freq,expected_daily,expected_weekly,expected_monthly",
        [
            # Daily schedule → all three tiers visible, all three set to
            # the user-friendly defaults (display 7/3/6 → internal 8/4/7).
            (ScheduleFrequency.DAILY.value, 8, 4, 7),
            # Weekly schedule → daily row hidden, weekly + monthly active.
            (ScheduleFrequency.WEEKLY.value, 1, 4, 7),
            # Monthly schedule → only monthly active.
            (ScheduleFrequency.MONTHLY.value, 1, 1, 7),
        ],
    )
    def test_retention_defaults_match_visible_tiers(
        self,
        wizard,
        freq: str,
        expected_daily: int,
        expected_weekly: int,
        expected_monthly: int,
    ) -> None:
        """Hidden tiers stay at their internal default of 1 (which the
        Retention tab renders as 0). Visible tiers get the friendly
        out-of-the-box value — wrapping +1 to convert display →
        internal.

        Parametrised so each iteration gets a fresh wizard fixture;
        ``_create_profile`` destroys the Toplevel, so a single wizard
        can only complete one round.
        """
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, freq)
        wizard._create_profile()
        assert wizard.result_profile.retention.gfs_daily == expected_daily
        assert wizard.result_profile.retention.gfs_weekly == expected_weekly
        assert wizard.result_profile.retention.gfs_monthly == expected_monthly

    def test_corrupted_frequency_falls_back_to_weekly_default(self, wizard) -> None:
        """Defensive: a bogus stored frequency must NOT trigger the
        Daily branch (which would silently bump retention)."""
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, "bogus_freq_value_xyz")

        wizard._create_profile()

        # Frequency degrades to WEEKLY → daily retention stays at 1.
        assert wizard.result_profile.schedule.frequency == ScheduleFrequency.WEEKLY
        assert wizard.result_profile.retention.gfs_daily == 1

    def test_missing_frequency_falls_back_to_weekly_default(self, wizard) -> None:
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
        # Step 6 (backup speed) is the new last step since v3.7.0. The
        # step itself only reads ``verify_after_backup`` (defaulting to
        # Fast=False), but ``_show_step`` may navigate from prior steps
        # that need ``schedule_frequency`` populated — seed a weekly
        # default to keep the test independent of unrelated rehydration.
        wizard._data["schedule_frequency"] = ScheduleFrequency.WEEKLY.value
        wizard._step = 6
        wizard._show_step()
        assert str(wizard._next_btn.cget("text")) == "Finish"

    def test_next_says_next_on_intermediate_step(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._step = 2
        wizard._show_step()
        # Encoded with the right-arrow so str(...) compares accurately.
        assert "Next" in str(wizard._next_btn.cget("text"))
        assert "Finish" not in str(wizard._next_btn.cget("text"))


# ---------------------------------------------------------------------------
# Step 5 — Retention step
# ---------------------------------------------------------------------------


class TestRetentionStepWiring:
    """The new retention step must be reachable from the personal-mode flow."""

    def test_step_five_builder_is_retention(self, wizard) -> None:
        """``_show_step`` must dispatch step 5 to the retention builder."""
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.WEEKLY.value
        wizard._step = 5

        called = {"hit": False}
        original = wizard._step_retention

        def wrapper():
            called["hit"] = True
            return original()

        wizard._step_retention = wrapper
        wizard._show_step()
        assert called["hit"], "Step 5 did not dispatch to _step_retention"


class TestRetentionStepAdaptsToFrequency:
    """The step's visible rows must match the frequency chosen at step 4."""

    def test_daily_shows_all_three_tiers(self, wizard) -> None:
        """Daily schedule → all three GFS rows populate ``_data``."""
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.DAILY.value
        wizard._step = 5
        wizard._show_step()

        assert "retention_gfs_daily" in wizard._data
        assert "retention_gfs_weekly" in wizard._data
        assert "retention_gfs_monthly" in wizard._data
        # Defaults are the user-facing display values.
        assert wizard._data["retention_gfs_daily"] == 7
        assert wizard._data["retention_gfs_weekly"] == 3
        assert wizard._data["retention_gfs_monthly"] == 6

    def test_weekly_hides_daily_row(self, wizard) -> None:
        """Weekly schedule → daily tier removed from ``_data`` so
        ``_create_profile`` reuses the historical default for it."""
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.WEEKLY.value
        # Pre-seed a stale daily value to prove the step strips it.
        wizard._data["retention_gfs_daily"] = 99
        wizard._step = 5
        wizard._show_step()

        assert "retention_gfs_daily" not in wizard._data
        assert wizard._data["retention_gfs_weekly"] == 3
        assert wizard._data["retention_gfs_monthly"] == 6

    def test_monthly_hides_daily_and_weekly_rows(self, wizard) -> None:
        """Monthly schedule → only the monthly row stays in ``_data``."""
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.MONTHLY.value
        # Pre-seed stale values to prove the step strips them.
        wizard._data["retention_gfs_daily"] = 99
        wizard._data["retention_gfs_weekly"] = 99
        wizard._step = 5
        wizard._show_step()

        assert "retention_gfs_daily" not in wizard._data
        assert "retention_gfs_weekly" not in wizard._data
        assert wizard._data["retention_gfs_monthly"] == 6


class TestRetentionStepRehydration:
    """User edits at step 5 must survive a Back → Forward navigation."""

    def test_user_value_preserved_across_revisits(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.DAILY.value
        wizard._step = 5
        wizard._show_step()

        # Simulate the user editing the spinbox.
        wizard._data["retention_gfs_daily"] = 14
        wizard._data["retention_gfs_weekly"] = 8
        wizard._data["retention_gfs_monthly"] = 12

        # Rebuild the step (Back → Forward round-trip).
        wizard._show_step()

        # Rebuilt UI must reflect the user's prior edits, not the
        # 7/3/6 defaults.
        assert wizard._data["retention_gfs_daily"] == 14
        assert wizard._data["retention_gfs_weekly"] == 8
        assert wizard._data["retention_gfs_monthly"] == 12


class TestRetentionStepValidation:
    """The validation guard for step 5 rejects non-numeric or negative
    entries so a corrupted ``_data`` cannot reach ``_create_profile``."""

    def test_validate_passes_with_defaults(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.DAILY.value
        wizard._step = 5
        wizard._show_step()  # Populates _data with valid defaults.
        assert wizard._validate_personal_step() is None

    def test_validate_rejects_negative_value(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.DAILY.value
        wizard._step = 5
        wizard._show_step()
        wizard._data["retention_gfs_daily"] = -1
        msg = wizard._validate_personal_step()
        assert msg is not None
        assert "retention" in msg.lower()

    def test_validate_rejects_non_integer(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        wizard._data["schedule_frequency"] = ScheduleFrequency.DAILY.value
        wizard._step = 5
        wizard._show_step()
        wizard._data["retention_gfs_monthly"] = "not-a-number"
        msg = wizard._validate_personal_step()
        assert msg is not None
        assert "retention" in msg.lower()


class TestRetentionStepFlowsIntoCreateProfile:
    """User-edited values at step 5 must land in ``RetentionConfig``."""

    def test_custom_daily_values_flow_through(self, wizard) -> None:
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, ScheduleFrequency.DAILY.value)
        # Simulate the user lowering retention from the defaults.
        wizard._data["retention_gfs_daily"] = 2  # display 2 → internal 3
        wizard._data["retention_gfs_weekly"] = 1
        wizard._data["retention_gfs_monthly"] = 12

        wizard._create_profile()

        # +1 conversion: display N maps to internal N+1.
        assert wizard.result_profile.retention.gfs_daily == 3
        assert wizard.result_profile.retention.gfs_weekly == 2
        assert wizard.result_profile.retention.gfs_monthly == 13

    def test_hidden_tier_falls_back_to_historical_default(self, wizard) -> None:
        """When the step hides a tier, ``_create_profile`` must reuse
        the legacy internal value (1) for it — preserving the look of
        existing Weekly/Monthly profiles."""
        wizard._select_mode(MODE_PERSONAL)
        _seed_required_data(wizard, ScheduleFrequency.MONTHLY.value)
        # Hidden tiers: daily + weekly absent from _data.
        wizard._data["retention_gfs_monthly"] = 24

        wizard._create_profile()

        assert wizard.result_profile.retention.gfs_daily == 1
        assert wizard.result_profile.retention.gfs_weekly == 1
        assert wizard.result_profile.retention.gfs_monthly == 25
