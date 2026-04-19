"""Retention tab: GFS backup rotation policy configuration."""

import contextlib
import tkinter as tk
from tkinter import ttk

from src.core.config import BackupProfile, RetentionConfig, RetentionPolicy, ScheduleFrequency
from src.ui.theme import Spacing


class RetentionTab(ttk.Frame):
    """GFS (Grandfather-Father-Son) retention policy configuration."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._schedule_freq: ScheduleFrequency = ScheduleFrequency.DAILY
        self._schedule_tab = None  # Set by set_schedule_tab() from the app
        self._build_ui()

    def set_schedule_tab(self, tab) -> None:
        """Wire the Schedule tab so the retention rows react to live edits.

        Without this link, Retention only re-reads the schedule frequency
        when a profile is loaded. A user who switches backup_type to
        Differential — which auto-flips the schedule combobox to Daily —
        would not see the Daily row reappear until they saved the
        profile. Registering a trace on the frequency var keeps Retention
        in sync with whatever the user is editing in Schedule right now.
        """
        self._schedule_tab = tab
        with contextlib.suppress(AttributeError, tk.TclError):
            tab.get_frequency_var().trace_add("write", lambda *_: self._on_schedule_freq_changed())

    def _on_schedule_freq_changed(self) -> None:
        """React to a live edit of the Schedule frequency combobox."""
        if self._schedule_tab is None:
            return
        try:
            label = self._schedule_tab.get_frequency_var().get()
        except (AttributeError, tk.TclError):
            return
        try:
            freq = ScheduleFrequency(label.lower())
        except ValueError:
            return
        self._apply_frequency_visibility(freq)

    def _build_ui(self):
        frame = ttk.LabelFrame(
            self,
            text="GFS Retention",
            padding=Spacing.PAD,
        )
        frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE)

        # User-facing fields (display value = internal value - 1)
        gfs_fields = [
            ("Days of history:", "gfs_daily", 1),
            ("Weeks of history:", "gfs_weekly", 1),
            ("Months of history:", "gfs_monthly", 1),
        ]
        self._gfs_vars = {}
        self._gfs_rows: dict[str, ttk.Frame] = {}
        for label, key, default in gfs_fields:
            row = ttk.Frame(frame)
            row.pack(fill="x", pady=2)
            self._gfs_rows[key] = row
            ttk.Label(row, text=label).pack(side="left")
            var = tk.IntVar(value=default)
            self._gfs_vars[key] = var
            spinbox = ttk.Spinbox(row, textvariable=var, from_=0, to=998, width=8)
            spinbox.pack(side="right")
            var.trace_add("write", lambda *_: self._update_summary())

        # Summary label
        self._summary_label = tk.Label(
            frame,
            text="",
            justify="left",
            anchor="w",
            fg="#444444",
        )
        self._summary_label.pack(fill="x", pady=(10, 0))

        self._update_summary()

    def _update_summary(self) -> None:
        """Update the retention summary text based on current values.

        Only shows lines for visible retention levels (depends on
        schedule frequency: monthly hides daily+weekly, weekly hides daily).
        """
        try:
            user_daily = self._gfs_vars["gfs_daily"].get()
            user_weekly = self._gfs_vars["gfs_weekly"].get()
            user_monthly = self._gfs_vars["gfs_monthly"].get()
        except (tk.TclError, ValueError):
            return

        freq = self._schedule_freq
        show_daily = freq not in (ScheduleFrequency.WEEKLY, ScheduleFrequency.MONTHLY)
        show_weekly = freq != ScheduleFrequency.MONTHLY

        # Internal values = user values + 1 (today is always kept)
        real_daily = user_daily + 1
        real_weekly = user_weekly + 1
        real_monthly = user_monthly + 1

        lines = ["Retention summary:"]

        # Daily line
        if show_daily:
            if user_daily == 0:
                lines.append("  \u2022 Today only (no history)")
            elif user_daily == 1:
                lines.append("  \u2022 Today + yesterday")
            else:
                lines.append(f"  \u2022 Today + {user_daily} days of history")

        # Weekly line
        if show_weekly:
            if user_weekly == 0:
                lines.append("  \u2022 No weekly history")
            elif user_weekly == 1:
                lines.append("  \u2022 1 week of history (1 weekly backup)")
            else:
                lines.append(
                    f"  \u2022 {user_weekly} weeks of history ({user_weekly} weekly backups)"
                )

        # Monthly line
        if user_monthly == 0:
            lines.append("  \u2022 No monthly history")
        elif user_monthly == 1:
            lines.append("  \u2022 1 month of history (1 monthly backup)")
        else:
            lines.append(
                f"  \u2022 {user_monthly} months of history ({user_monthly} monthly backups)"
            )

        # Total calculation (only count visible levels)
        total = real_monthly
        if show_weekly:
            total += max(real_weekly - 1, 0)
        if show_daily:
            total += max(real_daily - 1, 0)
        lines.append(f"Backups kept: {total}")

        self._summary_label.config(text="\n".join(lines))

    def load_profile(self, profile: BackupProfile):
        """Load retention config from profile.

        Internal values are stored with +1 offset.
        Display value = internal - 1.
        Hides "Days of history" when schedule is weekly or less frequent
        since daily retention is irrelevant without daily backups.
        """
        r = profile.retention
        self._gfs_enabled = r.gfs_enabled
        for key, var in self._gfs_vars.items():
            internal_val = getattr(r, key, var.get() + 1)
            var.set(max(internal_val - 1, 0))

        # Prefer the live Schedule combobox (if wired) over the saved
        # profile field: the auto-config that runs on Full→Differential
        # can switch the combobox to Daily without the profile being
        # saved yet, and Retention needs to reflect that immediately.
        freq = profile.schedule.frequency
        if self._schedule_tab is not None:
            try:
                label = self._schedule_tab.get_frequency_var().get()
                freq = ScheduleFrequency(label.lower())
            except (AttributeError, ValueError, tk.TclError):
                pass

        self._apply_frequency_visibility(freq)

    def _apply_frequency_visibility(self, freq: ScheduleFrequency) -> None:
        """Show/hide retention rows based on the given schedule frequency."""
        self._schedule_freq = freq

        daily_row = self._gfs_rows.get("gfs_daily")
        weekly_row = self._gfs_rows.get("gfs_weekly")
        monthly_row = self._gfs_rows.get("gfs_monthly")

        # Daily: hidden when schedule is weekly or monthly
        if daily_row:
            if freq in (ScheduleFrequency.WEEKLY, ScheduleFrequency.MONTHLY):
                daily_row.pack_forget()
            else:
                daily_row.pack(fill="x", pady=2, before=weekly_row)

        # Weekly: hidden when schedule is monthly
        if weekly_row:
            if freq == ScheduleFrequency.MONTHLY:
                weekly_row.pack_forget()
            else:
                weekly_row.pack(fill="x", pady=2, before=monthly_row)

        self._update_summary()

    def get_gfs_daily_var(self) -> tk.IntVar:
        """Return the Tk IntVar holding the user-facing daily retention.

        The displayed value is offset by -1 from the internal value
        (internal = var.get() + 1). Consumers that need the internal
        value must add 1 after reading.
        """
        return self._gfs_vars["gfs_daily"]

    def collect_config(self) -> dict:
        """Collect retention configuration.

        User values are +1 to get internal values.
        Preserves gfs_enabled from the loaded profile.
        """
        internal_values = {}
        for key, var in self._gfs_vars.items():
            internal_values[key] = var.get() + 1

        return {
            "retention": RetentionConfig(
                policy=RetentionPolicy.GFS,
                gfs_enabled=self._gfs_enabled,
                **internal_values,
            ),
        }

    _gfs_enabled: bool = True  # Preserved from load_profile
