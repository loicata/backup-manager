"""Schedule tab: backup scheduling configuration."""

import contextlib
import tkinter as tk
from tkinter import ttk

from src.core.config import BackupProfile, ScheduleConfig, ScheduleFrequency
from src.ui.theme import Spacing


class ScheduleTab(ttk.Frame):
    """Scheduling configuration and journal display."""

    def __init__(self, parent, scheduler=None, **kwargs):
        super().__init__(parent, **kwargs)
        self._scheduler = scheduler
        self._build_ui()

    def _build_ui(self):
        # Enable scheduling
        self.enabled_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            self,
            text="Enable automatic scheduling",
            variable=self.enabled_var,
            command=self._toggle_enabled,
        ).pack(anchor="w", padx=Spacing.LARGE, pady=Spacing.LARGE)

        self._content = ttk.Frame(self)
        self._content.pack(fill="both", expand=True)

        # Frequency
        freq_frame = ttk.LabelFrame(self._content, text="Schedule", padding=Spacing.PAD)
        freq_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        row1 = ttk.Frame(freq_frame)
        row1.pack(fill="x")

        ttk.Label(row1, text="Frequency:").pack(side="left")
        self.freq_var = tk.StringVar(value=ScheduleFrequency.DAILY.value)
        ttk.Combobox(
            row1,
            textvariable=self.freq_var,
            state="readonly",
            values=[
                f.value.capitalize()
                for f in ScheduleFrequency
                if f not in (ScheduleFrequency.MANUAL, ScheduleFrequency.HOURLY)
            ],
            width=15,
        ).pack(side="left", padx=Spacing.MEDIUM)

        ttk.Label(row1, text="Time (HH:MM):").pack(side="left", padx=(Spacing.LARGE, 0))
        self.time_var = tk.StringVar(value="02:00")
        ttk.Entry(row1, textvariable=self.time_var, width=8).pack(side="left", padx=Spacing.SMALL)

        row2 = ttk.Frame(freq_frame)
        row2.pack(fill="x", pady=(Spacing.MEDIUM, 0))

        ttk.Label(row2, text="Day of week:").pack(side="left")
        self.dow_var = tk.StringVar(value="Monday")
        ttk.Combobox(
            row2,
            textvariable=self.dow_var,
            state="readonly",
            values=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
            width=15,
        ).pack(side="left", padx=Spacing.MEDIUM)

        ttk.Label(row2, text="Day of month:").pack(side="left", padx=(Spacing.LARGE, 0))
        self.dom_var = tk.IntVar(value=1)
        ttk.Spinbox(row2, textvariable=self.dom_var, from_=1, to=28, width=5).pack(
            side="left", padx=Spacing.SMALL
        )

        # Schedule journal
        journal_frame = ttk.LabelFrame(self._content, text="Schedule journal", padding=Spacing.PAD)
        journal_frame.pack(
            fill="both", expand=True, padx=Spacing.LARGE, pady=(Spacing.MEDIUM, Spacing.LARGE)
        )

        self.journal_tree = ttk.Treeview(
            journal_frame,
            columns=("time", "profile", "status", "detail"),
            show="headings",
            height=6,
        )
        self.journal_tree.heading("time", text="Time")
        self.journal_tree.heading("profile", text="Profile")
        self.journal_tree.heading("status", text="Status")
        self.journal_tree.heading("detail", text="Detail")
        self.journal_tree.column("time", width=150)
        self.journal_tree.column("profile", width=120)
        self.journal_tree.column("status", width=80)
        self.journal_tree.column("detail", width=300)

        journal_scroll = ttk.Scrollbar(
            journal_frame, orient="vertical", command=self.journal_tree.yview
        )
        self.journal_tree.configure(yscrollcommand=journal_scroll.set)
        self.journal_tree.pack(side="left", fill="both", expand=True)
        journal_scroll.pack(side="right", fill="y")

        ttk.Button(journal_frame, text="Refresh", command=self._refresh_journal).pack(
            anchor="e", pady=(Spacing.SMALL, 0)
        )

        # Periodic integrity verification
        verify_frame = ttk.LabelFrame(
            self._content, text="Integrity verification", padding=Spacing.PAD
        )
        verify_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.verify_enabled_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            verify_frame,
            text="Enable periodic integrity verification",
            variable=self.verify_enabled_var,
        ).pack(anchor="w")

        interval_row = ttk.Frame(verify_frame)
        interval_row.pack(fill="x", pady=(Spacing.SMALL, 0))
        ttk.Label(interval_row, text="Verify every").pack(side="left")
        self.verify_interval_var = tk.IntVar(value=7)
        ttk.Spinbox(
            interval_row,
            textvariable=self.verify_interval_var,
            from_=1,
            to=90,
            width=5,
        ).pack(side="left", padx=Spacing.SMALL)
        ttk.Label(interval_row, text="days").pack(side="left")

        self._toggle_enabled()

    def _toggle_enabled(self):
        state = "normal" if self.enabled_var.get() else "disabled"
        for child in self._content.winfo_children():
            self._set_state_recursive(child, state)

    def _set_state_recursive(self, widget, state):
        with contextlib.suppress(tk.TclError):
            widget.configure(state=state)
        for child in widget.winfo_children():
            self._set_state_recursive(child, state)

    def _refresh_journal(self):
        for item in self.journal_tree.get_children():
            self.journal_tree.delete(item)
        if self._scheduler:
            entries = self._scheduler.journal.get_entries(limit=50)
            for entry in reversed(entries):
                self.journal_tree.insert(
                    "",
                    "end",
                    values=(
                        entry.get("timestamp", "")[:19],
                        entry.get("profile_name", ""),
                        entry.get("status", ""),
                        entry.get("detail", ""),
                    ),
                )

    def _day_name_to_int(self, name: str) -> int:
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        try:
            return days.index(name)
        except ValueError:
            return 0

    def _int_to_day_name(self, idx: int) -> str:
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        return days[idx % 7]

    def load_profile(self, profile: BackupProfile):
        """Load schedule config into UI widgets."""
        s = profile.schedule
        self.enabled_var.set(s.enabled)
        self.freq_var.set(s.frequency.value.capitalize())
        self.time_var.set(s.time)
        self.dow_var.set(self._int_to_day_name(s.day_of_week))
        self.dom_var.set(s.day_of_month)
        self.verify_enabled_var.set(s.verify_enabled)
        self.verify_interval_var.set(s.verify_interval_days)
        self._toggle_enabled()
        self._refresh_journal()

    def collect_config(self) -> dict:
        """Collect schedule configuration.

        Returns:
            Dict with 'schedule' key containing a ScheduleConfig.
            Note: retry fields are now collected by GeneralTab.
        """
        freq_str = self.freq_var.get().lower()
        try:
            freq = ScheduleFrequency(freq_str)
        except ValueError:
            freq = ScheduleFrequency.DAILY

        return {
            "schedule": ScheduleConfig(
                frequency=freq,
                time=self.time_var.get(),
                day_of_week=self._day_name_to_int(self.dow_var.get()),
                day_of_month=self.dom_var.get(),
                enabled=self.enabled_var.get(),
                verify_enabled=self.verify_enabled_var.get(),
                verify_interval_days=self.verify_interval_var.get(),
            ),
        }
