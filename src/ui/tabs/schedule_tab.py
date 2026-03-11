"""
Schedule Tab
============
Automatic scheduling settings: frequency, time, day, auto-start,
retry-on-failure, and the execution journal.
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

from src.core.config import ScheduleConfig, ScheduleFrequency
from src.core.scheduler import AutoStart

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

COLORS = {
    "bg":        "#f5f6fa",
    "sidebar":   "#2c3e50",
    "sidebar_fg": "#ecf0f1",
    "accent":    "#3498db",
    "success":   "#27ae60",
    "warning":   "#f39c12",
    "danger":    "#e74c3c",
    "text":      "#2c3e50",
    "muted":     "#95a5a6",
}


class ScheduleTab:
    """Schedule tab: frequency, time, auto-start, retry, journal."""

    def __init__(self, app, parent_frame):
        self.app = app
        self.parent = parent_frame
        self._build()

    # ── Build ──────────────────────────────────
    def _build(self):
        container = ttk.Frame(self.parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Automatic scheduling',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 10))

        # Enable toggle — store reference
        self.var_sched_enabled = tk.BooleanVar(value=False)
        self._sched_checkbox = ttk.Checkbutton(
            container, text="Enable automatic scheduling",
            variable=self.var_sched_enabled)
        self._sched_checkbox.pack(anchor="w", pady=(0, 10))

        # Store settings frame reference
        settings_frame = ttk.LabelFrame(container, text="Settings", padding=15)
        settings_frame.pack(fill=tk.X, pady=(0, 15))
        self._sched_settings_frame = settings_frame

        # Frequency
        ttk.Label(settings_frame, text="Frequency").grid(row=0, column=0, sticky="w", pady=5)
        self.var_frequency = tk.StringVar(value=ScheduleFrequency.DAILY.value)
        freq_combo = ttk.Combobox(
            settings_frame, textvariable=self.var_frequency,
            values=[
                ScheduleFrequency.HOURLY.value,
                ScheduleFrequency.DAILY.value,
                ScheduleFrequency.WEEKLY.value,
                ScheduleFrequency.MONTHLY.value,
            ],
            state="readonly", width=15,
        )
        freq_combo.grid(row=0, column=1, sticky="w", padx=10, pady=5)

        # Time
        ttk.Label(settings_frame, text="Time (HH:MM)").grid(row=1, column=0, sticky="w", pady=5)
        self.var_time = tk.StringVar(value="02:00")
        ttk.Entry(settings_frame, textvariable=self.var_time, width=8,
                  font=("Consolas", 11)).grid(row=1, column=1, sticky="w", padx=10, pady=5)

        # Day of week (for weekly)
        ttk.Label(settings_frame, text="Day (weekly)").grid(
            row=2, column=0, sticky="w", pady=5)

        self.var_day_of_week = tk.StringVar(value=DAYS[0])
        ttk.Combobox(settings_frame, textvariable=self.var_day_of_week,
                      values=DAYS, state="readonly", width=15).grid(
            row=2, column=1, sticky="w", padx=10, pady=5)

        # Day of month (for monthly)
        ttk.Label(settings_frame, text="Day of month (monthly)").grid(
            row=3, column=0, sticky="w", pady=5)
        self.var_day_of_month = tk.IntVar(value=1)
        ttk.Spinbox(settings_frame, from_=1, to=28, textvariable=self.var_day_of_month,
                     width=5).grid(row=3, column=1, sticky="w", padx=10, pady=5)

        # Auto-start with Windows
        autostart_frame = ttk.LabelFrame(container, text="Start with Windows", padding=15)
        autostart_frame.pack(fill=tk.X, pady=(0, 10))

        self.var_autostart = tk.BooleanVar(value=AutoStart.is_enabled())
        ttk.Checkbutton(
            autostart_frame,
            text="Launch Backup Manager automatically when Windows starts",
            variable=self.var_autostart,
            command=self._toggle_autostart,
        ).pack(anchor="w")

        tk.Label(autostart_frame, wraplength=1100, justify=tk.LEFT,
                 font=("Segoe UI", 8), fg="#7f8c8d",
                 text="When enabled, Backup Manager starts with Windows and runs scheduled "
                      "backups in the background.\n"
                      "If your computer was asleep or shut down during a scheduled backup, "
                      "the missed backup will run automatically when the application restarts."
                 ).pack(anchor="w", pady=(5, 0))

        # ── Retry on failure ──
        retry_frame = ttk.LabelFrame(container, text="Automatic retry on failure", padding=10)
        retry_frame.pack(fill=tk.X, pady=(10, 0))

        self.var_retry_enabled = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            retry_frame, text="Automatically retry failed scheduled backups",
            variable=self.var_retry_enabled,
        ).pack(anchor="w")

        retry_settings = ttk.Frame(retry_frame)
        retry_settings.pack(fill=tk.X, pady=(5, 0))

        ttk.Label(retry_settings, text="Max attempts:").pack(side=tk.LEFT, padx=(0, 5))
        self.var_retry_max = tk.IntVar(value=3)
        ttk.Spinbox(retry_settings, from_=1, to=8, width=4,
                      textvariable=self.var_retry_max,
                      command=self._sync_retry_delays).pack(side=tk.LEFT, padx=(0, 15))

        ttk.Label(retry_settings, text="Delays (min):").pack(side=tk.LEFT, padx=(0, 5))
        self.var_retry_delays = tk.StringVar(value="2, 10, 30")
        ttk.Entry(retry_settings, textvariable=self.var_retry_delays,
                  width=40, font=("Consolas", 10)).pack(side=tk.LEFT)

        ttk.Label(retry_frame,
                  text="\U0001f4a1 Delays are auto-adjusted when you change max attempts. "
                       "Each new delay doubles the previous one.",
                  font=("Segoe UI", 8), foreground="#95a5a6"
                  ).pack(anchor="w", pady=(5, 0))

        # Save
        ttk.Button(container, text='\U0001f4be Save',
                    command=self.app._save_profile, style="Accent.TButton").pack(
            anchor="e", pady=(10, 0))

        # ── Schedule execution journal ──
        journal_frame = ttk.LabelFrame(container, text="Scheduled execution journal", padding=10)
        journal_frame.pack(fill=tk.BOTH, expand=True, pady=(15, 0))

        # Next run info
        self.lbl_next_run = ttk.Label(journal_frame, text="",
                                       font=("Segoe UI", 9))
        self.lbl_next_run.pack(anchor="w", pady=(0, 5))

        # Journal treeview
        journal_cols = ("date", "profile", "status", "detail", "duration")
        self.schedule_journal_tree = ttk.Treeview(
            journal_frame, columns=journal_cols, show="headings", height=4)
        self.schedule_journal_tree.heading("date", text="Date")
        self.schedule_journal_tree.heading("profile", text="Profile")
        self.schedule_journal_tree.heading("status", text="Status")
        self.schedule_journal_tree.heading("detail", text="Detail")
        self.schedule_journal_tree.heading("duration", text="Duration")
        self.schedule_journal_tree.column("date", width=140)
        self.schedule_journal_tree.column("profile", width=120)
        self.schedule_journal_tree.column("status", width=80)
        self.schedule_journal_tree.column("detail", width=200)
        self.schedule_journal_tree.column("duration", width=70)

        journal_scroll = ttk.Scrollbar(journal_frame, orient=tk.VERTICAL,
                                        command=self.schedule_journal_tree.yview)
        self.schedule_journal_tree.configure(yscrollcommand=journal_scroll.set)
        self.schedule_journal_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        journal_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Journal buttons
        journal_btn_frame = ttk.Frame(container)
        journal_btn_frame.pack(fill=tk.X, pady=(5, 0))
        ttk.Button(journal_btn_frame, text='\U0001f504 Refresh',
                    command=self._refresh_schedule_journal).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(journal_btn_frame, text="\U0001f5d1 Clear journal",
                    command=self._clear_schedule_journal).pack(side=tk.LEFT)

    # ── Helpers ────────────────────────────────

    def _toggle_autostart(self):
        """Enable or disable auto-start with Windows."""
        enabled = self.var_autostart.get()
        AutoStart.set_enabled(enabled)

    def _sync_retry_delays(self):
        """Auto-adjust the delays list when max attempts changes.
        Adds new delays (doubling the last one) or trims excess delays."""
        try:
            max_att = self.var_retry_max.get()
        except (tk.TclError, ValueError):
            return

        # Parse current delays
        try:
            delays = [
                int(d.strip()) for d in self.var_retry_delays.get().split(",")
                if d.strip().isdigit()
            ]
        except (ValueError, AttributeError):
            delays = [2, 10, 30]

        if not delays:
            delays = [2]

        # Extend if too few delays
        while len(delays) < max_att:
            delays.append(min(delays[-1] * 2, 1440))  # Double, cap at 24h

        # Trim if too many delays
        delays = delays[:max_att]

        self.var_retry_delays.set(", ".join(str(d) for d in delays))

    def _refresh_schedule_journal(self):
        """Refresh the schedule journal treeview."""
        for item in self.schedule_journal_tree.get_children():
            self.schedule_journal_tree.delete(item)

        profile_filter = self.app.current_profile.id if self.app.current_profile else ""
        entries = self.app.scheduler.journal.get_entries(limit=50, profile_id=profile_filter)

        for entry in entries:
            ts = entry.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(ts)
                date_str = dt.strftime("%d/%m/%Y %H:%M")
            except (ValueError, TypeError):
                date_str = ts[:16] if ts else "\u2014"

            status = entry.get("status", "")
            status_icons = {
                "started": "\U0001f504 In progress",
                "success": "\u2705 Passed",
                "failed": "\u274c Failed",
                "skipped": "\u23ed Skipped",
                "cancelled": "\u23f9 Cancelled",
                "retry_pending": "\u23f3 Retry pending",
            }
            status_str = status_icons.get(status, status)

            duration = entry.get("duration_seconds", 0)
            if duration > 0:
                m, s = divmod(int(duration), 60)
                dur_str = f"{m}m{s:02d}s"
            else:
                dur_str = "\u2014"

            self.schedule_journal_tree.insert("", tk.END, values=(
                date_str,
                entry.get("profile_name", ""),
                status_str,
                entry.get("detail", ""),
                dur_str,
            ))

        # Update next run info
        if self.app.current_profile:
            info = self.app.scheduler.get_next_run_info(self.app.current_profile)
            self.lbl_next_run.configure(text=f"\U0001f550 {info}")
        else:
            self.lbl_next_run.configure(text="")

    def _clear_schedule_journal(self):
        """Clear the schedule execution journal."""
        confirm = messagebox.askyesno(
            "Clear journal",
            "Delete all scheduled execution history ?"
        )
        if confirm:
            self.app.scheduler.journal.clear()
            self._refresh_schedule_journal()

    # ── Profile load / collect ─────────────────

    def load_profile(self, profile):
        """Populate schedule fields from profile data."""
        sc = profile.schedule
        self.var_sched_enabled.set(sc.enabled)
        self.var_frequency.set(sc.frequency)
        self.var_time.set(sc.time)
        self.var_day_of_week.set(DAYS[sc.day_of_week])
        self.var_day_of_month.set(sc.day_of_month)
        self.var_retry_enabled.set(sc.retry_enabled)
        self.var_retry_max.set(sc.retry_max_attempts)
        self.var_retry_delays.set(", ".join(str(d) for d in (sc.retry_delay_minutes or [2, 10, 30])))
        # Refresh journal for this profile
        self._refresh_schedule_journal()

    def collect_config(self, profile):
        """Read schedule vars into profile.schedule."""
        try:
            retry_delays = [
                int(d.strip()) for d in self.var_retry_delays.get().split(",")
                if d.strip().isdigit()
            ]
        except (ValueError, AttributeError):
            retry_delays = [2, 10, 30]

        profile.schedule = ScheduleConfig(
            enabled=self.var_sched_enabled.get(),
            frequency=self.var_frequency.get(),
            time=self.var_time.get(),
            day_of_week=DAYS.index(self.var_day_of_week.get()) if self.var_day_of_week.get() in DAYS else 0,
            day_of_month=self.var_day_of_month.get(),
            retry_enabled=self.var_retry_enabled.get(),
            retry_max_attempts=self.var_retry_max.get(),
            retry_delay_minutes=retry_delays or [2, 10, 30],
        )
