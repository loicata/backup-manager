"""
Retention Tab — Retention policy settings (Simple / GFS).
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from src.core.config import RetentionPolicy, RetentionConfig


class RetentionTab:
    """Retention policy tab: simple count or GFS strategy."""

    def __init__(self, app, parent_frame):
        self.app = app
        self.root = app.root
        self._build(parent_frame)

    # ──────────────────────────────────────────
    #  Build UI
    # ──────────────────────────────────────────
    def _build(self, parent):
        container = ttk.Frame(parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text="Retention policy",
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))

        # Explanation
        info = tk.Frame(container, bg="#f0f4f8", padx=15, pady=10, relief=tk.SOLID, bd=1)
        info.pack(fill=tk.X, pady=(0, 10))
        tk.Label(info, bg="#f0f4f8", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="The retention policy determines HOW MANY old backups are kept.\n"
                      "The oldest are automatically deleted to free up space.\n"
                      "This is a balance between security (being able to go far back) "
                      "and disk space consumed."
                 ).pack(anchor="w")

        # Variables
        self.var_retention_policy = tk.StringVar(value=RetentionPolicy.SIMPLE.value)
        self.var_max_backups = tk.IntVar(value=10)
        self.var_gfs_daily = tk.IntVar(value=7)
        self.var_gfs_weekly = tk.IntVar(value=4)
        self.var_gfs_monthly = tk.IntVar(value=12)

        # ── Simple option ──
        simple_card = tk.Frame(container, bg="white", padx=15, pady=12,
                                relief=tk.SOLID, bd=1)
        simple_card.pack(fill=tk.X, pady=5)

        ttk.Radiobutton(simple_card, text="\U0001f522 Simple \u2014 Keep the last N backups",
                         variable=self.var_retention_policy,
                         value=RetentionPolicy.SIMPLE.value).pack(anchor="w")
        tk.Label(simple_card, bg="white", fg="#7f8c8d",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="Keeps a fixed number of backups. When the limit is reached, "
                      "the oldest is deleted.\n"
                      "Example: keep the last 10 = you can go back up to 10 backups."
                 ).pack(anchor="w", padx=(20, 0), pady=(2, 5))

        simple_row = ttk.Frame(simple_card)
        simple_row.pack(anchor="w", padx=(20, 0))
        ttk.Label(simple_row, text="Number to keep:").pack(side=tk.LEFT)
        ttk.Spinbox(simple_row, from_=1, to=999, textvariable=self.var_max_backups,
                     width=6).pack(side=tk.LEFT, padx=5)

        # ── GFS option ──
        gfs_card = tk.Frame(container, bg="white", padx=15, pady=12,
                             relief=tk.SOLID, bd=1)
        gfs_card.pack(fill=tk.X, pady=5)

        ttk.Radiobutton(gfs_card, text="\U0001f4c5 GFS \u2014 Grandfather / Father / Son",
                         variable=self.var_retention_policy,
                         value=RetentionPolicy.GFS.value).pack(anchor="w")
        tk.Label(gfs_card, bg="white", fg="#7f8c8d",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="Professional 3-level strategy. Keeps more recent restore points "
                      "and progressively spaces out in the past:\n\n"
                      "  \U0001f4c5 Daily (Son) \u2014 1 backup per day for the last X days\n"
                      "  \U0001f4c5 Weekly (Father) \u2014 1 backup per week for X weeks\n"
                      "  \U0001f4c5 Monthly (Grandfather) \u2014 1 backup per month for X months\n\n"
                      "Example with 7d + 4w + 12m: you can go back to any day of the "
                      "past week, any week of the past month, and any month of the past year."
                 ).pack(anchor="w", padx=(20, 0), pady=(2, 8))

        gfs_grid = ttk.Frame(gfs_card)
        gfs_grid.pack(anchor="w", padx=(20, 0), pady=(0, 5))

        ttk.Label(gfs_grid, text="\U0001f4c5 Days:").grid(row=0, column=0, padx=(0, 3))
        ttk.Spinbox(gfs_grid, from_=1, to=365, textvariable=self.var_gfs_daily,
                     width=5).grid(row=0, column=1, padx=(0, 20))
        ttk.Label(gfs_grid, text="\U0001f4c5 Weeks:").grid(row=0, column=2, padx=(0, 3))
        ttk.Spinbox(gfs_grid, from_=1, to=52, textvariable=self.var_gfs_weekly,
                     width=5).grid(row=0, column=3, padx=(0, 20))
        ttk.Label(gfs_grid, text="\U0001f4c5 Months:").grid(row=0, column=4, padx=(0, 3))
        ttk.Spinbox(gfs_grid, from_=1, to=120, textvariable=self.var_gfs_monthly,
                     width=5).grid(row=0, column=5)

        # GFS live summary
        self.lbl_gfs_tab_summary = ttk.Label(gfs_card, text="",
                                              font=("Segoe UI", 10, "bold"))
        self.lbl_gfs_tab_summary.pack(anchor="w", padx=(20, 0), pady=(3, 0))

        def update_gfs_summary(*args):
            try:
                d = self.var_gfs_daily.get()
                w = self.var_gfs_weekly.get()
                m = self.var_gfs_monthly.get()
                total = d + w + m
                self.lbl_gfs_tab_summary.configure(
                    text=f"\U0001f4a1 Up to ~{total} backups kept ({d}d + {w}w + {m}m)")
            except (tk.TclError, ValueError):
                pass

        self.var_gfs_daily.trace_add("write", update_gfs_summary)
        self.var_gfs_weekly.trace_add("write", update_gfs_summary)
        self.var_gfs_monthly.trace_add("write", update_gfs_summary)
        update_gfs_summary()

        # Save button
        ttk.Button(container, text='\U0001f4be Save',
                    command=self.app._save_profile, style="Accent.TButton").pack(
            anchor="e", pady=(15, 0))

    # ──────────────────────────────────────────
    #  Profile load / collect
    # ──────────────────────────────────────────
    def load_profile(self, p):
        """Load profile data into the tab's UI widgets."""
        ret = p.retention
        self.var_retention_policy.set(ret.policy)
        self.var_max_backups.set(ret.max_backups)
        self.var_gfs_daily.set(ret.gfs_daily)
        self.var_gfs_weekly.set(ret.gfs_weekly)
        self.var_gfs_monthly.set(ret.gfs_monthly)

    def collect_config(self, p):
        """Save tab's UI state into profile p."""
        p.retention = RetentionConfig(
            policy=self.var_retention_policy.get(),
            max_backups=self.var_max_backups.get(),
            gfs_daily=self.var_gfs_daily.get(),
            gfs_weekly=self.var_gfs_weekly.get(),
            gfs_monthly=self.var_gfs_monthly.get(),
        )
        return True
