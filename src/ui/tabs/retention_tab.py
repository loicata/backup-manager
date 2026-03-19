"""Retention tab: GFS backup rotation policy configuration."""

import tkinter as tk
from tkinter import ttk

from src.core.config import BackupProfile, RetentionConfig, RetentionPolicy
from src.ui.tabs import ScrollableTab
from src.ui.theme import Spacing


class RetentionTab(ScrollableTab):
    """GFS (Grandfather-Father-Son) retention policy configuration."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._build_ui()

    def _build_ui(self):
        frame = ttk.LabelFrame(
            self.inner, text="GFS Retention (Grandfather-Father-Son)",
            padding=Spacing.PAD,
        )
        frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE)

        gfs_fields = [
            ("Daily backups to keep (days):", "gfs_daily", 7),
            ("Weekly backups to keep (weeks):", "gfs_weekly", 4),
            ("Monthly backups to keep (months):", "gfs_monthly", 12),
        ]
        self._gfs_vars = {}
        for label, key, default in gfs_fields:
            row = ttk.Frame(frame)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=label).pack(side="left")
            var = tk.IntVar(value=default)
            self._gfs_vars[key] = var
            ttk.Spinbox(row, textvariable=var, from_=1, to=999, width=8).pack(side="right")

    def load_profile(self, profile: BackupProfile):
        """Load retention config from profile."""
        r = profile.retention
        for key, var in self._gfs_vars.items():
            var.set(getattr(r, key, var.get()))

    def collect_config(self) -> dict:
        """Collect retention configuration."""
        return {
            "retention": RetentionConfig(
                policy=RetentionPolicy.GFS,
                **{k: v.get() for k, v in self._gfs_vars.items()},
            ),
        }
