"""Protection tab: read-only display of S3 Object Lock status.

Shown instead of the Retention tab when the profile has
object_lock_enabled=True. Displays lock mode, retention durations,
region, bucket, and cost estimate.
"""

import tkinter as tk
from tkinter import ttk

from src.core.config import BackupProfile
from src.storage.s3_setup import RETENTION_OPTIONS
from src.ui.theme import Colors, Fonts, Spacing


class ProtectionTab(ttk.Frame):
    """Read-only tab showing Object Lock protection status."""

    def __init__(self, parent: ttk.Notebook):
        super().__init__(parent)
        self._build_ui()

    def _build_ui(self) -> None:
        """Build the static layout with placeholder labels."""
        pad = Spacing.PAD

        # Header
        header = ttk.Frame(self)
        header.pack(fill="x", padx=Spacing.LARGE, pady=(Spacing.LARGE, 0))
        tk.Label(
            header,
            text="\U0001f6e1\ufe0f",
            font=("Segoe UI", 28),
        ).pack(side="left", padx=(0, Spacing.MEDIUM))
        ttk.Label(
            header,
            text="Anti-Ransomware Protection Active",
            font=Fonts.title(),
            foreground=Colors.SUCCESS,
        ).pack(side="left")

        # Status card
        status = ttk.LabelFrame(
            self,
            text="Object Lock Status",
            padding=pad,
        )
        status.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self._fields: dict[str, ttk.Label] = {}
        field_defs = [
            ("Mode", "mode"),
            ("Differential retention", "diff_retention"),
            ("Full backup retention", "full_retention"),
            ("Region", "region"),
            ("Bucket", "bucket"),
        ]
        for label_text, key in field_defs:
            row = ttk.Frame(status)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=f"{label_text}:", font=Fonts.bold(), width=25).pack(
                side="left",
            )
            lbl = ttk.Label(row, text="\u2014")
            lbl.pack(side="left")
            self._fields[key] = lbl

    def load_profile(self, profile: BackupProfile) -> None:
        """Populate fields from a loaded profile.

        Args:
            profile: Backup profile with Object Lock settings.
        """
        s = profile.storage
        if not profile.object_lock_enabled:
            return

        self._fields["mode"].config(text=f"{s.s3_object_lock_mode} (immutable)")

        diff_days = s.s3_object_lock_days
        full_days = diff_days + s.s3_object_lock_full_extra_days

        # Find matching label
        diff_label = f"{diff_days} days"
        full_label = f"{full_days} days"
        for label, _months, days in RETENTION_OPTIONS:
            if days == diff_days:
                diff_label = f"{label} ({days} days)"
            if days == full_days:
                full_label = f"{label} ({days} days)"

        self._fields["diff_retention"].config(text=diff_label)
        self._fields["full_retention"].config(text=full_label)
        self._fields["region"].config(text=s.s3_region)
        self._fields["bucket"].config(text=s.s3_bucket)

    def collect_config(self) -> dict:
        """No-op: protection settings are read-only."""
        return {}
