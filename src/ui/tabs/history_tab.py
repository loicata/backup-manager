"""History tab: browse backup logs."""

import tkinter as tk
from tkinter import ttk
import os
import subprocess
from pathlib import Path

from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing


class HistoryTab(ScrollableTab):
    """Browse and view backup execution logs."""

    def __init__(self, parent, log_dir: Path = None, **kwargs):
        super().__init__(parent, **kwargs)
        appdata = os.environ.get("APPDATA", "")
        self._log_dir = log_dir or Path(appdata) / "BackupManager" / "logs"
        self._build_ui()

    def _build_ui(self):
        # Log list
        list_frame = ttk.LabelFrame(self.inner, text="Backup logs", padding=Spacing.PAD)
        list_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.LARGE)

        self.log_tree = ttk.Treeview(
            list_frame, columns=("date", "profile", "size"),
            show="headings", height=12,
        )
        self.log_tree.heading("date", text="Date")
        self.log_tree.heading("profile", text="Profile")
        self.log_tree.heading("size", text="Size")
        self.log_tree.column("date", width=180)
        self.log_tree.column("profile", width=200)
        self.log_tree.column("size", width=100)

        scrollbar = ttk.Scrollbar(list_frame, orient="vertical",
                                    command=self.log_tree.yview)
        self.log_tree.configure(yscrollcommand=scrollbar.set)
        self.log_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Buttons
        btn_frame = ttk.Frame(self.inner)
        btn_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        ttk.Button(btn_frame, text="Refresh", command=self.refresh).pack(side="left")
        ttk.Button(btn_frame, text="Open logs folder",
                    command=self._open_folder).pack(side="left", padx=Spacing.MEDIUM)

        self.refresh()

    def refresh(self):
        for item in self.log_tree.get_children():
            self.log_tree.delete(item)

        if not self._log_dir.exists():
            return

        log_files = sorted(
            self._log_dir.glob("backup_*.log"),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )

        for log_file in log_files[:100]:
            name = log_file.stem
            # Extract profile from filename: backup_PROFILEID_YYYYMMDD_HHMMSS
            parts = name.split("_", 2)
            profile = parts[1] if len(parts) > 1 else "Unknown"
            date_str = parts[2] if len(parts) > 2 else ""

            size = log_file.stat().st_size
            size_str = f"{size / 1024:.1f} KB" if size > 1024 else f"{size} B"

            from datetime import datetime
            try:
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                date_display = mtime.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                date_display = date_str

            self.log_tree.insert("", "end", values=(date_display, profile, size_str))

    def _open_folder(self):
        if self._log_dir.exists():
            os.startfile(str(self._log_dir))

    def load_profile(self, profile):
        pass

    def collect_config(self) -> dict:
        return {}
