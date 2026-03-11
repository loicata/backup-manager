"""
History Tab
===========
Backup history: list of log files with date, profile, and size.
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import tkinter as tk
from tkinter import ttk


class HistoryTab:
    """History tab: backup log file listing."""

    def __init__(self, app, parent_frame):
        self.app = app
        self.parent = parent_frame
        self._build()

    # ── Build ──────────────────────────────────
    def _build(self):
        container = ttk.Frame(self.parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Backup history',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 10))

        # Treeview for log files
        columns = ("date", "profile", "size")
        self.history_tree = ttk.Treeview(container, columns=columns, show="headings", height=12)
        self.history_tree.heading("date", text="Date")
        self.history_tree.heading("profile", text="Profile")
        self.history_tree.heading("size", text="Log size")
        self.history_tree.column("date", width=200)
        self.history_tree.column("profile", width=200)
        self.history_tree.column("size", width=100)
        self.history_tree.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X)
        ttk.Button(btn_frame, text='\U0001f504 Refresh',
                    command=self._refresh_history).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_frame, text="\U0001f4c2 Open logs folder",
                    command=self._open_logs_folder).pack(side=tk.LEFT)

        self._refresh_history()

    # ── Helpers ────────────────────────────────

    def _refresh_history(self):
        """Refresh the history treeview from log files."""
        for item in self.history_tree.get_children():
            self.history_tree.delete(item)

        log_dir = self.app.config.LOG_DIR
        if not log_dir.exists():
            return

        log_files = sorted(log_dir.glob("backup_*.log"), reverse=True)
        for lf in log_files[:50]:
            try:
                stat = lf.stat()
                parts = lf.stem.split("_")
                profile_id = parts[1] if len(parts) > 1 else "?"
                date_str = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                size_kb = f"{stat.st_size / 1024:.1f} Ko"
                self.history_tree.insert("", tk.END, values=(date_str, profile_id, size_kb))
            except OSError:
                pass

    def _open_logs_folder(self):
        log_dir = str(self.app.config.LOG_DIR)
        if sys.platform == "win32":
            os.startfile(log_dir)
        else:
            subprocess.Popen(["xdg-open", log_dir], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # ── Profile load / collect ─────────────────

    def load_profile(self, profile):
        """Refresh history when profile is loaded."""
        self._refresh_history()

    def collect_config(self, profile):
        """No-op: history tab has no config to collect."""
        pass
