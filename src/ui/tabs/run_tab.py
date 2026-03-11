"""
Run Tab
=======
Backup execution: progress bar, status log, start/cancel buttons.
"""

import tkinter as tk
from tkinter import ttk


class RunTab:
    """Run tab: progress, log, start/cancel buttons."""

    def __init__(self, app, parent_frame):
        self.app = app
        self.parent = parent_frame
        self._build()

    # ── Build ──────────────────────────────────
    def _build(self):
        container = ttk.Frame(self.parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Run backup',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))

        self.lbl_run_profile = ttk.Label(container, text="No profile selected",
                                          style="SubHeader.TLabel")
        self.lbl_run_profile.pack(anchor="w", pady=(0, 15))

        # Progress
        progress_frame = ttk.LabelFrame(container, text="Progress", padding=15)
        progress_frame.pack(fill=tk.X, pady=(0, 10))

        self.progress_var = tk.DoubleVar(value=0)
        self.progressbar = ttk.Progressbar(
            progress_frame, variable=self.progress_var,
            maximum=100, length=500, mode="determinate",
            style="Green.Horizontal.TProgressbar",
        )
        self.progressbar.pack(fill=tk.X, pady=(0, 5))

        self.lbl_progress = ttk.Label(progress_frame, text="Waiting...",
                                       font=("Segoe UI", 9))
        self.lbl_progress.pack(anchor="w")

        self.lbl_progress_pct = ttk.Label(progress_frame, text="0%",
                                           font=("Segoe UI", 10, "bold"))
        self.lbl_progress_pct.pack(anchor="e")

        self.lbl_current_file = ttk.Label(progress_frame, text="",
                                           font=("Segoe UI", 8),
                                           foreground="#95a5a6")
        self.lbl_current_file.pack(anchor="w")

        # Status log
        log_frame = ttk.LabelFrame(container, text="Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.log_text = tk.Text(log_frame, font=("Consolas", 9), height=10,
                                 state=tk.DISABLED, bg="#2d2d2d", fg="#00ff00",
                                 relief=tk.FLAT, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        log_scroll = ttk.Scrollbar(self.log_text, command=self.log_text.yview)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=log_scroll.set)

        # Buttons
        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X)

        self.btn_run = ttk.Button(btn_frame, text='\u25b6 Start backup',
                                    command=self.app._run_backup, style="Accent.TButton")
        self.btn_run.pack(side=tk.LEFT, padx=(0, 5))

        self.btn_cancel = ttk.Button(btn_frame, text='\u23f9 Cancel',
                                       command=self.app._cancel_backup, state=tk.DISABLED,
                                       style="Danger.TButton")
        self.btn_cancel.pack(side=tk.LEFT)

    # ── Helpers ────────────────────────────────

    def _append_log(self, text: str):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _clear_log(self):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def update_progress(self, current: int, total: int, filename: str = ""):
        """Update the progress bar, percentage label, and current file indicator.

        Intended to be called from the main thread (via root.after).
        """
        pct = min((current / total * 100) if total else 0, 100)
        self.progress_var.set(pct)
        self.lbl_progress_pct.configure(text=f"{pct:.0f}%")
        if filename:
            self.lbl_current_file.configure(text=f"File {current}/{total}: {filename}")
        elif current >= total and total > 0:
            self.lbl_current_file.configure(text="")

    def set_status(self, text: str):
        """Update the progress status label and append to log."""
        self.lbl_progress.configure(text=text)
        self._append_log(text)

    # ── Profile load / collect ─────────────────

    def load_profile(self, profile):
        """Update run tab info label from profile data."""
        from src.core.config import BackupType
        last = profile.last_backup or "Never"
        run_info = f"Profile: {profile.name}  |  Type: {profile.backup_type}  |  Last backup: {last}"
        if profile.backup_type == BackupType.DIFFERENTIAL.value:
            last_full = profile.last_full_backup or "Never"
            run_info += f"  |  Last full: {last_full}"
        self.lbl_run_profile.configure(text=run_info)

    def collect_config(self, profile):
        """No-op: run tab has no config to collect."""
        pass
