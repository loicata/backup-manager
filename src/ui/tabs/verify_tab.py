"""Verify tab: periodic integrity verification of existing backups."""

import contextlib
import tkinter as tk
from tkinter import ttk

from src.core.events import EventBus
from src.ui.theme import Colors, Fonts, Spacing


class VerifyTab(ttk.Frame):
    """Integrity verification: results table, progress bar, log output.

    Args:
        parent: Parent notebook widget.
        events: Event bus for progress and log events.
    """

    def __init__(self, parent, events: EventBus | None = None, **kwargs):
        super().__init__(parent, **kwargs)
        self._events = events or EventBus()
        self._running = False
        self._build_ui()
        self._subscribe_events()

    def _build_ui(self) -> None:
        # Header
        header = ttk.Label(self, text="Integrity verification", font=Fonts.title())
        header.pack(anchor="w", padx=Spacing.LARGE, pady=Spacing.LARGE)

        self.last_verify_label = ttk.Label(
            self,
            text="Last verification: Never",
            foreground=Colors.TEXT_SECONDARY,
        )
        self.last_verify_label.pack(anchor="w", padx=Spacing.LARGE)

        # S3 limitation notice
        notice = ttk.Label(
            self,
            text=(
                "Note: Encrypted backups (.tar.wbenc) on S3 are verified by size only "
                "(not full hash) — S3 providers guarantee data integrity at rest."
            ),
            foreground=Colors.TEXT_DISABLED,
            font=Fonts.small(),
        )
        notice.pack(anchor="w", padx=Spacing.LARGE, pady=(Spacing.SMALL, 0))

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self.start_btn = ttk.Button(
            btn_frame,
            text="Verify all backups",
            style="Accent.TButton",
        )
        self.start_btn.pack(side="left")

        self.cancel_btn = ttk.Button(
            btn_frame,
            text="Cancel",
            style="Accent.TButton",
        )
        self.cancel_btn.state(["disabled"])
        self.cancel_btn.pack(side="left", padx=Spacing.MEDIUM)

        # Results table
        results_frame = ttk.LabelFrame(self, text="Results", padding=Spacing.PAD)
        results_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        columns = ("destination", "backup", "status", "message")
        self.results_tree = ttk.Treeview(
            results_frame,
            columns=columns,
            show="headings",
            height=10,
        )
        self.results_tree.heading("destination", text="Destination")
        self.results_tree.heading("backup", text="Backup")
        self.results_tree.heading("status", text="Status")
        self.results_tree.heading("message", text="Details")

        self.results_tree.column("destination", width=100, minwidth=80)
        self.results_tree.column("backup", width=280, minwidth=200)
        self.results_tree.column("status", width=80, minwidth=60)
        self.results_tree.column("message", width=300, minwidth=150)

        tree_scroll = ttk.Scrollbar(
            results_frame, orient="vertical", command=self.results_tree.yview
        )
        self.results_tree.configure(yscrollcommand=tree_scroll.set)
        self.results_tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")

        # Configure tag colors for status
        self.results_tree.tag_configure("ok", foreground=Colors.SUCCESS)
        self.results_tree.tag_configure("corrupted", foreground=Colors.DANGER)
        self.results_tree.tag_configure("missing", foreground="#f39c12")
        self.results_tree.tag_configure("error", foreground=Colors.DANGER)

        # Progress section
        progress_frame = ttk.LabelFrame(self, text="Progress", padding=Spacing.PAD)
        progress_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self.progress_bar = ttk.Progressbar(progress_frame, mode="determinate", maximum=100)
        self.progress_bar.pack(fill="x")

        status_row = ttk.Frame(progress_frame)
        status_row.pack(fill="x", pady=(Spacing.SMALL, 0))

        self.status_label = ttk.Label(status_row, text="Idle", foreground=Colors.TEXT_SECONDARY)
        self.status_label.pack(side="left")

        self.percent_label = ttk.Label(status_row, text="0%", foreground=Colors.TEXT_SECONDARY)
        self.percent_label.pack(side="right")

        # Log output
        log_frame = ttk.LabelFrame(self, text="Log", padding=Spacing.PAD)
        log_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.log_text = tk.Text(
            log_frame,
            bg=Colors.LOG_BG,
            fg=Colors.LOG_TEXT,
            font=Fonts.mono(),
            wrap="word",
            state="disabled",
            height=8,
        )
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=log_scroll.set)
        self.log_text.pack(side="left", fill="both", expand=True)
        log_scroll.pack(side="right", fill="y")

    def _subscribe_events(self) -> None:
        """Subscribe to events — currently unused (updates via add_result)."""

    def set_running(self, running: bool) -> None:
        """Update button states based on running status.

        Args:
            running: True if verification is in progress.
        """
        self._running = running
        try:
            if running:
                self.start_btn.state(["disabled"])
                self.cancel_btn.state(["!disabled"])
                self.status_label.config(text="Running...", foreground=Colors.TEXT_SECONDARY)
            else:
                self.start_btn.state(["!disabled"])
                self.cancel_btn.state(["disabled"])
        except tk.TclError:
            pass

    def set_complete(self, ok_count: int, error_count: int, duration: float) -> None:
        """Update UI after verification completes.

        Args:
            ok_count: Number of backups that passed.
            error_count: Number of backups that failed.
            duration: Duration in seconds.
        """
        self._running = False
        try:
            self.start_btn.state(["!disabled"])
            self.cancel_btn.state(["disabled"])
            self.progress_bar["value"] = 100
            self.percent_label.config(text="100%")

            if error_count == 0:
                msg = f"All {ok_count} backups verified OK ({duration:.1f}s)"
                color = Colors.SUCCESS
            else:
                msg = f"{error_count} error(s), {ok_count} OK ({duration:.1f}s)"
                color = Colors.DANGER
            self.status_label.config(text=msg, foreground=color)
        except tk.TclError:
            pass

    def add_result(self, destination: str, backup_name: str, status: str, message: str) -> None:
        """Add a result row to the treeview and update progress.

        This is called on the main thread via root.after(), so all
        Tkinter updates are safe.

        Args:
            destination: Destination role (e.g., "primary").
            backup_name: Backup name.
            status: "ok", "corrupted", "missing", or "error".
            message: Detail message.
        """
        status_display = {
            "ok": "OK",
            "corrupted": "CORRUPTED",
            "missing": "MISSING",
            "error": "ERROR",
        }.get(status, status.upper())

        with contextlib.suppress(tk.TclError):
            self.results_tree.insert(
                "",
                "end",
                values=(destination, backup_name, status_display, message),
                tags=(status,),
            )
            self.results_tree.yview_moveto(1.0)

            self.status_label.config(text=f"{destination}: {backup_name}")

            # Append to log
            self.log_text.config(state="normal")
            self.log_text.insert(
                "end", f"  [{status_display}] {destination}/{backup_name}: {message}\n"
            )
            self.log_text.see("end")
            self.log_text.config(state="disabled")

    def clear(self) -> None:
        """Reset the tab for a new verification run."""
        with contextlib.suppress(tk.TclError):
            for item in self.results_tree.get_children():
                self.results_tree.delete(item)
            self.log_text.config(state="normal")
            self.log_text.delete("1.0", "end")
            self.log_text.config(state="disabled")
            self.progress_bar["value"] = 0
            self.percent_label.config(text="0%")
            self.status_label.config(text="Idle", foreground=Colors.TEXT_SECONDARY)

    def update_last_verify(self, timestamp: str) -> None:
        """Update the 'Last verification' label.

        Args:
            timestamp: Human-readable timestamp string.
        """
        with contextlib.suppress(tk.TclError):
            self.last_verify_label.config(text=f"Last verification: {timestamp}")
