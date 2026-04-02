"""Run tab: backup execution with progress and log display."""

import contextlib
import tkinter as tk
from tkinter import ttk

from src.core.events import LOG, PHASE_CHANGED, PHASE_COUNT, PROGRESS, STATUS, EventBus
from src.ui.theme import Colors, Fonts, Spacing


class RunTab(ttk.Frame):
    """Backup execution: progress bar, log output, start/cancel."""

    def __init__(self, parent, events: EventBus = None, **kwargs):
        super().__init__(parent, **kwargs)
        self._events = events or EventBus()
        self._phase_totals: dict[str, int] = {}
        self._phase_done: dict[str, int] = {}
        self._phase_order: list[str] = []
        self._phase_weights: dict[str, int] = {}
        self._last_pct = 0
        self._build_ui()
        self._subscribe_events()

    def _build_ui(self):
        # Header
        self.header_label = ttk.Label(self, text="Run backup", font=Fonts.title())
        self.header_label.pack(anchor="w", padx=Spacing.LARGE, pady=Spacing.LARGE)

        self.profile_label = ttk.Label(
            self,
            text="Profile: — | Type: — | Last backup: Never",
            foreground=Colors.TEXT_SECONDARY,
        )
        self.profile_label.pack(anchor="w", padx=Spacing.LARGE)

        # Progress section
        progress_frame = ttk.LabelFrame(self, text="Progress", padding=Spacing.PAD)
        progress_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self.progress_bar = ttk.Progressbar(
            progress_frame,
            mode="determinate",
            maximum=100,
        )
        self.progress_bar.pack(fill="x")

        status_row = ttk.Frame(progress_frame)
        status_row.pack(fill="x", pady=(Spacing.SMALL, 0))

        self.status_label = ttk.Label(
            status_row,
            text="Waiting...",
            foreground=Colors.TEXT_SECONDARY,
        )
        self.status_label.pack(side="left")

        self.percent_label = ttk.Label(
            status_row,
            text="0%",
            foreground=Colors.TEXT_SECONDARY,
        )
        self.percent_label.pack(side="right")

        # Log output
        log_frame = ttk.LabelFrame(self, text="Log", padding=Spacing.PAD)
        log_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self.log_text = tk.Text(
            log_frame,
            bg=Colors.LOG_BG,
            fg=Colors.LOG_TEXT,
            font=Fonts.mono(),
            wrap="word",
            state="disabled",
            height=15,
        )
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        self.log_text.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.start_btn = tk.Button(
            btn_frame,
            text="▶ Start backup",
            bg=Colors.ACCENT,
            fg="white",
            activebackground=Colors.ACCENT_HOVER,
            activeforeground="white",
            relief="flat",
            font=Fonts.normal(),
        )
        self.start_btn.pack(side="left")

        self.cancel_btn = tk.Button(
            btn_frame,
            text="■ Cancel",
            bg=Colors.DANGER,
            fg="white",
            activebackground="#c0392b",
            activeforeground="white",
            relief="flat",
            font=Fonts.normal(),
            state="disabled",
            disabledforeground=Colors.TEXT_DISABLED,
        )
        self.cancel_btn.pack(side="left", padx=Spacing.MEDIUM)

    def _subscribe_events(self):
        self._events.subscribe(PROGRESS, self._on_progress)
        self._events.subscribe(LOG, self._on_log)
        self._events.subscribe(STATUS, self._on_status)
        self._events.subscribe(PHASE_CHANGED, self._on_phase)
        self._events.subscribe(PHASE_COUNT, self._on_phase_count)

    def _on_phase_count(self, weights=None, **kw):
        """Receive phase weights for progress bar calculation.

        Each phase gets a share proportional to its weight.
        E.g. hashing=1, backup=2, upload=5 → upload gets 5/8 of the bar.
        """
        if weights:
            self._phase_weights = dict(weights)

    def _on_progress(self, current=0, total=0, filename="", phase="", **kw):
        if total <= 0:
            return

        # Track phase order
        if phase not in self._phase_totals:
            self._phase_totals[phase] = total
            self._phase_done[phase] = 0
            self._phase_order.append(phase)

        # Update phase done count
        self._phase_done[phase] = min(current, self._phase_totals.get(phase, total))

        # Each phase gets a share proportional to its weight.
        # Use ALL declared phases for total (not just seen ones),
        # so early phases don't inflate their share of the bar.
        all_phases = list(self._phase_weights.keys()) if self._phase_weights else []
        # Add any seen phase not declared in weights (safety fallback)
        for p in self._phase_order:
            if p not in all_phases:
                all_phases.append(p)
        total_weight = sum(self._phase_weights.get(p, 1) for p in all_phases)
        if total_weight <= 0:
            total_weight = 1

        pct = 0.0
        for p in self._phase_order:
            p_total = max(self._phase_totals.get(p, 1), 1)
            p_done = self._phase_done.get(p, 0)
            weight = self._phase_weights.get(p, 1)
            pct += (p_done / p_total) * (weight / total_weight) * 100.0

        pct_int = min(int(pct), 99)  # Never 100% — only on success

        # Monotone: never go backwards
        if pct_int >= self._last_pct:
            self._last_pct = pct_int

        try:
            self.progress_bar["value"] = self._last_pct
            self.percent_label.config(text=f"{self._last_pct}%")
            if filename:
                self.status_label.config(text=f"{phase}: {filename}")
        except tk.TclError:
            pass

    def _on_phase(self, phase="", **kw):
        with contextlib.suppress(tk.TclError):
            self.status_label.config(text=phase, foreground=Colors.ACCENT)

    def _on_log(self, message="", level="info", **kw):
        try:
            self.log_text.config(state="normal")
            self.log_text.insert("end", f"  {message}\n")
            self.log_text.see("end")
            self.log_text.config(state="disabled")
        except tk.TclError:
            pass

    def _on_status(self, state="", **kw):
        try:
            if state == "running":
                self.start_btn.config(state="disabled")
                self.cancel_btn.config(state="normal")
                self.status_label.config(text="Running...")
            elif state == "success":
                self.start_btn.config(state="normal")
                self.cancel_btn.config(state="disabled")
                self.progress_bar["value"] = 100
                self.percent_label.config(text="100%")
                self.status_label.config(text="Backup complete!", foreground=Colors.SUCCESS)
            elif state == "error":
                self.start_btn.config(state="normal")
                self.cancel_btn.config(state="disabled")
                self.status_label.config(text="Backup failed!", foreground=Colors.DANGER)
            elif state == "idle":
                self.start_btn.config(state="normal")
                self.cancel_btn.config(state="disabled")
                self.status_label.config(text="Waiting...", foreground=Colors.TEXT_SECONDARY)
        except tk.TclError:
            pass

    def update_profile_info(self, name: str, backup_type: str, last_backup: str):
        last = last_backup or "Never"
        self.profile_label.config(
            text=f"Profile: {name} | Type: {backup_type} | Last backup: {last}"
        )

    def clear_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")
        self.progress_bar["value"] = 0
        self.percent_label.config(text="0%")
        self._phase_totals.clear()
        self._phase_done.clear()
        self._phase_order.clear()
        self._phase_weights.clear()
        self._last_pct = 0
        self.status_label.config(text="Waiting...", foreground=Colors.TEXT_SECONDARY)
