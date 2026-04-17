"""Run tab: backup execution with progress and log display."""

import contextlib
import tkinter as tk
from tkinter import ttk

from src.core.events import (
    BACKUP_TYPE_DETERMINED,
    LOG,
    PHASE_CHANGED,
    PHASE_COUNT,
    PROGRESS,
    STATUS,
    EventBus,
)
from src.core.health_checker import DestinationHealth, format_bytes
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
        # Profile info baseline — so the BACKUP_TYPE_DETERMINED override
        # can be replaced with the canonical configured view once the
        # backup ends (STATUS = success / error / idle).
        self._profile_info_baseline: tuple[str, str, str, str] | None = None
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

        # Health dashboard (3 cards in a row)
        self._build_health_dashboard()

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

    def _build_health_dashboard(self):
        """Build the 3-card health dashboard row."""
        self._dashboard_frame = ttk.Frame(self)
        self._dashboard_frame.pack(
            fill="x",
            padx=Spacing.LARGE,
            pady=(Spacing.MEDIUM, 0),
        )

        # Card 1: Last backup
        self._card_last = self._make_card(self._dashboard_frame, "Last backup")
        self._card_last["frame"].pack(side="left", fill="both", expand=True)

        # Card 2: Next scheduled
        self._card_next = self._make_card(self._dashboard_frame, "Next scheduled")
        self._card_next["frame"].pack(
            side="left",
            fill="both",
            expand=True,
            padx=(Spacing.MEDIUM, 0),
        )

        # Card 3: Destinations
        self._card_dest = self._make_card(self._dashboard_frame, "Destinations")
        self._card_dest["frame"].pack(
            side="left",
            fill="both",
            expand=True,
            padx=(Spacing.MEDIUM, 0),
        )

        self._dest_labels: list[tuple[ttk.Label, ttk.Label]] = []

        # Default state (no profile selected yet)
        self.update_last_backup_card("")
        self.update_next_scheduled_card("—")
        self.update_destinations_card([])

    def _make_card(
        self,
        parent: ttk.Frame,
        title: str,
    ) -> dict:
        """Create a LabelFrame card with a content label.

        Args:
            parent: Parent frame.
            title: Card title.

        Returns:
            Dict with 'frame' and 'content' (inner frame for content).
        """
        frame = ttk.LabelFrame(parent, text=title, padding=Spacing.PAD)
        content = ttk.Frame(frame)
        content.pack(fill="both", expand=True)
        return {"frame": frame, "content": content}

    @staticmethod
    def _format_ago(timestamp: str) -> str:
        """Format an ISO timestamp as a human-readable 'ago' string.

        Args:
            timestamp: ISO format datetime string.

        Returns:
            String like "2h ago", "3d ago", or the raw timestamp on error.
        """
        from datetime import datetime

        try:
            dt = datetime.fromisoformat(timestamp)
            delta = datetime.now() - dt
            total_seconds = int(delta.total_seconds())
            if total_seconds >= 86400:
                return f"{total_seconds // 86400}d ago"
            if total_seconds >= 3600:
                return f"{total_seconds // 3600}h ago"
            if total_seconds >= 60:
                return f"{total_seconds // 60}min ago"
            return "Just now"
        except (ValueError, TypeError):
            return timestamp

    def update_last_backup_card(
        self,
        last_backup: str,
        files_count: int = 0,
        success: bool = True,
        is_differential: bool = False,
        last_full_backup: str = "",
        last_full_files_count: int = 0,
    ) -> None:
        """Update the Last backup card.

        Args:
            last_backup: ISO timestamp of last backup, or empty.
            files_count: Number of files in last backup.
            success: Whether last backup succeeded.
            is_differential: Whether the profile uses differential backups.
            last_full_backup: ISO timestamp of last full backup.
            last_full_files_count: Number of files in last full backup.
        """
        content = self._card_last["content"]
        for widget in content.winfo_children():
            widget.destroy()

        if not last_backup:
            ttk.Label(
                content,
                text="Never",
                foreground=Colors.TEXT_SECONDARY,
            ).pack(anchor="w")
            return

        ago = self._format_ago(last_backup)
        status_icon = "\u2713" if success else "\u2717"
        status_color = Colors.SUCCESS if success else Colors.DANGER

        # Line 1: status + ago + files count on same line
        files_str = f" \u00b7 {files_count:,} files" if files_count > 0 else ""
        ttk.Label(
            content,
            text=(
                f"{status_icon} Success \u2014 {ago}{files_str}"
                if success
                else f"{status_icon} Failed \u2014 {ago}{files_str}"
            ),
            foreground=status_color,
            font=Fonts.normal(),
        ).pack(anchor="w")

        # Line 2: last full info (only for differential profiles)
        if is_differential and last_full_backup:
            full_ago = self._format_ago(last_full_backup)
            full_files = (
                f" \u00b7 {last_full_files_count:,} files" if last_full_files_count > 0 else ""
            )
            ttk.Label(
                content,
                text=f"  Last full: {full_ago}{full_files}",
                foreground=Colors.TEXT_SECONDARY,
                font=Fonts.small(),
            ).pack(anchor="w")

    def update_next_scheduled_card(self, next_info: str) -> None:
        """Update the Next scheduled card.

        Args:
            next_info: Human-readable next run info from scheduler.
        """
        content = self._card_next["content"]
        for widget in content.winfo_children():
            widget.destroy()

        ttk.Label(
            content,
            text=next_info,
            foreground=Colors.TEXT_SECONDARY,
        ).pack(anchor="w")

    def update_destinations_card(
        self,
        destinations: list[tuple[str, str]],
    ) -> None:
        """Set up destination rows with loading placeholders.

        Args:
            destinations: List of (label, backend_type) for each
                configured destination. E.g. [("Storage", "local"), ...].
        """
        content = self._card_dest["content"]
        for widget in content.winfo_children():
            widget.destroy()
        self._dest_labels.clear()

        if not destinations:
            ttk.Label(
                content,
                text="Not configured",
                foreground=Colors.TEXT_SECONDARY,
            ).pack(anchor="w")
            return

        for label_text, _backend_type in destinations:
            row = ttk.Frame(content)
            row.pack(fill="x", anchor="w")

            name_lbl = ttk.Label(
                row,
                text=f"{label_text}:",
                font=Fonts.small(),
            )
            name_lbl.pack(side="left")

            status_lbl = ttk.Label(
                row,
                text="  ...",
                foreground=Colors.TEXT_SECONDARY,
                font=Fonts.small(),
            )
            status_lbl.pack(side="left", padx=(Spacing.SMALL, 0))

            self._dest_labels.append((name_lbl, status_lbl))

    def update_destination_status(
        self,
        index: int,
        health: DestinationHealth,
    ) -> None:
        """Update a single destination row after async check.

        Must be called on the main thread (use self.after()).

        Args:
            index: Destination index (0=storage, 1+=mirrors).
            health: Health check result.
        """
        if index >= len(self._dest_labels):
            return

        _name_lbl, status_lbl = self._dest_labels[index]

        if health.online is None:
            status_lbl.config(text="  ...", foreground=Colors.TEXT_SECONDARY)
        elif health.online:
            if health.free_bytes is not None:
                text = f"  {format_bytes(health.free_bytes)} free"
            else:
                text = "  \u2713 Online"
            status_lbl.config(text=text, foreground=Colors.SUCCESS)
        else:
            error_short = health.error[:30] if health.error else "Unreachable"
            status_lbl.config(
                text=f"  \u2717 {error_short}",
                foreground=Colors.DANGER,
            )

    def _subscribe_events(self):
        self._events.subscribe(PROGRESS, self._on_progress)
        self._events.subscribe(LOG, self._on_log)
        self._events.subscribe(STATUS, self._on_status)
        self._events.subscribe(PHASE_CHANGED, self._on_phase)
        self._events.subscribe(PHASE_COUNT, self._on_phase_count)
        self._events.subscribe(BACKUP_TYPE_DETERMINED, self._on_backup_type_determined)

    def _on_backup_type_determined(self, backup_type: str = "", forced_full: bool = False, **_):
        """Update the Run tab header with the effective backup_type.

        Fires once per backup after ``_maybe_force_full``. When an
        auto-promotion happened, display ``full (auto-promoted)`` so the
        user sees what is ACTUALLY running, not the configured DIFF.
        Thread-safe: the engine emits from the backup thread so we hop
        onto the main thread via ``after``.
        """
        self.after(0, self._apply_active_backup_type, backup_type, forced_full)

    def _apply_active_backup_type(self, backup_type: str, forced_full: bool) -> None:
        if self._profile_info_baseline is None:
            return
        name, _configured_type, last, last_full = self._profile_info_baseline
        type_display = (
            "full (auto-promoted)" if forced_full else backup_type or _configured_type
        )
        with contextlib.suppress(tk.TclError):
            self.profile_label.config(
                text=f"Profile: {name} | Type: {type_display} | Last backup: {last}"
            )

    def _on_phase_count(self, weights=None, **kw):
        """Receive phase weights for progress bar calculation.

        Each phase gets a share proportional to its weight.
        E.g. hashing=1, backup=2, upload=5 → upload gets 5/8 of the bar.
        """
        if weights:
            self._phase_weights = dict(weights)

    def _on_progress(self, current=0, total=0, filename="", phase="", **kw):
        """Schedule progress update on the main thread."""
        self.after(0, self._update_progress, current, total, filename, phase)

    def _update_progress(self, current, total, filename, phase):
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

        with contextlib.suppress(tk.TclError):
            self.progress_bar["value"] = self._last_pct
            self.percent_label.config(text=f"{self._last_pct}%")
            if filename:
                self.status_label.config(text=f"{phase}: {filename}")

    def _on_phase(self, phase="", **kw):
        """Schedule phase label update on the main thread."""
        self.after(0, self._update_phase, phase)

    def _update_phase(self, phase):
        with contextlib.suppress(tk.TclError):
            self.status_label.config(text=phase, foreground=Colors.ACCENT)

    def _on_log(self, message="", level="info", **kw):
        """Schedule log append on the main thread."""
        self.after(0, self._append_log, message)

    def _append_log(self, message):
        with contextlib.suppress(tk.TclError):
            self.log_text.config(state="normal")
            self.log_text.insert("end", f"  {message}\n")
            self.log_text.see("end")
            self.log_text.config(state="disabled")

    def _on_status(self, state="", **kw):
        """Schedule status update on the main thread."""
        self.after(0, self._update_status, state)

    def _update_status(self, state):
        with contextlib.suppress(tk.TclError):
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

    def update_profile_info(
        self,
        name: str,
        backup_type: str,
        last_backup: str,
        last_full_backup: str = "",
    ):
        """Refresh the Run tab header with profile configuration.

        When ``backup_type == "differential"`` and ``last_full_backup``
        is within ~5 minutes of ``last_backup``, the previous run was
        auto-promoted to FULL — surface this so the user understands
        why a supposedly incremental backup ran as a full one.
        """
        last = last_backup or "Never"
        type_display = backup_type
        if backup_type == "differential":
            if not last_backup:
                type_display = "differential (will auto-promote to full)"
            elif self._last_run_was_auto_promoted(last_backup, last_full_backup):
                type_display = "differential — last run: full (auto-promoted)"
        self._profile_info_baseline = (name, backup_type, last, last_full_backup)
        with contextlib.suppress(tk.TclError):
            self.profile_label.config(
                text=f"Profile: {name} | Type: {type_display} | Last backup: {last}"
            )

    @staticmethod
    def _last_run_was_auto_promoted(last_backup: str, last_full_backup: str) -> bool:
        """True when the two timestamps point to the same backup run.

        A DIFF that runs normally has ``last_backup > last_full_backup``
        (days apart). An auto-promoted FULL writes both fields within
        seconds of each other. Use a 5-minute window to stay robust to
        whatever overhead sits between ``_phase_update_delta`` (sets
        ``last_full_backup``) and the UI success callback (sets
        ``last_backup``).
        """
        if not last_backup or not last_full_backup:
            return False
        try:
            from datetime import datetime

            t1 = datetime.fromisoformat(last_backup)
            t2 = datetime.fromisoformat(last_full_backup)
        except (ValueError, TypeError):
            return False
        return abs((t1 - t2).total_seconds()) < 300.0

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
