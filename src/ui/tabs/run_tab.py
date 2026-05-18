"""Run tab: backup execution with progress and log display."""

import contextlib
import re
import tkinter as tk
from datetime import datetime
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
from src.core.file_categorizer import (
    CATEGORY_ORDER,
    categorize,
    extension_of,
)
from src.core.health_checker import DestinationHealth, format_bytes
from src.core.run_history import RunHistoryStore
from src.ui._status_text import truncate_status_text
from src.ui.theme import Colors, Fonts, Spacing

# PHASE_CHANGED carries the announcement message text rather than a
# short phase identifier (the engine emits ``"Collecting files..."``
# as the phase value, not ``"collector"``). We map it back to a short
# tag for the Log's Phase column so engine-level emits without an
# explicit phase don't leave the column blank for half the run.
_PHASE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"^Collecting", re.I), "collector"),
    (re.compile(r"^Filtering", re.I), "filter"),
    (re.compile(r"manifest", re.I), "manifest"),
    (re.compile(r"^(?:Copy|Upload).*(?:Storage|to mirror)", re.I), "writer"),
    (re.compile(r"commit marker", re.I), "commit_marker"),
    (re.compile(r"^Verifying", re.I), "verifier"),
    (re.compile(r"^Uploading to mirror", re.I), "mirror"),
    (re.compile(r"rotat", re.I), "rotator"),
)


def _infer_phase(announcement: str) -> str:
    """Return the short phase tag for an engine PHASE_CHANGED announcement.

    Empty string when no pattern matches — falls back to the previous
    phase tag in ``RunTab._current_phase``.
    """
    for pattern, phase in _PHASE_PATTERNS:
        if pattern.search(announcement):
            return phase
    return ""


# Terminal messages close the backup run. The fallback-to-previous-phase
# rule used elsewhere is misleading here because the last seen phase
# (``rotator`` in the success path, an earlier one if the run failed
# mid-pipeline) is not the *current* phase — nothing is running. We
# match these messages explicitly so the Phase column on the final
# row stays blank, signalling "done" without dragging a stale tag.
_TERMINAL_LOG_PATTERN: re.Pattern[str] = re.compile(r"^Backup (complete|failed|cancelled)", re.I)


def _is_terminal_log_message(message: str) -> bool:
    """True for the engine's final summary line of a run."""
    return bool(_TERMINAL_LOG_PATTERN.match(message))


class RunTab(ttk.Frame):
    """Backup execution: progress bar, log output, start/cancel."""

    def __init__(
        self,
        parent,
        events: EventBus = None,
        history_store: RunHistoryStore | None = None,
        **kwargs,
    ):
        super().__init__(parent, **kwargs)
        self._events = events or EventBus()
        self._phase_totals: dict[str, int] = {}
        self._phase_done: dict[str, int] = {}
        self._phase_order: list[str] = []
        self._phase_weights: dict[str, int] = {}
        self._last_pct = 0
        # Tracks the current pipeline phase as inferred from PHASE_CHANGED
        # announcements. Used to fill the Phase column for LOG events
        # that arrive without an explicit phase tag (typically engine-
        # level emits like "Saving manifest..." or "Backup complete").
        self._current_phase: str = ""
        # Profile info baseline — so the BACKUP_TYPE_DETERMINED override
        # can be replaced with the canonical configured view once the
        # backup ends (STATUS = success / error / idle).
        self._profile_info_baseline: tuple[str, str, str, str] | None = None
        # True only between STATUS=running and STATUS=success/error/idle.
        # Used to filter out PROGRESS events emitted by an independent
        # action (e.g. the user clicks "Verify all backups" in the
        # Verify tab) so this tab's progress bar does not move while no
        # backup is actually running here.
        self._backup_active: bool = False
        # Currently-selected profile id in the sidebar. Set by the app
        # on every profile-switch via ``set_current_profile_id``. Pipeline
        # events carry their own ``profile_id`` (since v3.7.12), and
        # ``_event_belongs_to_current_profile`` drops every event whose
        # tag does not match this id — that's how a background scheduler
        # run on profile A stops bleeding into the Run-tab view of
        # profile B (17/05/2026 user report).
        self._current_profile_id: str = ""
        # Per-profile persisted history of LOG events. ``None`` disables
        # persistence (kept for tests and embedding contexts that don't
        # care about cross-session restore). When set, every LOG event
        # with a non-empty profile_id is appended on the worker thread,
        # and ``set_current_profile_id`` re-renders the log_tree from
        # the new profile's file on every sidebar switch.
        self._history_store = history_store
        # Fast-mode verify prompts inserted as clickable rows inside
        # the log tree (since this iteration the cards live inline,
        # not in a separate alerts area). Keyed by the parent row's
        # item id. Each value tracks the action child ids + the
        # callbacks bound when the prompt was created. Not persisted
        # because callbacks die with the running session — switching
        # profiles drops in-flight prompts (rare; the periodic verify
        # cron still fires later if armed).
        self._verify_prompts: dict[str, dict] = {}
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

        # IMPORTANT: pack ``percent_label`` BEFORE ``status_label``.
        # Tk's pack manager gives priority to widgets packed first,
        # so packing the % first (``side="right"``) reserves its slot
        # on the right edge of the row.  ``status_label`` then takes
        # what's left with ``fill="x", expand=True``.  Without this
        # ordering, a long file path can claim the full width and
        # push the percent off-screen.
        self.percent_label = ttk.Label(
            status_row,
            text="0%",
            foreground=Colors.TEXT_SECONDARY,
        )
        self.percent_label.pack(side="right")

        self.status_label = ttk.Label(
            status_row,
            text="Waiting...",
            foreground=Colors.TEXT_SECONDARY,
            anchor="w",
        )
        self.status_label.pack(side="left", fill="x", expand=True)

        # Post-backup alerts area (Fast-mode verify prompts, etc.).
        # Empty by default — zero vertical height when no cards. Each
        # card is appended below previous ones, so a sequence of N
        # Fast-mode backups produces N visible cards stacked here
        # instead of N stacked modal Toplevels (the pre-v3.7.10 design
        # blocked the user with grab_set and broke profile chaining).
        self.alerts_frame = ttk.Frame(self)
        self.alerts_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.MEDIUM))

        # Log output — Treeview-based to mirror the Schedule journal
        # styling (clear background, structured rows). Events with a
        # ``details`` payload (e.g. the collector's "Skipped N file(s)"
        # summary) are rendered as expandable parents so the user can
        # drill down to see exactly which files were not backed up,
        # grouped by file type / extension. Plain events are flat rows.
        # Frame instead of LabelFrame: the bold "Log" title above the
        # tree was visually heavy and redundant with the implicit
        # context — the only multi-row scrollable widget on the tab.
        log_frame = ttk.Frame(self)
        log_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        # Two-column layout: #0 = Message (tree column with caret +
        # native indentation for children), "phase" = Phase (fixed
        # width on the right). Tk forces the tree column to render
        # first, so Phase ends up on the right — semantically
        # correct: the caret stays glued to the message it expands,
        # and child rows (categories / extensions / paths) leave the
        # Phase column empty (a path has no phase of its own).
        self.log_tree = ttk.Treeview(
            log_frame,
            columns=("phase",),
            show="tree headings",
            height=15,
            selectmode="browse",
        )
        # ``anchor="w"`` left-aligns the heading text. The default
        # is ``tk.CENTER``, which centred "Message" / "Phase" while
        # the cell content was left-aligned — the visual mismatch
        # made the columns look off-axis.
        self.log_tree.heading("#0", text="Message", anchor="w")
        self.log_tree.heading("phase", text="Phase", anchor="w")
        self.log_tree.column("#0", stretch=True)
        # 160 px fits the longest tag (``commit_marker``, 13 chars) with
        # a comfort margin under Win11's default Tk font. 130 px clipped
        # ``commit_marker`` to ``commit_mark…``.
        self.log_tree.column("phase", width=160, stretch=False, anchor="w")
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_tree.yview)
        self.log_tree.configure(yscrollcommand=scrollbar.set)
        self.log_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Tag styles: warning/error get a discreet background tint,
        # info stays default. Successful outcomes ("Verification OK",
        # "Backup complete") are NOT colored — the in-log green tint
        # was redundant with the green "✓ Success" pill at the top of
        # the Run tab, which is the canonical success indicator.
        self.log_tree.tag_configure("warning", background="#fff8e0")
        self.log_tree.tag_configure("error", background="#fde8e8")
        self.log_tree.tag_configure("muted", foreground="#666666")
        # Inline Fast-mode verify prompt rows. Parent reuses the
        # success palette so it visually flags the run completion;
        # action rows render in the accent color so the user reads
        # them as clickable. The toggle row keeps the default
        # foreground because it is a checkbox-style line, not a
        # primary action.
        self.log_tree.tag_configure(
            "verify_parent",
            foreground=Colors.SUCCESS,
        )
        self.log_tree.tag_configure(
            "verify_action",
            foreground=Colors.ACCENT,
        )
        self.log_tree.tag_configure("verify_toggle")

        # Lazy-load state for the Skipped subtree. The full payload
        # (which can run into hundreds of thousands of paths on a
        # pathological workload) lives in this dict; widgets are only
        # created when the user expands a sub-node. Keys are tree item
        # IDs, values describe what to materialize on demand.
        self._lazy_subtrees: dict[str, dict] = {}
        self.log_tree.bind("<<TreeviewOpen>>", self._on_tree_open)
        # ``add="+"`` so the binding coexists with the default
        # Treeview selection handlers — without it Tk drops the
        # built-in row selection visual that helps users see which
        # action they are about to fire.
        self.log_tree.bind("<Button-1>", self._on_log_tree_click, add="+")

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

    @staticmethod
    def _format_bytes(size_bytes: int) -> str:
        """Format a byte count as a short human-readable string.

        Mirrors ``email_notifier._format_size`` so the Run-tab card
        and the success email agree on the number the user sees
        (e.g. ``44.39 GB``).
        """
        if size_bytes < 1024:
            return f"{size_bytes} B"
        if size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        if size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    def update_last_backup_card(
        self,
        last_backup: str,
        files_count: int = 0,
        bytes_source: int = 0,
        success: bool = True,
        is_differential: bool = False,
        last_full_backup: str = "",
        last_full_files_count: int = 0,
    ) -> None:
        """Update the Last backup card.

        Args:
            last_backup: ISO timestamp of last backup, or empty.
            files_count: Number of files in last backup.
            bytes_source: Total source size in bytes (0 = unknown,
                shown only when > 0).
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

        # Line 2: source size (only when we have a real measurement \u2014
        # older journal entries from before bytes_source existed leave
        # it at 0, in which case we just suppress the line rather than
        # show a misleading "0 B").
        if bytes_source > 0:
            ttk.Label(
                content,
                text=f"  Source size: {self._format_bytes(bytes_source)}",
                foreground=Colors.TEXT_SECONDARY,
                font=Fonts.small(),
            ).pack(anchor="w")

        # Line 3: last full info (only for differential profiles)
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

    def _on_backup_type_determined(
        self,
        backup_type: str = "",
        forced_full: bool = False,
        profile_id: str = "",
        **_,
    ):
        """Update the Run tab header with the effective backup_type.

        Fires once per backup after ``_maybe_force_full``. When an
        auto-promotion happened, display ``full (auto-promoted)`` so the
        user sees what is ACTUALLY running, not the configured DIFF.
        Thread-safe: the engine emits from the backup thread so we hop
        onto the main thread via ``after``.
        """
        if not self._event_belongs_to_current_profile(profile_id):
            return
        self.after(0, self._apply_active_backup_type, backup_type, forced_full)

    def _apply_active_backup_type(self, backup_type: str, forced_full: bool) -> None:
        if self._profile_info_baseline is None:
            return
        name, _configured_type, last, last_full = self._profile_info_baseline
        type_display = "full (auto-promoted)" if forced_full else backup_type or _configured_type
        with contextlib.suppress(tk.TclError):
            self.profile_label.config(
                text=f"Profile: {name} | Type: {type_display} | Last backup: {last}"
            )

    def _event_belongs_to_current_profile(self, event_profile_id) -> bool:
        """Return True if this event should drive the Run-tab view.

        v3.7.12 contract: every pipeline event is tagged with the
        profile id of the backup that produced it (engine wraps its
        EventBus in ``ProfileTaggingEventBus`` for the duration of
        ``run_backup``). The Run-tab is bound to whichever profile
        the user has selected in the sidebar — events from any other
        profile must not move the bar, status label, log tree, or
        alerts area.

        Untagged events (``profile_id`` missing or empty) pass through
        for backwards compatibility: tray-emitted notices, the verify
        loop on the Verify tab, and any test fixture that builds an
        engine without a profile context.

        Args:
            event_profile_id: Value of the ``profile_id`` kwarg the
                event carried — typically a 32-char UUID hex or empty.

        Returns:
            True when the event should be consumed by the handler.
        """
        if not event_profile_id:
            return True
        if not self._current_profile_id:
            # No profile selected yet (cold start, between deletes) —
            # accept everything so the very first profile click sees
            # the events that may have arrived in the meantime.
            return True
        return event_profile_id == self._current_profile_id

    def set_current_profile_id(self, profile_id: str) -> None:
        """Bind the Run-tab to a specific profile id for event filtering.

        Called by ``BackupManagerApp._load_profile`` on every sidebar
        switch. After this call, only events tagged with
        ``profile_id`` (or untagged events) reach the handlers.

        Triggers (in order) a reset of the volatile widgets (progress
        bar, status label, phase counters) and a re-render of the
        log_tree from the new profile's persisted history. Without
        the reset, the bar and the "Scanning..." text would stay
        frozen on the previous profile's values when its run is the
        active one and we switch to a non-running profile (the
        live PROGRESS events get filtered out and never overwrite
        the stale state). No-ops when the id is unchanged so
        re-selecting the same profile does not blink the log.
        """
        previous_id = self._current_profile_id
        self._current_profile_id = profile_id or ""
        if profile_id and profile_id != previous_id:
            self._clear_run_state()
            self._reload_log_history()

    def _reload_log_history(self) -> None:
        """Repopulate the log_tree from the current profile's history.

        Runs on the main thread (called from ``set_current_profile_id``
        which is invoked from the UI's profile-switch path). Each
        entry is rendered through ``_append_log`` so structured payloads
        (skipped categories, exclude patterns) keep their lazy-load
        tree shape just like a live run. When the last persisted line
        is a terminal one (``Backup complete|failed|cancelled``) the
        success-pill style (green label, bar at 100 %) is restored so
        that re-selecting a profile right after its run finishes does
        not flash a misleading "Waiting..." over the log of a clearly
        successful backup.

        When no history store is attached the widget is just cleared,
        matching the legacy "blank slate on switch" behaviour.
        """
        self._clear_log_widget()
        if self._history_store is None or not self._current_profile_id:
            return
        entries = self._history_store.load(self._current_profile_id)
        for entry in entries:
            self._append_log(
                entry.get("msg", ""),
                entry.get("level", "info"),
                entry.get("phase", ""),
                entry.get("details"),
            )
        self._restore_terminal_status(entries)

    def _restore_terminal_status(self, entries: list[dict]) -> None:
        """Re-apply the status pill colour after a reload.

        ``_clear_run_state`` (called just before this on profile
        switch) reset the progress bar to 0 % and the status label to
        "Waiting...". For a profile whose last run already completed,
        that overwrite hides the "Success" / "Failed" visual cue. We
        scan from the tail of ``entries`` for the last terminal log
        line and re-apply the matching style — but only when it is
        unambiguously the trailing event of the history, otherwise a
        random "Backup complete" buried under later activity would
        wrongly flip the bar to 100 %.
        """
        if not entries:
            return
        last_msg = entries[-1].get("msg", "")
        if not _is_terminal_log_message(last_msg):
            return
        with contextlib.suppress(tk.TclError):
            if last_msg.lower().startswith("backup complete"):
                self.progress_bar["value"] = 100
                self.percent_label.config(text="100%")
                self.status_label.config(
                    text="Backup complete!",
                    foreground=Colors.SUCCESS,
                )
            elif last_msg.lower().startswith("backup failed"):
                self.status_label.config(
                    text="Backup failed!",
                    foreground=Colors.DANGER,
                )
            elif last_msg.lower().startswith("backup cancelled"):
                self.status_label.config(
                    text="Backup cancelled",
                    foreground=Colors.TEXT_SECONDARY,
                )

    def _clear_log_widget(self) -> None:
        """Remove every row from the log tree and reset lazy state.

        Extracted from ``clear_log`` so the profile-switch swap can
        clear just the log without also resetting progress bar / phase
        counters / alerts (those are reset by ``clear_log`` for the
        "new run" entry point).
        """
        with contextlib.suppress(tk.TclError):
            self.log_tree.delete(*self.log_tree.get_children(""))
        self._lazy_subtrees.clear()
        # Inline verify prompts live as Treeview rows; deleting all
        # children drops them too, so the callback registry must be
        # cleared as well — otherwise a click on a future row that
        # happens to share an item id would dispatch to a stale
        # callback.
        self._verify_prompts.clear()
        # Reset the current phase tracker so the first row of the
        # next backup ("Backup type: full" etc.) does not inherit
        # the previous run's last phase.
        self._current_phase = ""

    def _persist_log(
        self,
        message: str,
        level: str,
        phase: str,
        details: dict | None,
        profile_id: str,
    ) -> None:
        """Append a LOG event to the profile's persistent history.

        Called from the worker thread on every LOG arrival, BEFORE
        the per-profile widget filter so that background runs on a
        non-selected profile are still preserved in their own
        history file. No-op when persistence is disabled or the
        event carries no profile id.
        """
        if self._history_store is None or not profile_id:
            return
        entry: dict = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "msg": message,
            "level": level,
            "phase": phase,
        }
        if details is not None:
            entry["details"] = details
        self._history_store.append(profile_id, entry)

    def _on_phase_count(self, weights=None, profile_id="", **kw):
        """Receive phase weights for progress bar calculation.

        Each phase gets a share proportional to its weight.
        E.g. hashing=1, backup=2, upload=5 → upload gets 5/8 of the bar.
        """
        if not self._event_belongs_to_current_profile(profile_id):
            return
        if weights:
            self._phase_weights = dict(weights)

    def _on_progress(self, current=0, total=0, filename="", phase="", profile_id="", **kw):
        """Schedule progress update on the main thread.

        Ignores PROGRESS events while no backup is active on this tab.
        The same EventBus is shared with the Verify tab, so a manual
        verify launched from there would otherwise push the bar back
        to a "verifying..." view even when the user is just looking
        at this tab between runs.
        """
        if not self._event_belongs_to_current_profile(profile_id):
            return
        if not self._backup_active:
            return
        self.after(0, self._update_progress, current, total, filename, phase)

    def _update_progress(self, current, total, filename, phase):
        # Indeterminate scan heartbeat (collector walking the source
        # tree): ``total == 0`` signals "no total yet, just keep the
        # UI alive". Update the status label only — the determinate
        # progress bar stays at 0 % until manifest / write / verify
        # report real ratios. Without this, a 100 k-file walk shows
        # nothing for ~60 s between the "Applying exclude patterns"
        # log line and the "Collected N files" one, and the user
        # legitimately thinks the app froze.
        if total == 0 and current > 0:
            with contextlib.suppress(tk.TclError):
                self.status_label.config(
                    text=f"Scanning... {filename}" if filename else "Scanning...",
                    foreground=Colors.TEXT_SECONDARY,
                )
            return

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
                self.status_label.config(text=truncate_status_text(phase, filename))

    def _on_phase(self, phase="", profile_id="", **kw):
        """Schedule phase label update on the main thread.

        Also updates ``_current_phase`` so subsequent LOG events
        without an explicit phase tag inherit it for the Log's
        Phase column. Falls back to the previous tag when the
        announcement is unrecognised — better stale than blank.
        """
        if not self._event_belongs_to_current_profile(profile_id):
            return
        inferred = _infer_phase(phase)
        if inferred:
            self._current_phase = inferred
        self.after(0, self._update_phase, phase)

    def _update_phase(self, phase):
        with contextlib.suppress(tk.TclError):
            # Status messages stay grey throughout the run — only the
            # final "Backup complete!" pill turns green (success) and
            # "Backup failed!" red (error). The previous Colors.ACCENT
            # blue made the user perceive each phase as "highlighted"
            # which competed with the canonical success / failure
            # signal at the end.
            self.status_label.config(text=phase, foreground=Colors.TEXT_SECONDARY)

    def _on_log(self, message="", level="info", phase="", details=None, profile_id="", **kw):
        """Schedule log append on the main thread.

        Engine-level emits (``backup_engine._log`` / ``_phase``) do
        not carry a phase tag in their LOG event — they would show a
        blank Phase cell in the tree. We fill the gap two ways:

        1. **Self-detect from the message**: ``_phase()`` emits
           ``LOG`` with messages like ``"Filtering changed files..."``
           that match ``_PHASE_PATTERNS``. We update ``_current_phase``
           BEFORE adding the row so this very announcement carries
           its own phase tag — without this, the row would be tagged
           with the previous phase because PHASE_CHANGED always fires
           AFTER the LOG event in ``backup_engine._phase()``.
        2. **Inherit current phase**: messages that don't match a
           phase pattern (``"Backup written: ..."``, ``"Manifest
           created: ..."``) inherit the last known phase so they
           stay aligned with their announcing parent.

        Cross-tab isolation: this tab shares LOG with the Verify tab
        (and any future emitter). A manual "Verify all backups" click
        in the Verify tab would otherwise drop "Verification OK: N/N"
        rows into THIS tab's Message panel between runs. Drop log
        events when no backup is currently active here, matching the
        same gate the PROGRESS subscriber uses.

        Terminal log lines (``Backup complete: …`` etc.) are always
        let through. The engine emits ``STATUS=success`` immediately
        before the terminal log, and on Windows Tk can process the
        resulting ``_update_status`` (which flips ``_backup_active``
        to False) before the terminal log's ``after(0, _append_log)``
        is even scheduled — silently dropping the only row that
        carries the run duration. Terminal lines are also unique to
        this tab's engine (``_TERMINAL_LOG_PATTERN`` matches
        ``Backup complete|failed|cancelled`` which the Verify tab
        never emits), so an unconditional exemption is safe from
        cross-tab pollution.

        Persistence happens BEFORE the per-profile widget filter so
        that a background scheduler run on a non-selected profile
        still feeds its own history file — re-selecting that profile
        later restores the full row stream via ``_reload_log_history``.
        """
        self._persist_log(message, level, phase, details, profile_id)
        if not self._event_belongs_to_current_profile(profile_id):
            return
        is_terminal = _is_terminal_log_message(message)
        if not self._backup_active and not is_terminal:
            return
        if not phase:
            if is_terminal:
                # Backup is finished: no phase is active any more. Reset
                # ``_current_phase`` so we don't leak a stale tag into a
                # follow-up run, and force the column blank for this row.
                self._current_phase = ""
                phase = ""
            else:
                inferred = _infer_phase(message)
                if inferred:
                    self._current_phase = inferred
                phase = self._current_phase
        self.after(0, self._append_log, message, level, phase, details)

    def _append_log(self, message, level="info", phase="", details=None):
        """Insert a log entry into the Treeview.

        Two columns: ``#0`` (tree column) carries the message + caret
        + indentation, ``phase`` carries the phase name on the right.
        Child rows (categories / extensions / paths under Skipped or
        the patterns under Applying exclude patterns) leave the phase
        cell empty — a leaf path has no phase of its own.

        Three rendering shapes:

        1. **Plain event** (``details is None``): one row.
        2. **Exclude-pattern listing** (``details = {"patterns": [...]}``):
           parent row + one child per pattern (eager, cheap).
        3. **Skipped summary** (``details`` has ``permission_denied`` /
           ``os_errors`` / ``excluded_by_pattern`` keys): parent row +
           lazy category placeholders. Categories materialize their
           extension+path children only when the user expands them.
        """
        with contextlib.suppress(tk.TclError):
            tags = self._tags_for(level, message)
            phase_value = (phase,)

            if details is None:
                self.log_tree.insert("", "end", text=message, values=phase_value, tags=tags)
            elif "patterns" in details:
                parent = self.log_tree.insert(
                    "", "end", text=message, values=phase_value, tags=tags, open=False
                )
                for pat in details["patterns"]:
                    self.log_tree.insert(parent, "end", text=pat, values=("",), tags=("muted",))
            elif self._is_skipped_payload(details):
                self._insert_skipped_node(message, phase, tags, details)
            else:
                # Unknown payload shape — render as plain row to be safe.
                self.log_tree.insert("", "end", text=message, values=phase_value, tags=tags)

            self._scroll_to_end()

    @staticmethod
    def _tags_for(level: str, message: str) -> tuple[str, ...]:
        """Pick Treeview tags based on the log level.

        Only ``warning`` and ``error`` get a colored row. INFO lines —
        including success outcomes like "Verification OK" or "Backup
        complete" — stay on the default background because the green
        "Success" pill at the top of the Run tab already conveys the
        result and an in-log echo would be visual duplication.
        """
        del message  # noqa — kept for API stability if heuristics return later
        if level == "error":
            return ("error",)
        if level == "warning":
            return ("warning",)
        return ()

    @staticmethod
    def _is_skipped_payload(details: dict) -> bool:
        """True when ``details`` matches the collector's skipped summary."""
        return any(k in details for k in ("permission_denied", "os_errors", "excluded_by_pattern"))

    def _insert_skipped_node(
        self,
        message: str,
        phase: str,
        tags: tuple[str, ...],
        details: dict,
    ) -> None:
        """Create the Skipped parent + lazy category placeholders.

        We pre-compute the per-category buckets here (cheap categorical
        partitioning of all the skipped paths) and stash them in
        ``_lazy_subtrees``. The category nodes are inserted as visible
        rows; their children (extensions and individual paths) are
        only materialized when the user expands a category, which
        happens via ``_on_tree_open``.
        """
        # Materialize one entry per skipped path with (path, reason)
        # so all categories share the same per-row contract downstream.
        # ``reason`` is what we display in grey at the right of the path.
        all_paths: list[tuple[str, str]] = []
        for path in details.get("permission_denied", []):
            all_paths.append((path, "permission denied"))
        for path, msg in details.get("os_errors", []):
            all_paths.append((path, f"OS error: {msg}"))
        for path, pattern in details.get("excluded_by_pattern", []):
            all_paths.append((path, f"excluded: {pattern}"))

        # Bucketize by category, preserving display order. Categories
        # with zero entries are intentionally suppressed at render time
        # to keep the visual weight proportional to the actual data.
        buckets: dict[str, list[tuple[str, str]]] = {c: [] for c in CATEGORY_ORDER}
        for path, reason in all_paths:
            buckets[categorize(path)].append((path, reason))

        # Parent node for the whole Skipped summary. Phase is set on
        # the parent only — child rows (categories, extensions, paths)
        # have ``values=("",)`` because they are not pipeline events.
        parent = self.log_tree.insert(
            "", "end", text=message, values=(phase,), tags=tags, open=False
        )

        # Insert one stub per non-empty category. The Treeview needs at
        # least one child to render a caret; we add a transient
        # placeholder that ``_on_tree_open`` replaces with real content
        # the first time the category is expanded.
        for category in CATEGORY_ORDER:
            entries = buckets[category]
            if not entries:
                continue
            cat_text = f"{category}  ({len(entries)})"
            cat_node = self.log_tree.insert(parent, "end", text=cat_text, values=("",))
            placeholder = self.log_tree.insert(cat_node, "end", text="…", values=("",))
            self._lazy_subtrees[cat_node] = {
                "kind": "category",
                "entries": entries,
                "placeholder": placeholder,
            }

    def _on_tree_open(self, _event=None) -> None:
        """Materialize lazy subtree contents on first expand."""
        item = self.log_tree.focus()
        if not item:
            return
        spec = self._lazy_subtrees.pop(item, None)
        if spec is None:
            return  # Already materialized or never lazy.

        with contextlib.suppress(tk.TclError):
            # Drop the placeholder before inserting the real children
            # so the user does not see a transient "…" + content frame.
            self.log_tree.delete(spec["placeholder"])

        if spec["kind"] == "category":
            self._materialize_category(item, spec["entries"])

    def _materialize_category(self, category_node: str, entries: list[tuple[str, str]]) -> None:
        """Build extension sub-groups + path leaves under a category.

        Sub-groups are sorted by path count descending so the heaviest
        offender shows up first when the category is opened — the user
        is most likely to find their file there.
        """
        by_extension: dict[str, list[tuple[str, str]]] = {}
        for path, reason in entries:
            ext = extension_of(path) or "(no extension)"
            by_extension.setdefault(ext, []).append((path, reason))

        sorted_exts = sorted(
            by_extension.items(),
            key=lambda kv: (-len(kv[1]), kv[0]),
        )
        for ext, items in sorted_exts:
            ext_text = f"{ext}  ({len(items)})"
            ext_node = self.log_tree.insert(category_node, "end", text=ext_text, values=("",))
            # Path-level rows are leaves — no further lazy load. We
            # sort alphabetically for stable navigation.
            for path, reason in sorted(items):
                # Use multiple spaces so the reason aligns roughly
                # right of the path on common widths. A monospace font
                # would do better but we are intentionally on the
                # standard proportional font for visual consistency
                # with Schedule journal.
                leaf_text = f"{path}    {reason}"
                self.log_tree.insert(ext_node, "end", text=leaf_text, values=("",), tags=("muted",))

    def _scroll_to_end(self) -> None:
        """Scroll the log tree to the last top-level row.

        Mimics the auto-scroll of the legacy ``tk.Text`` so newly
        emitted events are always visible. The user can manually scroll
        up; the next event will tug them back down — same behaviour as
        before, no surprise.
        """
        children = self.log_tree.get_children("")
        if children:
            with contextlib.suppress(tk.TclError):
                self.log_tree.see(children[-1])

    def _on_status(self, state="", profile_id="", **kw):
        """Schedule status update on the main thread."""
        if not self._event_belongs_to_current_profile(profile_id):
            return
        self.after(0, self._update_status, state)

    def _update_status(self, state):
        # Track whether a backup is currently active here so PROGRESS
        # events from other tabs (e.g. Verify) are filtered out.
        if state == "running":
            self._backup_active = True
        elif state in ("success", "error", "idle"):
            self._backup_active = False
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
        """Reset the Run tab to a blank slate.

        Wipes the log tree plus the volatile run state (progress bar,
        phase counters, status label) and dismisses any pending
        Fast-mode verify alerts. Called when a new backup starts on
        the current profile so the previous run's UI is gone.

        Profile-switch goes through ``set_current_profile_id`` (which
        also resets the volatile state via ``_clear_run_state`` but
        keeps alerts so a Fast-mode prompt for the new profile is
        not lost).
        """
        self._clear_log_widget()
        self._clear_run_state()
        self.clear_alerts()

    def _clear_run_state(self) -> None:
        """Reset the progress bar, status label and phase counters.

        Extracted from ``clear_log`` so the profile-switch path can
        return the bar/label to their idle baseline without also
        wiping the log_tree (which is repopulated separately from
        the per-profile history file) or the alerts. Leaves
        ``_backup_active`` untouched — that flag is the cross-tab
        contract that gates PROGRESS events from the Verify tab and
        must follow STATUS events, not user navigation.
        """
        with contextlib.suppress(tk.TclError):
            self.progress_bar["value"] = 0
            self.percent_label.config(text="0%")
            self.status_label.config(
                text="Waiting...",
                foreground=Colors.TEXT_SECONDARY,
            )
        self._phase_totals.clear()
        self._phase_done.clear()
        self._phase_order.clear()
        self._phase_weights.clear()
        self._last_pct = 0

    def show_verify_prompt(
        self,
        profile_name: str,
        periodic_armed: bool,
        interval_days: int,
        on_verify_now,
        on_dismiss,
        on_dont_ask_again,
    ) -> str:
        """Insert an inline Fast-mode verify prompt into the log tree.

        Four rows are appended:

        * parent — ``✓ Backup '<name>' complete — verification skipped``
        * info — periodic verify status / "no periodic" warning
        * ``▶ Verify now`` — click fires ``on_verify_now`` and removes
          the four rows
        * ``✕ Dismiss`` — click fires ``on_dismiss`` and removes them
        * ``☐ Don't ask again for this profile`` — click toggles the
          checkbox glyph and forwards the new state to ``on_dont_ask_again``

        The action rows survive scrolling (they are real Treeview rows,
        not overlay widgets) and are removed atomically with their
        parent when an action is taken. Switching profile clears them
        — there is no persistence because the callbacks are tied to
        the running app session.

        Args:
            profile_name: Display name of the profile that just finished.
            periodic_armed: Whether periodic verify is scheduled.
                Drives the info line copy + colour.
            interval_days: Days until the next periodic verify.
                Ignored when ``periodic_armed`` is False.
            on_verify_now: Callback fired on ``Verify now``. The four
                rows are removed first, then this runs. No arguments.
            on_dismiss: Callback fired on ``Dismiss``. Same lifecycle.
            on_dont_ask_again: Callback fired on every toggle of the
                checkbox row. Receives the new bool state.

        Returns:
            The parent row's item id (useful for tests).
        """
        parent_text = (
            f"✓ Backup '{profile_name}' complete — verification skipped (Fast mode)"
        )
        parent_id = self.log_tree.insert(
            "",
            "end",
            text=parent_text,
            values=("",),
            tags=("verify_parent",),
            open=True,
        )

        if periodic_armed:
            day_word = "day" if interval_days == 1 else "days"
            info_text = f"Next periodic verification in {interval_days} {day_word}."
            info_tag: tuple[str, ...] = ("muted",)
        else:
            info_text = "No periodic verification is scheduled for this profile."
            info_tag = ("warning",)
        self.log_tree.insert(
            parent_id, "end", text=info_text, values=("",), tags=info_tag
        )

        verify_item = self.log_tree.insert(
            parent_id,
            "end",
            text="▶  Verify now",
            values=("",),
            tags=("verify_action",),
        )
        dismiss_item = self.log_tree.insert(
            parent_id,
            "end",
            text="✕  Dismiss",
            values=("",),
            tags=("verify_action",),
        )
        dont_ask_item = self.log_tree.insert(
            parent_id,
            "end",
            text="☐  Don't ask again for this profile",
            values=("",),
            tags=("verify_toggle",),
        )

        self._verify_prompts[parent_id] = {
            "verify_item": verify_item,
            "dismiss_item": dismiss_item,
            "dont_ask_item": dont_ask_item,
            "dont_ask_state": False,
            "on_verify": on_verify_now,
            "on_dismiss": on_dismiss,
            "on_dont_ask": on_dont_ask_again,
        }
        self._scroll_to_end()
        return parent_id

    def _on_log_tree_click(self, event) -> None:
        """Dispatch clicks on actionable verify-prompt rows.

        Identifies the row under the cursor and matches it against
        every registered prompt's action items. Returns silently for
        non-prompt rows so the default Treeview selection behaviour
        is preserved everywhere else.
        """
        item = self.log_tree.identify_row(event.y)
        if not item:
            return
        for parent_id, spec in list(self._verify_prompts.items()):
            if item == spec["verify_item"]:
                self._destroy_verify_prompt(parent_id)
                spec["on_verify"]()
                return
            if item == spec["dismiss_item"]:
                self._destroy_verify_prompt(parent_id)
                spec["on_dismiss"]()
                return
            if item == spec["dont_ask_item"]:
                self._toggle_dont_ask(parent_id, spec)
                return

    def _toggle_dont_ask(self, parent_id: str, spec: dict) -> None:
        """Flip the ``Don't ask again`` glyph and notify the callback."""
        new_state = not spec["dont_ask_state"]
        spec["dont_ask_state"] = new_state
        glyph = "☑" if new_state else "☐"
        with contextlib.suppress(tk.TclError):
            self.log_tree.item(
                spec["dont_ask_item"],
                text=f"{glyph}  Don't ask again for this profile",
            )
        spec["on_dont_ask"](new_state)
        # ``parent_id`` is unused here but kept in the signature so the
        # call site reads as a coherent "toggle this prompt's row".
        del parent_id

    def _destroy_verify_prompt(self, parent_id: str) -> None:
        """Remove the four rows of a verify prompt and forget callbacks."""
        with contextlib.suppress(tk.TclError):
            self.log_tree.delete(parent_id)
        self._verify_prompts.pop(parent_id, None)

    def clear_alerts(self) -> None:
        """Remove every pending verify prompt and clear the alerts frame.

        Inline verify prompts (since this iteration) live as rows of
        the log tree; the legacy ``alerts_frame`` is still emptied for
        any external code path that might still parent widgets there.
        """
        for parent_id in list(self._verify_prompts):
            self._destroy_verify_prompt(parent_id)
        with contextlib.suppress(tk.TclError):
            for child in list(self.alerts_frame.winfo_children()):
                child.destroy()
