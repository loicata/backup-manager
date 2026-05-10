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
from src.core.file_categorizer import (
    CATEGORY_ORDER,
    categorize,
    extension_of,
)
from src.core.health_checker import DestinationHealth, format_bytes
from src.ui.theme import Colors, Fonts, Spacing

# Maximum characters displayed in the "phase: filename" status line.
# Beyond this length the path is truncated with a leading ellipsis so the
# percent label on the right of the same row stays visible.  Calibrated
# for a ~1400 px window with the default Tk font: 80 chars leaves ~6-8
# chars of empty pad before the % column even on smaller screens.
_STATUS_MAX_CHARS = 80


def _truncate_status_text(phase: str, filename: str, max_chars: int = _STATUS_MAX_CHARS) -> str:
    """Build a "phase: filename" line that never exceeds ``max_chars``.

    The truncation keeps the **end** of the path (basename + a few
    parents) which is the part the user actually wants to see — the
    leading components are replaced with ``...``.

    Why we truncate at all: ``status_label`` and ``percent_label`` share
    a horizontal row with ``fill="x"``.  Tk Labels do not clip their
    own text, so a 200-char path requests a 200-char-wide label and
    pushes the percent off-screen even when ``side="right"`` is used.

    Examples:
        >>> _truncate_status_text("hashing", "a.txt", max_chars=80)
        'hashing: a.txt'
        >>> _truncate_status_text("hashing", "/very/long/path/...long.../mail.eml", max_chars=30)
        'hashing: ...g.../mail.eml'

    Args:
        phase: Phase name (``hashing``, ``upload``, ...).  May be empty.
        filename: File path being processed.  May be empty.
        max_chars: Hard cap on the returned string length.

    Returns:
        Display-ready status line bounded by ``max_chars``.
    """
    if not filename:
        return phase[:max_chars]
    full = f"{phase}: {filename}" if phase else filename
    if len(full) <= max_chars:
        return full

    prefix = f"{phase}: ..." if phase else "..."
    # If even the prefix doesn't fit, hard-clip from the right.  This is
    # a degenerate case (phase name itself > max_chars) — better to
    # truncate the phase than crash the layout.
    tail_budget = max_chars - len(prefix)
    if tail_budget < 1:
        return full[:max_chars]
    return prefix + filename[-tail_budget:]


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

        # Single-column "show=tree" — Tk forces the tree column (#0,
        # bearing the carets and indentation) to render first. Carrying
        # both phase and message in #0 (separated by spaces) keeps the
        # caret next to the message it expands, which is what the
        # mockup specifies. A real two-column layout would put the
        # caret on the wrong side because Tk does not let you display
        # the tree column anywhere except left.
        self.log_tree = ttk.Treeview(
            log_frame,
            show="tree",
            height=15,
            selectmode="browse",
        )
        scrollbar = ttk.Scrollbar(
            log_frame, orient="vertical", command=self.log_tree.yview
        )
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

        # Lazy-load state for the Skipped subtree. The full payload
        # (which can run into hundreds of thousands of paths on a
        # pathological workload) lives in this dict; widgets are only
        # created when the user expands a sub-node. Keys are tree item
        # IDs, values describe what to materialize on demand.
        self._lazy_subtrees: dict[str, dict] = {}
        self.log_tree.bind("<<TreeviewOpen>>", self._on_tree_open)

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
        type_display = "full (auto-promoted)" if forced_full else backup_type or _configured_type
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
                self.status_label.config(
                    text=_truncate_status_text(phase, filename)
                )

    def _on_phase(self, phase="", **kw):
        """Schedule phase label update on the main thread."""
        self.after(0, self._update_phase, phase)

    def _update_phase(self, phase):
        with contextlib.suppress(tk.TclError):
            self.status_label.config(text=phase, foreground=Colors.ACCENT)

    def _on_log(self, message="", level="info", phase="", details=None, **kw):
        """Schedule log append on the main thread."""
        self.after(0, self._append_log, message, level, phase, details)

    def _append_log(self, message, level="info", phase="", details=None):
        """Insert a log entry into the Treeview.

        Three rendering shapes:

        1. **Plain event** (``details is None``): a single row whose
           visible text is ``"{phase}  {message}"``. Most events take
           this path.
        2. **Exclude-pattern listing** (``details = {"patterns": [...]}``):
           parent row with the same prefix, children are the individual
           patterns. Children are inserted eagerly (cheap — at most a
           few dozen patterns).
        3. **Skipped summary** (``details`` has ``permission_denied`` /
           ``os_errors`` / ``excluded_by_pattern`` keys): parent row
           plus a category placeholder per non-empty category. Each
           category is a stub at first — its children (extensions, then
           paths) are materialized lazily when the user expands it,
           via ``_on_tree_open``. Avoids inserting ~100 k widgets up
           front on pathological workloads.
        """
        with contextlib.suppress(tk.TclError):
            tags = self._tags_for(level, message)
            row_text = self._compose_row_text(phase, message)

            if details is None:
                self.log_tree.insert("", "end", text=row_text, tags=tags)
            elif "patterns" in details:
                parent = self.log_tree.insert(
                    "", "end", text=row_text, tags=tags, open=False
                )
                for pat in details["patterns"]:
                    self.log_tree.insert(parent, "end", text=pat, tags=("muted",))
            elif self._is_skipped_payload(details):
                self._insert_skipped_node(row_text, tags, details)
            else:
                # Unknown payload shape — render as plain row to be safe.
                self.log_tree.insert("", "end", text=row_text, tags=tags)

            self._scroll_to_end()

    @staticmethod
    def _compose_row_text(phase: str, message: str) -> str:
        """Build the visible text of a top-level log row.

        ``phase`` is preserved as a left-padded prefix so the user can
        still scan which pipeline phase emitted the line — the
        Schedule journal does the equivalent with its ``Profile``
        column. Empty phase falls back to message-only.
        """
        if not phase:
            return message
        return f"[{phase}]  {message}"

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
        return any(
            k in details
            for k in ("permission_denied", "os_errors", "excluded_by_pattern")
        )

    def _insert_skipped_node(
        self, row_text: str, tags: tuple[str, ...], details: dict
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

        # Parent node for the whole Skipped summary.
        parent = self.log_tree.insert("", "end", text=row_text, tags=tags, open=False)

        # Insert one stub per non-empty category. The Treeview needs at
        # least one child to render a caret; we add a transient
        # placeholder that ``_on_tree_open`` replaces with real content
        # the first time the category is expanded.
        for category in CATEGORY_ORDER:
            entries = buckets[category]
            if not entries:
                continue
            cat_text = f"{category}  ({len(entries)})"
            cat_node = self.log_tree.insert(parent, "end", text=cat_text)
            placeholder = self.log_tree.insert(cat_node, "end", text="…")
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

    def _materialize_category(
        self, category_node: str, entries: list[tuple[str, str]]
    ) -> None:
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
            ext_node = self.log_tree.insert(category_node, "end", text=ext_text)
            # Path-level rows are leaves — no further lazy load. We
            # sort alphabetically for stable navigation.
            for path, reason in sorted(items):
                # Use multiple spaces so the reason aligns roughly
                # right of the path on common widths. A monospace font
                # would do better but we are intentionally on the
                # standard proportional font for visual consistency
                # with Schedule journal.
                leaf_text = f"{path}    {reason}"
                self.log_tree.insert(
                    ext_node, "end", text=leaf_text, tags=("muted",)
                )

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
        with contextlib.suppress(tk.TclError):
            self.log_tree.delete(*self.log_tree.get_children(""))
        # Drop the lazy-load registry so reopened items don't try to
        # materialize children that no longer exist.
        self._lazy_subtrees.clear()
        self.progress_bar["value"] = 0
        self.percent_label.config(text="0%")
        self._phase_totals.clear()
        self._phase_done.clear()
        self._phase_order.clear()
        self._phase_weights.clear()
        self._last_pct = 0
        self.status_label.config(text="Waiting...", foreground=Colors.TEXT_SECONDARY)
