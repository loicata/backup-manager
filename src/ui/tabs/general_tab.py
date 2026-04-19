"""General tab: profile name, backup type, source paths, exclusions,
bandwidth usage, auto-start, and retry on failure."""

import contextlib
import logging
import os
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk

from src.core.config import BackupProfile, BackupType, ScheduleFrequency
from src.core.scheduler import AutoStart
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)


class GeneralTab(ScrollableTab):
    """Profile name, backup type, sources, exclusion patterns,
    bandwidth usage, auto-start, and retry settings."""

    _DAY_NAMES = (
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    )

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._size_cancel = threading.Event()
        self._retention_tab = None
        self._schedule_tab = None
        self._current_profile: BackupProfile | None = None
        self._last_backup_type = BackupType.FULL.value
        self._loading = False
        # Autoconfig snapshot state — set when _apply_autoconfig_if_needed()
        # performs a change, cleared by _hide_autoconfig_block(). The expected
        # values are what we WROTE; any subsequent user write to a different
        # value hides the block and removes the traces.
        self._autoconfig_expected_schedule: str | None = None
        self._autoconfig_expected_retention_ui: int | None = None
        self._autoconfig_traces: list[tuple[tk.Variable, str]] = []
        self._build_ui()

    def _build_ui(self):
        # Profile identity (name + immutable type badge)
        name_frame = ttk.LabelFrame(self.inner, text="Profile", padding=Spacing.PAD)
        name_frame.pack(fill="x", padx=Spacing.LARGE, pady=(Spacing.LARGE, Spacing.MEDIUM))

        ttk.Label(name_frame, text="Profile name:").pack(anchor="w")
        self.name_var = tk.StringVar(value="New profile")
        ttk.Entry(name_frame, textvariable=self.name_var, width=40).pack(
            fill="x", pady=(Spacing.SMALL, 0)
        )

        type_row = ttk.Frame(name_frame)
        type_row.pack(fill="x", pady=(Spacing.MEDIUM, 0))
        ttk.Label(type_row, text="Profile type:").pack(side="left")
        self.profile_type_var = tk.StringVar(value="")
        ttk.Label(
            type_row,
            textvariable=self.profile_type_var,
        ).pack(side="left", padx=(Spacing.MEDIUM, 0))

        # Backup type
        self._type_frame = ttk.LabelFrame(self.inner, text="Backup type", padding=Spacing.PAD)
        self._type_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)
        type_frame = self._type_frame

        # Interactive radios for Classic profiles
        self._type_radio_frame = ttk.Frame(type_frame)
        self._type_radio_frame.pack(fill="x")
        self.type_var = tk.StringVar(value=BackupType.FULL.value)
        for bt in BackupType:
            label = bt.value.capitalize()
            ttk.Radiobutton(
                self._type_radio_frame,
                text=label,
                value=bt.value,
                variable=self.type_var,
            ).pack(anchor="w", pady=2)

        # Read-only label for Anti-Ransomware (type is locked to Differential)
        self._type_readonly_label = ttk.Label(
            type_frame,
            text="Differential every day",
        )

        # Differential info (shown only for differential)
        self._diff_info_frame = ttk.Frame(type_frame)
        self._diff_info_frame.pack(fill="x", pady=(4, 0))

        # Editable full-backup schedule selector (Classic profiles)
        self._full_sched_frame = ttk.Frame(self._diff_info_frame)
        self._full_sched_frame.pack(fill="x", pady=(Spacing.SMALL, 0))
        ttk.Label(self._full_sched_frame, text="Full backup frequency:").pack(anchor="w")

        mode_row = ttk.Frame(self._full_sched_frame)
        mode_row.pack(anchor="w", pady=(2, 0))
        self.full_sched_mode_var = tk.StringVar(value="monthly")
        for mode_value, mode_label in (
            ("daily", "Daily"),
            ("weekly", "Weekly"),
            ("monthly", "Monthly"),
        ):
            ttk.Radiobutton(
                mode_row,
                text=mode_label,
                value=mode_value,
                variable=self.full_sched_mode_var,
            ).pack(side="left", padx=(0, Spacing.MEDIUM))

        self._full_dow_frame = ttk.Frame(self._full_sched_frame)
        ttk.Label(self._full_dow_frame, text="Day of week:").pack(side="left")
        self.full_day_of_week_var = tk.StringVar(value="Monday")
        ttk.Combobox(
            self._full_dow_frame,
            textvariable=self.full_day_of_week_var,
            values=self._DAY_NAMES,
            state="readonly",
            width=12,
        ).pack(side="left", padx=(Spacing.SMALL, 0))

        self._full_dom_frame = ttk.Frame(self._full_sched_frame)
        ttk.Label(self._full_dom_frame, text="Day of month:").pack(side="left")
        self.full_day_of_month_var = tk.IntVar(value=1)
        ttk.Spinbox(
            self._full_dom_frame,
            from_=1,
            to=31,
            width=4,
            textvariable=self.full_day_of_month_var,
        ).pack(side="left", padx=(Spacing.SMALL, 0))

        # Read-only label for Anti-Ransomware (mode is fixed)
        self._full_sched_readonly = ttk.Label(
            self._diff_info_frame,
            text="Full backup: Automatic on the 1st of each month",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        )

        self.full_sched_mode_var.trace_add("write", lambda *a: self._toggle_full_sched_selectors())
        self._toggle_full_sched_selectors()

        # Info label: shows current daily retention while DIFF is selected.
        # Updated reactively when retention_tab is wired via set_retention_tab().
        self._retention_info_label = ttk.Label(
            self._diff_info_frame,
            text="",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        )

        self.type_var.trace_add("write", lambda *a: self._toggle_diff_info())
        self._toggle_diff_info()

        # Source paths
        self._src_frame = ttk.LabelFrame(self.inner, text="Source paths", padding=Spacing.PAD)
        self._src_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.MEDIUM)
        src_frame = self._src_frame

        # Treeview for sources
        tree_frame = ttk.Frame(src_frame)
        tree_frame.pack(fill="both", expand=True)

        self.sources_tree = ttk.Treeview(
            tree_frame,
            columns=("path", "type"),
            show="headings",
            height=7,
        )
        self.sources_tree.heading("path", text="Path")
        self.sources_tree.heading("type", text="Type")
        self.sources_tree.column("path", width=500)
        self.sources_tree.column("type", width=80)

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.sources_tree.yview)
        self.sources_tree.configure(yscrollcommand=scrollbar.set)
        self.sources_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Source buttons — order: Add, Remove, Move Up, Move Down
        btn_frame = ttk.Frame(src_frame)
        btn_frame.pack(fill="x", pady=(Spacing.MEDIUM, 0))

        ttk.Button(btn_frame, text="Add", command=self._add).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Remove", command=self._remove_selected).pack(
            side="left", padx=2
        )
        ttk.Button(btn_frame, text="Move Up", command=lambda: self._move(-1)).pack(
            side="left", padx=2
        )
        ttk.Button(btn_frame, text="Move Down", command=lambda: self._move(1)).pack(
            side="left", padx=2
        )

        self.total_size_var = tk.StringVar(value="Total: --")
        ttk.Label(
            btn_frame,
            textvariable=self.total_size_var,
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(side="right", padx=(Spacing.MEDIUM, 0))

        # Exclusion patterns
        excl_frame = ttk.LabelFrame(self.inner, text="Exclusion patterns", padding=Spacing.PAD)
        excl_frame.pack(fill="x", padx=Spacing.LARGE, pady=(Spacing.MEDIUM, Spacing.MEDIUM))

        self.exclude_var = tk.StringVar(
            value="*.tmp, *.log, ~$*, Thumbs.db, desktop.ini, __pycache__, .git, node_modules"
        )
        ttk.Entry(excl_frame, textvariable=self.exclude_var).pack(fill="x")
        ttk.Label(
            excl_frame,
            text="Comma-separated glob patterns",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w")

        # Bandwidth usage
        bw_frame = ttk.LabelFrame(self.inner, text="Bandwidth usage", padding=Spacing.PAD)
        bw_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.MEDIUM))

        self.bw_percent_var = tk.IntVar(value=75)
        radio_row = ttk.Frame(bw_frame)
        radio_row.pack(anchor="w")
        for pct in (25, 50, 75, 100):
            ttk.Radiobutton(
                radio_row, text=f"{pct}%", value=pct, variable=self.bw_percent_var
            ).pack(side="left", padx=(0, Spacing.MEDIUM))

        ttk.Label(
            bw_frame,
            text="Percentage of available bandwidth for network destinations. "
            "Local drives always use 100%.",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w", pady=(Spacing.SMALL, 0))

        # Auto-start
        start_frame = ttk.LabelFrame(self.inner, text="Auto-start", padding=Spacing.PAD)
        start_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.MEDIUM))

        self.autostart_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            start_frame,
            text="Start Backup Manager at Windows login",
            variable=self.autostart_var,
        ).pack(anchor="w")

        self.minimized_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            start_frame,
            text="Start minimized to tray",
            variable=self.minimized_var,
        ).pack(anchor="w")

        # Retry on failure
        retry_frame = ttk.LabelFrame(
            self.inner,
            text="Retry on failure",
            padding=Spacing.PAD,
        )
        retry_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.retry_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            retry_frame,
            text="Enable retry",
            variable=self.retry_var,
        ).pack(anchor="w")

        ttk.Label(
            retry_frame,
            text="Retry delays: 2, 10, 30, 90, 240 minutes",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w", pady=(Spacing.SMALL, 0))

    def _add(self):
        """Open a directory chooser to add a folder or files."""
        path = filedialog.askdirectory(title="Select folder")
        if path:
            p = Path(path)
            ptype = "Folder" if p.is_dir() else "File"
            self._add_path(path, ptype)

    def _add_path(self, path: str, path_type: str):
        # Avoid duplicates
        existing = [
            self.sources_tree.item(iid)["values"][0] for iid in self.sources_tree.get_children()
        ]
        if path not in existing:
            self.sources_tree.insert("", "end", values=(path, path_type))
            self._update_total_size()

    def _remove_selected(self):
        for item in self.sources_tree.selection():
            self.sources_tree.delete(item)
        self._update_total_size()

    def _move(self, direction: int):
        sel = self.sources_tree.selection()
        if not sel:
            return
        item = sel[0]
        idx = self.sources_tree.index(item)
        new_idx = max(0, idx + direction)
        self.sources_tree.move(item, "", new_idx)

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format byte count into human-readable string.

        Args:
            size_bytes: Size in bytes.

        Returns:
            Formatted string like '1.23 GB', '456 MB', etc.
        """
        if size_bytes < 0:
            return "0 B"
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(size_bytes) < 1024.0:
                if unit == "B":
                    return f"{size_bytes} {unit}"
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

    @staticmethod
    def _calculate_dir_size(path: Path, cancel: threading.Event) -> int:
        """Recursively calculate directory size in bytes.

        Args:
            path: Directory or file path.
            cancel: Event to signal cancellation.

        Returns:
            Total size in bytes, or 0 if path is inaccessible.
        """
        if cancel.is_set():
            return 0
        if not path.exists():
            return 0
        if path.is_file():
            try:
                return path.stat().st_size
            except OSError:
                return 0
        total = 0
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    if cancel.is_set():
                        return 0
                    try:
                        if entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                        elif entry.is_dir(follow_symlinks=False):
                            total += GeneralTab._calculate_dir_size(Path(entry.path), cancel)
                    except OSError:
                        continue
        except OSError as exc:
            logger.debug("Cannot scan %s: %s", path, exc)
        return total

    def _update_total_size(self):
        """Recalculate total size of all source paths asynchronously."""
        # Cancel any running calculation
        self._size_cancel.set()
        self._size_cancel = threading.Event()
        cancel = self._size_cancel

        paths = []
        for iid in self.sources_tree.get_children():
            paths.append(self.sources_tree.item(iid)["values"][0])

        if not paths:
            self.total_size_var.set("Total: --")
            return

        self.total_size_var.set("Total: Calculating...")

        def _worker():
            total = 0
            for p in paths:
                if cancel.is_set():
                    return
                total += self._calculate_dir_size(Path(str(p)), cancel)
            if cancel.is_set():
                return
            formatted = self._format_size(total)
            with contextlib.suppress(RuntimeError):  # Widget destroyed
                self.after(0, lambda: self.total_size_var.set(f"Total: {formatted}"))

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()

    def _toggle_diff_info(self) -> None:
        """Show/hide the differential info frame and drive the autoconfig.

        On the VERY FIRST Full->Differential transition for the current
        profile (gated by profile.differential_auto_configured), run the
        one-shot autoconfig: set schedule to Daily if it isn't already, and
        bump daily retention up to the full-backup cycle if it is below.
        Persist the gate flag on the profile so subsequent transitions are
        a no-op. On every Diff->Full transition, hide the snapshot block
        and clear the traces but NEVER touch retention or schedule.
        """
        current = self.type_var.get()
        is_diff = current == BackupType.DIFFERENTIAL.value
        if is_diff:
            self._diff_info_frame.pack(fill="x", pady=(4, 0))
        else:
            self._diff_info_frame.pack_forget()

        if not self._loading:
            was_full = self._last_backup_type == BackupType.FULL.value
            if was_full and is_diff:
                self._apply_autoconfig_if_needed()
            elif not is_diff:
                # Any departure from Diff clears the snapshot block.
                self._hide_autoconfig_block()

        self._last_backup_type = current

    def _apply_autoconfig_if_needed(self) -> None:
        """One-shot autoconfig for the first Full->Differential transition.

        Switches the schedule to DAILY if it is something else, so that a
        profile originally scheduled WEEKLY/MONTHLY does not silently
        skip differential runs. Runs at most once per profile
        (persistent across sessions via ``differential_auto_configured``).
        """
        if self._current_profile is None:
            return
        if self._current_profile.differential_auto_configured:
            return
        if self._schedule_tab is None:
            return

        try:
            freq_var = self._schedule_tab.get_frequency_var()
            current_freq = freq_var.get()
        except (tk.TclError, ValueError, AttributeError):
            return

        changed_schedule = current_freq.lower() != ScheduleFrequency.DAILY.value

        # Flip the gate regardless of whether anything concrete changed.
        self._current_profile.differential_auto_configured = True

        if not changed_schedule:
            return

        target_freq = ScheduleFrequency.DAILY.value.capitalize()
        freq_var.set(target_freq)
        self._autoconfig_expected_schedule = target_freq
        tid = freq_var.trace_add("write", lambda *_: self._on_autoconfig_var_changed("schedule"))
        self._autoconfig_traces.append((freq_var, tid))

        self._retention_info_label.config(
            text="Auto configuration for first differential (you can change):"
            "\n    \u2022 Schedule: daily"
        )
        self._retention_info_label.pack(anchor="w", pady=(4, 0))

    def _on_autoconfig_var_changed(self, which: str) -> None:
        """Hide the snapshot block when the user writes a different value."""
        if which != "schedule":
            return
        if self._schedule_tab is None or self._autoconfig_expected_schedule is None:
            return
        try:
            current = self._schedule_tab.get_frequency_var().get()
        except (tk.TclError, ValueError, AttributeError):
            return
        if current != self._autoconfig_expected_schedule:
            self._hide_autoconfig_block()

    def _hide_autoconfig_block(self) -> None:
        """Hide the snapshot label and detach any registered traces."""
        self._retention_info_label.pack_forget()
        for var, tid in self._autoconfig_traces:
            with contextlib.suppress(tk.TclError):
                var.trace_remove("write", tid)
        self._autoconfig_traces.clear()
        self._autoconfig_expected_schedule = None
        self._autoconfig_expected_retention_ui = None

    def set_retention_tab(self, tab) -> None:
        """Wire the Retention tab so the autoconfig can read/write daily retention.

        Called once from the app after all tabs are constructed.
        """
        self._retention_tab = tab

    def set_schedule_tab(self, tab) -> None:
        """Wire the Schedule tab so the autoconfig can read/write frequency.

        Called once from the app after all tabs are constructed.
        """
        self._schedule_tab = tab

    def load_profile(self, profile: BackupProfile):
        """Load profile data into UI widgets."""
        # Clear any snapshot block left from a previously loaded profile —
        # the block is tied to a specific Full->Diff transition, not to the
        # tab at large.
        self._hide_autoconfig_block()
        self._current_profile = profile
        self._loading = True
        try:
            self.name_var.set(profile.name)
            self.type_var.set(profile.backup_type.value)

            # Swap the interactive and read-only widgets based on mode.
            # Always pack_forget + pack unconditionally: winfo_ismapped()
            # returns False during the very first load_profile (the Tk
            # root has not rendered yet), which caused the "Backup type"
            # LabelFrame to appear empty on Anti-Ransomware profiles at
            # startup. pack_forget on an unpacked widget is a no-op, so
            # the unconditional version is safe.
            self._type_radio_frame.pack_forget()
            self._type_readonly_label.pack_forget()
            self._full_sched_frame.pack_forget()
            self._full_sched_readonly.pack_forget()

            if profile.object_lock_enabled:
                self.profile_type_var.set("\U0001f512 Anti-Ransomware")
                self._type_readonly_label.pack(anchor="w", pady=2)
                self._full_sched_readonly.pack(anchor="w", pady=(Spacing.SMALL, 0))
            else:
                self.profile_type_var.set("\U0001f4e6 Classic")
                self._type_radio_frame.pack(fill="x")
                self._full_sched_frame.pack(fill="x", pady=(Spacing.SMALL, 0))

            self.full_sched_mode_var.set(profile.full_schedule_mode)
            self.full_day_of_week_var.set(self._int_to_day_name(profile.full_day_of_week))
            self.full_day_of_month_var.set(profile.full_day_of_month)

            # Clear and reload sources
            for item in self.sources_tree.get_children():
                self.sources_tree.delete(item)
            for path in profile.source_paths:
                p = Path(path)
                ptype = "Folder" if p.is_dir() else "File"
                self.sources_tree.insert("", "end", values=(path, ptype))

            self.exclude_var.set(", ".join(profile.exclude_patterns))
            self.bw_percent_var.set(profile.bandwidth_percent)

            # Retry from schedule config
            self.retry_var.set(profile.schedule.retry_enabled)

            # Load auto-start state from system
            self.autostart_var.set(AutoStart.is_enabled())
            self.minimized_var.set(not AutoStart.is_show_window())
        finally:
            self._loading = False

        # Remember the loaded backup type so the very next user-driven
        # write to type_var is correctly detected as a transition.
        self._last_backup_type = self.type_var.get()

        self._update_total_size()

    def collect_config(self) -> dict:
        """Collect configuration from all widgets.

        Returns:
            Dict with profile fields and retry/autostart settings.
        """
        sources = [
            self.sources_tree.item(iid)["values"][0] for iid in self.sources_tree.get_children()
        ]
        excludes = [p.strip() for p in self.exclude_var.get().split(",") if p.strip()]

        return {
            "name": self.name_var.get().strip() or "Unnamed",
            "backup_type": BackupType(self.type_var.get()),
            "source_paths": sources,
            "exclude_patterns": excludes,
            "bandwidth_percent": self.bw_percent_var.get(),
            "autostart": self.autostart_var.get(),
            "autostart_minimized": self.minimized_var.get(),
            "retry_enabled": self.retry_var.get(),
            "full_schedule_mode": self.full_sched_mode_var.get(),
            "full_day_of_week": self._day_name_to_int(self.full_day_of_week_var.get()),
            "full_day_of_month": self.full_day_of_month_var.get(),
        }

    def _toggle_full_sched_selectors(self) -> None:
        """Show the day-of-week or day-of-month selector based on the
        selected full_schedule_mode. Daily mode hides both."""
        mode = self.full_sched_mode_var.get()
        if mode == "weekly":
            self._full_dom_frame.pack_forget()
            if not self._full_dow_frame.winfo_ismapped():
                self._full_dow_frame.pack(anchor="w", pady=(2, 0))
        elif mode == "monthly":
            self._full_dow_frame.pack_forget()
            if not self._full_dom_frame.winfo_ismapped():
                self._full_dom_frame.pack(anchor="w", pady=(2, 0))
        else:
            self._full_dow_frame.pack_forget()
            self._full_dom_frame.pack_forget()

    @classmethod
    def _int_to_day_name(cls, value: int) -> str:
        """Convert a 0-6 weekday index to its English name."""
        if 0 <= value < len(cls._DAY_NAMES):
            return cls._DAY_NAMES[value]
        return cls._DAY_NAMES[0]

    @classmethod
    def _day_name_to_int(cls, name: str) -> int:
        """Convert an English weekday name to a 0-6 index."""
        try:
            return cls._DAY_NAMES.index(name)
        except ValueError:
            return 0
