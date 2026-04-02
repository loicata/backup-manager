"""General tab: profile name, backup type, source paths, exclusions,
bandwidth limit, auto-start, and retry on failure."""

import contextlib
import logging
import os
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk

from src.core.config import BackupProfile, BackupType
from src.core.scheduler import AutoStart
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)


class GeneralTab(ScrollableTab):
    """Profile name, backup type, sources, exclusion patterns,
    bandwidth limit, auto-start, and retry settings."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._size_cancel = threading.Event()
        self._build_ui()

    def _build_ui(self):
        # Profile name
        name_frame = ttk.LabelFrame(self.inner, text="Profile", padding=Spacing.PAD)
        name_frame.pack(fill="x", padx=Spacing.LARGE, pady=(Spacing.LARGE, Spacing.MEDIUM))

        ttk.Label(name_frame, text="Profile name:").pack(anchor="w")
        self.name_var = tk.StringVar(value="New profile")
        ttk.Entry(name_frame, textvariable=self.name_var, width=40).pack(
            fill="x", pady=(Spacing.SMALL, 0)
        )

        # Backup type
        type_frame = ttk.LabelFrame(self.inner, text="Backup type", padding=Spacing.PAD)
        type_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self.type_var = tk.StringVar(value=BackupType.FULL.value)
        for bt in BackupType:
            label = bt.value.capitalize()
            ttk.Radiobutton(type_frame, text=label, value=bt.value, variable=self.type_var).pack(
                anchor="w", pady=2
            )

        # Differential info (shown only for differential)
        self._diff_info_frame = ttk.Frame(type_frame)
        self._diff_info_frame.pack(fill="x", pady=(4, 0))

        self._diff_info_label = ttk.Label(
            self._diff_info_frame,
            text="Differential applies to daily backup only. "
            "Weekly and monthly retention always use full backups.",
            foreground=Colors.TEXT_SECONDARY,
            wraplength=400,
            justify="left",
        )
        self._diff_info_label.pack(anchor="w")

        # Full backup cycle
        self._full_every_frame = ttk.Frame(self._diff_info_frame)
        self._full_every_frame.pack(fill="x", pady=(4, 0))
        ttk.Label(self._full_every_frame, text="Full backup every").pack(side="left")
        self.full_every_var = tk.IntVar(value=7)
        full_spin = ttk.Spinbox(
            self._full_every_frame,
            from_=1,
            to=7,
            width=4,
            textvariable=self.full_every_var,
        )
        full_spin.pack(side="left", padx=4)
        ttk.Label(self._full_every_frame, text="backups").pack(side="left")

        self.type_var.trace_add("write", lambda *a: self._toggle_diff_info())
        self._toggle_diff_info()

        # Source paths
        src_frame = ttk.LabelFrame(self.inner, text="Source paths", padding=Spacing.PAD)
        src_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.MEDIUM)

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

        # Bandwidth limit
        bw_frame = ttk.LabelFrame(self.inner, text="Bandwidth limit", padding=Spacing.PAD)
        bw_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.MEDIUM))

        ttk.Label(bw_frame, text="Limit (KB/s, 0 = unlimited):").pack(anchor="w")
        self.bw_var = tk.IntVar(value=0)
        ttk.Entry(bw_frame, textvariable=self.bw_var, width=10).pack(
            anchor="w", pady=(Spacing.SMALL, 0)
        )

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
        """Show/hide the differential info and cycle field."""
        is_diff = self.type_var.get() == BackupType.DIFFERENTIAL.value
        if is_diff:
            self._diff_info_frame.pack(fill="x", pady=(4, 0))
        else:
            self._diff_info_frame.pack_forget()

    def load_profile(self, profile: BackupProfile):
        """Load profile data into UI widgets."""
        self.name_var.set(profile.name)
        self.type_var.set(profile.backup_type.value)

        # Clear and reload sources
        for item in self.sources_tree.get_children():
            self.sources_tree.delete(item)
        for path in profile.source_paths:
            p = Path(path)
            ptype = "Folder" if p.is_dir() else "File"
            self.sources_tree.insert("", "end", values=(path, ptype))

        self.exclude_var.set(", ".join(profile.exclude_patterns))
        self.bw_var.set(profile.bandwidth_limit_kbps)
        self.full_every_var.set(profile.full_backup_every)

        # Retry from schedule config
        self.retry_var.set(profile.schedule.retry_enabled)

        # Load auto-start state from system
        self.autostart_var.set(AutoStart.is_enabled())
        self.minimized_var.set(not AutoStart.is_show_window())

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
            "full_backup_every": self.full_every_var.get(),
            "source_paths": sources,
            "exclude_patterns": excludes,
            "bandwidth_limit_kbps": self.bw_var.get(),
            "autostart": self.autostart_var.get(),
            "autostart_minimized": self.minimized_var.get(),
            "retry_enabled": self.retry_var.get(),
        }
