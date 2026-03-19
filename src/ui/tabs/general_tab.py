"""General tab: profile name, backup type, source paths, exclusions,
bandwidth limit, auto-start, and retry on failure."""

import tkinter as tk
from tkinter import ttk, filedialog
from pathlib import Path

from src.core.config import BackupProfile, BackupType
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing


class GeneralTab(ScrollableTab):
    """Profile name, backup type, sources, exclusion patterns,
    bandwidth limit, auto-start, and retry settings."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
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
        beta_types = {BackupType.INCREMENTAL, BackupType.DIFFERENTIAL}
        for bt in BackupType:
            label = bt.value.capitalize()
            if bt in beta_types:
                label += " (beta)"
            ttk.Radiobutton(
                type_frame, text=label, value=bt.value, variable=self.type_var
            ).pack(anchor="w", pady=2)

        # Source paths
        src_frame = ttk.LabelFrame(self.inner, text="Source paths", padding=Spacing.PAD)
        src_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        # Treeview for sources
        tree_frame = ttk.Frame(src_frame)
        tree_frame.pack(fill="both", expand=True)

        self.sources_tree = ttk.Treeview(
            tree_frame, columns=("path", "type"), show="headings", height=8,
        )
        self.sources_tree.heading("path", text="Path")
        self.sources_tree.heading("type", text="Type")
        self.sources_tree.column("path", width=500)
        self.sources_tree.column("type", width=80)

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical",
                                   command=self.sources_tree.yview)
        self.sources_tree.configure(yscrollcommand=scrollbar.set)
        self.sources_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Source buttons — order: Add, Remove, Move Up, Move Down
        btn_frame = ttk.Frame(src_frame)
        btn_frame.pack(fill="x", pady=(Spacing.MEDIUM, 0))

        ttk.Button(btn_frame, text="Add",
                    command=self._add).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Remove",
                    command=self._remove_selected).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Move Up",
                    command=lambda: self._move(-1)).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Move Down",
                    command=lambda: self._move(1)).pack(side="left", padx=2)

        # Exclusion patterns
        excl_frame = ttk.LabelFrame(self.inner, text="Exclusion patterns", padding=Spacing.PAD)
        excl_frame.pack(fill="x", padx=Spacing.LARGE, pady=(Spacing.MEDIUM, Spacing.MEDIUM))

        self.exclude_var = tk.StringVar(
            value="*.tmp, *.log, ~$*, Thumbs.db, desktop.ini, __pycache__, .git, node_modules"
        )
        ttk.Entry(excl_frame, textvariable=self.exclude_var).pack(fill="x")
        ttk.Label(excl_frame, text="Comma-separated glob patterns",
                   foreground=Colors.TEXT_SECONDARY,
                   font=Fonts.small()).pack(anchor="w")

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
            start_frame, text="Start Backup Manager at Windows login",
            variable=self.autostart_var,
        ).pack(anchor="w")

        self.minimized_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            start_frame, text="Start minimized to tray",
            variable=self.minimized_var,
        ).pack(anchor="w")

        # Retry on failure
        retry_frame = ttk.LabelFrame(
            self.inner, text="Retry on failure", padding=Spacing.PAD,
        )
        retry_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.retry_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            retry_frame, text="Enable retry",
            variable=self.retry_var,
        ).pack(anchor="w")

        ttk.Label(
            retry_frame, text="Retry delays: 2, 10, 30, 90, 240 minutes",
            foreground=Colors.TEXT_SECONDARY, font=Fonts.small(),
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
        existing = [self.sources_tree.item(iid)["values"][0]
                     for iid in self.sources_tree.get_children()]
        if path not in existing:
            self.sources_tree.insert("", "end", values=(path, path_type))

    def _remove_selected(self):
        for item in self.sources_tree.selection():
            self.sources_tree.delete(item)

    def _move(self, direction: int):
        sel = self.sources_tree.selection()
        if not sel:
            return
        item = sel[0]
        idx = self.sources_tree.index(item)
        new_idx = max(0, idx + direction)
        self.sources_tree.move(item, "", new_idx)

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

        # Retry from schedule config
        self.retry_var.set(profile.schedule.retry_enabled)

    def collect_config(self) -> dict:
        """Collect configuration from all widgets.

        Returns:
            Dict with profile fields and retry/autostart settings.
        """
        sources = [
            self.sources_tree.item(iid)["values"][0]
            for iid in self.sources_tree.get_children()
        ]
        excludes = [
            p.strip() for p in self.exclude_var.get().split(",") if p.strip()
        ]

        return {
            "name": self.name_var.get().strip() or "Unnamed",
            "backup_type": BackupType(self.type_var.get()),
            "source_paths": sources,
            "exclude_patterns": excludes,
            "bandwidth_limit_kbps": self.bw_var.get(),
            "autostart": self.autostart_var.get(),
            "autostart_minimized": self.minimized_var.get(),
            "retry_enabled": self.retry_var.get(),
        }
