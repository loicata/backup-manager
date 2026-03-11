"""
General Tab — Profile name, backup type, sources, exclusions.
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path

from src.core.config import BackupType


class GeneralTab:
    """General settings tab: name, type, compression, sources, exclusions."""

    def __init__(self, app, parent_frame):
        self.app = app
        self.root = app.root
        self._build(parent_frame)

    # ──────────────────────────────────────────
    #  Build UI
    # ──────────────────────────────────────────
    def _build(self, parent):
        container = ttk.Frame(parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        # Profile name
        ttk.Label(container, text="Profile name", style="Header.TLabel").grid(
            row=0, column=0, sticky="w", pady=(0, 5))
        self.var_name = tk.StringVar()
        ttk.Entry(container, textvariable=self.var_name, font=("Segoe UI", 11),
                  width=40).grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 15))

        # Backup type
        ttk.Label(container, text="Backup type").grid(
            row=2, column=0, sticky="w", pady=(0, 5))
        self.var_backup_type = tk.StringVar(value=BackupType.FULL.value)
        type_frame = ttk.Frame(container)
        type_frame.grid(row=3, column=0, columnspan=2, sticky="w", pady=(0, 15))
        ttk.Radiobutton(type_frame, text="Full (all files)",
                         variable=self.var_backup_type, value=BackupType.FULL.value
                         ).pack(anchor="w")
        ttk.Radiobutton(type_frame, text="Incremental (modified since last backup)",
                         variable=self.var_backup_type, value=BackupType.INCREMENTAL.value
                         ).pack(anchor="w")
        ttk.Radiobutton(type_frame, text="Differential (modified since last full backup)",
                         variable=self.var_backup_type, value=BackupType.DIFFERENTIAL.value
                         ).pack(anchor="w")

        # Compression
        self.var_compress = tk.BooleanVar(value=False)
        ttk.Checkbutton(container, text="Compress as ZIP",
                         variable=self.var_compress).grid(
            row=4, column=0, sticky="w", pady=(0, 0))
        ttk.Label(container,
                  text="\u26a0 Compression significantly increases backup time.",
                  font=("Segoe UI", 8), foreground="#95a5a6").grid(
            row=5, column=0, sticky="w", pady=(0, 15))

        # Source paths
        ttk.Label(container, text="Source folders and files", style="Header.TLabel").grid(
            row=6, column=0, sticky="w", pady=(10, 5))

        # Source list with enhanced display
        sources_frame = ttk.Frame(container)
        sources_frame.grid(row=7, column=0, columnspan=2, sticky="nsew", pady=(0, 5))

        # Treeview instead of simple Listbox for richer display
        src_columns = ("path", "type", "info")
        self.source_tree = ttk.Treeview(
            sources_frame, columns=src_columns, show="headings", height=6,
            selectmode=tk.EXTENDED,
        )
        self.source_tree.heading("path", text="Path")
        self.source_tree.heading("type", text="Type")
        self.source_tree.heading("info", text="Info")
        self.source_tree.column("path", width=400)
        self.source_tree.column("type", width=80)
        self.source_tree.column("info", width=120)
        self.source_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(sources_frame, command=self.source_tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.source_tree.configure(yscrollcommand=scrollbar.set)

        # Action buttons
        src_btn_frame = ttk.Frame(container)
        src_btn_frame.grid(row=8, column=0, columnspan=2, sticky="w", pady=(5, 10))
        ttk.Button(src_btn_frame, text="\U0001f4c1 Add folder",
                    command=self._add_source_folder).pack(side=tk.LEFT, padx=(0, 3))
        ttk.Button(src_btn_frame, text="\U0001f5c2 Multiple selection...",
                    command=self._open_multi_folder_dialog).pack(side=tk.LEFT, padx=(0, 3))
        ttk.Button(src_btn_frame, text="\U0001f4c4 Add files",
                    command=self._add_source_file).pack(side=tk.LEFT, padx=(0, 3))
        ttk.Button(src_btn_frame, text="\u2b06", width=3,
                    command=self._move_source_up).pack(side=tk.LEFT, padx=(10, 1))
        ttk.Button(src_btn_frame, text="\u2b07", width=3,
                    command=self._move_source_down).pack(side=tk.LEFT, padx=(0, 3))
        ttk.Button(src_btn_frame, text="\u2715 Remove selected",
                    command=self._remove_source).pack(side=tk.LEFT, padx=(10, 3))
        ttk.Button(src_btn_frame, text="\U0001f5d1 Clear all",
                    command=self._clear_sources).pack(side=tk.LEFT)

        # Exclusions
        ttk.Label(container, text="Exclusion patterns (one per line)").grid(
            row=9, column=0, sticky="w", pady=(10, 3))
        self.exclude_text = tk.Text(container, font=("Consolas", 9), height=3,
                                     relief=tk.SOLID, bd=1, wrap=tk.WORD)
        self.exclude_text.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(0, 10))

        # Save button
        ttk.Button(container, text='\U0001f4be Save profile',
                    command=self.app._save_profile, style="Accent.TButton").grid(
            row=11, column=0, columnspan=2, sticky="e", pady=(20, 0))

        container.columnconfigure(0, weight=1)

    # ──────────────────────────────────────────
    #  Source Management
    # ──────────────────────────────────────────
    def _get_user_home(self) -> Path:
        """Get the Windows user home directory."""
        return Path(os.path.expanduser("~"))

    def _format_size(self, size_bytes: int) -> str:
        """Format byte size to human-readable string."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} Po"

    def _get_folder_info(self, path_str: str) -> tuple[str, str]:
        """Get type and info string for a source path."""
        p = Path(path_str)
        if not p.exists():
            return "\u26a0 Not found", "\u2014"
        if p.is_file():
            return "\U0001f4c4 File", self._format_size(p.stat().st_size)
        if p.is_dir():
            try:
                count = sum(1 for _ in p.rglob("*") if _.is_file())
                return "\U0001f4c1 Folder", f"~{count} files"
            except PermissionError:
                return "\U0001f4c1 Folder", "limited access"
        return "\u2753", "\u2014"

    def _insert_source_item(self, path_str: str):
        """Insert a source path into the treeview with metadata."""
        # Avoid duplicates
        for item in self.source_tree.get_children():
            if self.source_tree.item(item, "values")[0] == path_str:
                return  # Already in the list

        type_str, info_str = self._get_folder_info(path_str)
        self.source_tree.insert("", tk.END, values=(path_str, type_str, info_str))

    def _get_all_source_paths(self) -> list[str]:
        """Get all source paths from the treeview."""
        return [self.source_tree.item(item, "values")[0]
                for item in self.source_tree.get_children()]

    def _add_source_folder(self):
        """Add a single folder via native dialog."""
        path = filedialog.askdirectory(title="Add a folder source")
        if path:
            self._insert_source_item(path)

    def _add_source_file(self):
        """Add one or more files via native dialog."""
        paths = filedialog.askopenfilenames(title="Add source files")
        for path in paths:
            self._insert_source_item(path)

    def _remove_source(self):
        """Remove all selected items from the source list."""
        selected = self.source_tree.selection()
        if not selected:
            messagebox.showinfo("Info", "Please select one or more items to remove.")
            return
        for item in selected:
            self.source_tree.delete(item)

    def _clear_sources(self):
        """Clear all source paths."""
        for item in self.source_tree.get_children():
            self.source_tree.delete(item)

    def _move_source_up(self):
        """Move selected source item up in the list."""
        selected = self.source_tree.selection()
        if not selected:
            return
        for item in selected:
            idx = self.source_tree.index(item)
            if idx > 0:
                self.source_tree.move(item, "", idx - 1)

    def _move_source_down(self):
        """Move selected source item down in the list."""
        selected = self.source_tree.selection()
        if not selected:
            return
        for item in reversed(selected):
            idx = self.source_tree.index(item)
            if idx < len(self.source_tree.get_children()) - 1:
                self.source_tree.move(item, "", idx + 1)

    def _open_multi_folder_dialog(self):
        """Open a dialog to browse and select multiple folders at once."""
        dialog = tk.Toplevel(self.app.root)
        dialog.title("\U0001f5c2 Multiple folder selection")
        dialog.geometry("700x550")
        dialog.transient(self.app.root)
        dialog.grab_set()

        # Instructions
        ttk.Label(dialog,
                  text="Check folders to include in backup :",
                  font=("Segoe UI", 10)).pack(padx=15, pady=(10, 5), anchor="w")

        # Path entry for navigation
        nav_frame = ttk.Frame(dialog)
        nav_frame.pack(fill=tk.X, padx=15, pady=(0, 5))
        ttk.Label(nav_frame, text="Emplacement :").pack(side=tk.LEFT)
        nav_var = tk.StringVar(value=str(self._get_user_home()))
        nav_entry = ttk.Entry(nav_frame, textvariable=nav_var, font=("Consolas", 9))
        nav_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(nav_frame, text="Aller",
                    command=lambda: populate_tree(nav_var.get())).pack(side=tk.LEFT)

        # Quick navigation buttons
        quick_nav = ttk.Frame(dialog)
        quick_nav.pack(fill=tk.X, padx=15, pady=(0, 5))
        for label, path_str in [
            ("\U0001f3e0 Home", str(self._get_user_home())),
            ("\U0001f4bb C:\\", "C:\\"),
            ("\U0001f4bb D:\\", "D:\\"),
        ]:
            ttk.Button(quick_nav, text=label, width=10,
                        command=lambda p=path_str: [nav_var.set(p), populate_tree(p)]
                        ).pack(side=tk.LEFT, padx=2)

        # Also detect all available drives
        if sys.platform == "win32":
            import string
            for letter in string.ascii_uppercase[4:]:  # E: onwards
                drive = f"{letter}:\\"
                if Path(drive).exists():
                    ttk.Button(quick_nav, text=f"\U0001f4bb {drive}", width=6,
                                command=lambda p=drive: [nav_var.set(p), populate_tree(p)]
                                ).pack(side=tk.LEFT, padx=2)

        # Treeview with checkboxes (simulated with tags)
        tree_frame = ttk.Frame(dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=5)

        tree = ttk.Treeview(tree_frame, show="tree", selectmode=tk.EXTENDED)
        tree_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=tree_scroll.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        checked_paths: set[str] = set()

        def toggle_check(event):
            item = tree.focus()
            if not item:
                return
            path_str = tree.item(item, "values")[0] if tree.item(item, "values") else ""
            if not path_str:
                return
            if path_str in checked_paths:
                checked_paths.discard(path_str)
                tree.item(item, text="\u2610 " + Path(path_str).name)
            else:
                checked_paths.add(path_str)
                tree.item(item, text="\u2611 " + Path(path_str).name)
            update_count()

        def update_count():
            count_label.configure(text=f"{len(checked_paths)} folder(s) selected")

        def populate_tree(root_path: str):
            tree.delete(*tree.get_children())
            checked_paths.clear()
            update_count()

            root = Path(root_path)
            if not root.exists():
                return

            try:
                items = sorted(root.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
            except PermissionError:
                return

            for item in items:
                if item.name.startswith("."):
                    continue
                if item.is_dir():
                    try:
                        icon = "\u2610 "
                        node = tree.insert("", tk.END, text=icon + item.name,
                                           values=(str(item),), open=False)
                        # Add dummy child so the expand arrow shows
                        tree.insert(node, tk.END, text="...")
                    except (PermissionError, OSError):
                        pass

        def on_expand(event):
            item = tree.focus()
            if not item:
                return
            children = tree.get_children(item)
            # If only dummy child, load real contents
            if len(children) == 1 and tree.item(children[0], "text") == "...":
                tree.delete(children[0])
                path_str = tree.item(item, "values")[0] if tree.item(item, "values") else ""
                if path_str:
                    try:
                        sub = Path(path_str)
                        entries = sorted(sub.iterdir(),
                                         key=lambda p: (not p.is_dir(), p.name.lower()))
                        for entry in entries:
                            if entry.name.startswith("."):
                                continue
                            if entry.is_dir():
                                try:
                                    icon = "\u2611 " if str(entry) in checked_paths else "\u2610 "
                                    node = tree.insert(item, tk.END, text=icon + entry.name,
                                                       values=(str(entry),), open=False)
                                    tree.insert(node, tk.END, text="...")
                                except (PermissionError, OSError):
                                    pass
                    except PermissionError:
                        pass

        tree.bind("<Double-1>", toggle_check)
        tree.bind("<space>", toggle_check)
        tree.bind("<<TreeviewOpen>>", on_expand)

        # Bottom section
        bottom = ttk.Frame(dialog)
        bottom.pack(fill=tk.X, padx=15, pady=10)

        count_label = ttk.Label(bottom, text="0 folder(s) selected",
                                 font=("Segoe UI", 9))
        count_label.pack(side=tk.LEFT)

        def on_confirm():
            for p in checked_paths:
                self._insert_source_item(p)
            dialog.destroy()

        ttk.Button(bottom, text="\u2705 Add selection",
                    command=on_confirm, style="Accent.TButton").pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(bottom, text="Cancel",
                    command=dialog.destroy).pack(side=tk.RIGHT)

        # Initial population
        populate_tree(str(self._get_user_home()))

    # ──────────────────────────────────────────
    #  Profile load / collect
    # ──────────────────────────────────────────
    def load_profile(self, p):
        """Load profile data into the tab's UI widgets."""
        self.var_name.set(p.name)
        self.var_backup_type.set(p.backup_type)
        self.var_compress.set(p.compress)

        # Sources
        self._clear_sources()
        for src in p.source_paths:
            self._insert_source_item(src)

        # Exclusions
        self.exclude_text.delete("1.0", tk.END)
        self.exclude_text.insert("1.0", "\n".join(p.exclude_patterns))

    def collect_config(self, p):
        """Save tab's UI state into profile p."""
        p.name = self.var_name.get().strip() or "Unnamed"
        p.backup_type = self.var_backup_type.get()
        p.compress = self.var_compress.get()

        # Sources — validate paths exist
        p.source_paths = self._get_all_source_paths()
        if p.source_paths:
            missing = [s for s in p.source_paths if not Path(s).exists()]
            if missing:
                msg = (f"{len(missing)} source path(s) not found:\n\n"
                       + "\n".join(f"  \u2022 {m}" for m in missing[:5]))
                if len(missing) > 5:
                    msg += f"\n  ... and {len(missing) - 5} more"
                msg += "\n\nSave anyway? (paths may be on a disconnected drive)"
                if not messagebox.askyesno("Missing paths", msg):
                    return False  # Signal to abort save

        # Exclusions
        raw = self.exclude_text.get("1.0", tk.END).strip()
        p.exclude_patterns = [line.strip() for line in raw.split("\n") if line.strip()]
        return True
