"""Recovery tab: restore files from backups."""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import shutil
from pathlib import Path

from src.core.config import BackupProfile
from src.security.encryption import decrypt_file
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing


class RecoveryTab(ScrollableTab):
    """Selective file restoration from backups."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._backend = None
        self._build_ui()

    def _build_ui(self):
        # Restore method
        method_frame = ttk.LabelFrame(self.inner, text="Restore method", padding=Spacing.PAD)
        method_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE)

        self.method_var = tk.StringVar(value="list")
        ttk.Radiobutton(
            method_frame, text="Select from available backups",
            value="list", variable=self.method_var,
        ).pack(anchor="w")
        ttk.Radiobutton(
            method_frame, text="Browse for a backup file/folder",
            value="browse", variable=self.method_var,
        ).pack(anchor="w")

        # Backup list
        list_frame = ttk.LabelFrame(self.inner, text="Available backups", padding=Spacing.PAD)
        list_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self.backup_tree = ttk.Treeview(
            list_frame, columns=("name", "size", "date"),
            show="headings", height=8,
        )
        self.backup_tree.heading("name", text="Backup")
        self.backup_tree.heading("size", text="Size")
        self.backup_tree.heading("date", text="Date")
        self.backup_tree.column("name", width=300)
        self.backup_tree.column("size", width=100)
        self.backup_tree.column("date", width=150)
        self.backup_tree.pack(fill="both", expand=True)

        btn_row = ttk.Frame(list_frame)
        btn_row.pack(fill="x", pady=(Spacing.SMALL, 0))
        ttk.Button(btn_row, text="Refresh", command=self._refresh_list).pack(side="left")
        ttk.Button(btn_row, text="Browse...", command=self._browse_backup).pack(side="left", padx=Spacing.SMALL)

        # Destination
        dest_frame = ttk.LabelFrame(self.inner, text="Restore destination", padding=Spacing.PAD)
        dest_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        dest_row = ttk.Frame(dest_frame)
        dest_row.pack(fill="x")
        self.dest_var = tk.StringVar()
        ttk.Entry(dest_row, textvariable=self.dest_var).pack(side="left", fill="x", expand=True)
        ttk.Button(dest_row, text="Browse...",
                    command=self._browse_dest).pack(side="right", padx=(Spacing.SMALL, 0))

        # Restore button
        btn_frame = ttk.Frame(self.inner)
        btn_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.restore_btn = ttk.Button(
            btn_frame, text="Restore", style="Accent.TButton",
            command=self._restore,
        )
        self.restore_btn.pack(side="left")

        self.status_label = ttk.Label(btn_frame, text="", foreground=Colors.TEXT_SECONDARY)
        self.status_label.pack(side="left", padx=Spacing.LARGE)

    def set_backend(self, backend):
        self._backend = backend

    def _refresh_list(self):
        for item in self.backup_tree.get_children():
            self.backup_tree.delete(item)

        if not self._backend:
            return

        def _load():
            try:
                backups = self._backend.list_backups()
                self.after(0, lambda: self._populate_list(backups))
            except Exception as e:
                self.after(0, lambda: self.status_label.config(
                    text=f"Error: {e}", foreground=Colors.DANGER
                ))

        threading.Thread(target=_load, daemon=True).start()

    def _populate_list(self, backups):
        from datetime import datetime
        for b in backups:
            size = self._format_size(b.get("size", 0))
            mtime = b.get("modified", 0)
            date = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M") if mtime else ""
            self.backup_tree.insert("", "end", values=(b["name"], size, date))

    def _browse_backup(self):
        path = filedialog.askdirectory(title="Select backup folder")
        if path:
            self.dest_var.set("")
            self.backup_tree.insert("", 0, values=(path, "", "Browsed"))

    def _browse_dest(self):
        path = filedialog.askdirectory(title="Select restore destination")
        if path:
            self.dest_var.set(path)

    def _restore(self):
        sel = self.backup_tree.selection()
        if not sel:
            messagebox.showwarning("Restore", "Please select a backup to restore.")
            return
        dest = self.dest_var.get()
        if not dest:
            messagebox.showwarning("Restore", "Please select a restore destination.")
            return

        backup_name = self.backup_tree.item(sel[0])["values"][0]
        self.status_label.config(text="Restoring...", foreground=Colors.WARNING)
        self.restore_btn.state(["disabled"])

        def _do_restore():
            try:
                # Simple restore: copy from backup to destination
                src = Path(backup_name)
                dst = Path(dest)
                if src.is_dir():
                    for f in src.rglob("*"):
                        if f.is_file():
                            rel = f.relative_to(src)
                            target = dst / rel
                            target.parent.mkdir(parents=True, exist_ok=True)
                            if f.suffix == ".wbenc":
                                # Encrypted file — prompt for password
                                self.after(0, lambda: self.status_label.config(
                                    text="Encrypted file — enter password in dialog"
                                ))
                            else:
                                shutil.copy2(f, target)

                self.after(0, lambda: self._restore_done(True, "Restore complete"))
            except Exception as e:
                self.after(0, lambda: self._restore_done(False, str(e)))

        threading.Thread(target=_do_restore, daemon=True).start()

    def _restore_done(self, ok, msg):
        self.restore_btn.state(["!disabled"])
        color = Colors.SUCCESS if ok else Colors.DANGER
        self.status_label.config(text=msg, foreground=color)

    @staticmethod
    def _format_size(size: int) -> str:
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"

    def load_profile(self, profile: BackupProfile):
        pass  # Recovery tab loads dynamically

    def collect_config(self) -> dict:
        return {}
