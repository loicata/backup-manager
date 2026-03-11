"""
Recovery Tab
============
Restore a backup: select from available backups or browse for a file,
decrypt if needed, extract ZIP or copy directory, with full restore log.
"""

import os
import shutil
import secrets
import tempfile
import threading
import zipfile
from pathlib import Path
from datetime import datetime

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from src.storage import get_storage_backend
from src.security.encryption import get_crypto_engine, ENCRYPTED_EXTENSION
from src.security.verification import MANIFEST_EXTENSION

COLORS = {
    "bg":        "#f5f6fa",
    "sidebar":   "#2c3e50",
    "sidebar_fg": "#ecf0f1",
    "accent":    "#3498db",
    "success":   "#27ae60",
    "warning":   "#f39c12",
    "danger":    "#e74c3c",
    "text":      "#2c3e50",
    "muted":     "#95a5a6",
}


class RecoveryTab:
    """Recovery tab: restore a backup from available list or file browse."""

    def __init__(self, app, parent_frame):
        self.app = app
        self.parent = parent_frame
        self._build()

    # ── Build ──────────────────────────────────
    def _build(self):
        container = ttk.Frame(self.parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text="Restore a backup",
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))
        ttk.Label(container,
                  text="Restore files from a previous Backup Manager backup. "
                       "Select from available backups or browse for a file, "
                       "preview contents, and choose which files to restore.",
                  style="SubHeader.TLabel", wraplength=1100).pack(anchor="w", pady=(0, 15))

        # ── Top pane: Backup source selection ──
        source_frame = ttk.LabelFrame(container, text="Backup source", padding=10)
        source_frame.pack(fill=tk.X, pady=(0, 10))

        # Two methods: from available backups OR browse
        method_row = ttk.Frame(source_frame)
        method_row.pack(fill=tk.X, pady=(0, 5))

        self.var_restore_method = tk.StringVar(value="available")
        ttk.Radiobutton(method_row, text="Select from available backups",
                         variable=self.var_restore_method, value="available",
                         command=self._toggle_restore_method).pack(anchor="w")
        ttk.Radiobutton(method_row, text="Browse for a file",
                         variable=self.var_restore_method, value="browse",
                         command=self._toggle_restore_method).pack(anchor="w")

        # Sub-frame: Available backups list
        self._restore_available_frame = ttk.Frame(source_frame)
        self._restore_available_frame.pack(fill=tk.X, pady=(5, 0))

        list_row = ttk.Frame(self._restore_available_frame)
        list_row.pack(fill=tk.X)

        backup_columns = ("date", "type", "size", "file")
        self.restore_backup_tree = ttk.Treeview(
            list_row, columns=backup_columns, show="headings", height=5,
            selectmode=tk.BROWSE)
        self.restore_backup_tree.heading("date", text="Date")
        self.restore_backup_tree.heading("type", text="Type")
        self.restore_backup_tree.heading("size", text="Size")
        self.restore_backup_tree.heading("file", text="File name")
        self.restore_backup_tree.column("date", width=160)
        self.restore_backup_tree.column("type", width=90)
        self.restore_backup_tree.column("size", width=100)
        self.restore_backup_tree.column("file", width=400)
        self.restore_backup_tree.pack(side=tk.LEFT, fill=tk.X, expand=True)

        scroll_bk = ttk.Scrollbar(list_row, command=self.restore_backup_tree.yview)
        scroll_bk.pack(side=tk.RIGHT, fill=tk.Y)
        self.restore_backup_tree.configure(yscrollcommand=scroll_bk.set)

        bk_btn_row = ttk.Frame(self._restore_available_frame)
        bk_btn_row.pack(fill=tk.X, pady=(5, 0))
        ttk.Button(bk_btn_row, text="\U0001f504 Refresh list",
                    command=self._refresh_restore_backups).pack(side=tk.LEFT, padx=(0, 5))

        self.restore_backup_tree.bind("<<TreeviewSelect>>", self._on_restore_backup_select)

        # Sub-frame: Browse file (hidden by default)
        self._restore_browse_frame = ttk.Frame(source_frame)

        file_row = ttk.Frame(self._restore_browse_frame)
        file_row.pack(fill=tk.X)
        self.var_restore_file = tk.StringVar()
        ttk.Entry(file_row, textvariable=self.var_restore_file,
                  font=("Consolas", 10)).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(file_row, text="Browse...",
                    command=self._browse_restore_file).pack(side=tk.RIGHT)

        self.lbl_restore_file_info = ttk.Label(self._restore_browse_frame, text="",
                                                font=("Segoe UI", 9))
        self.lbl_restore_file_info.pack(anchor="w", pady=(5, 0))
        self.var_restore_file.trace_add("write", self._update_restore_file_info)

        # ── Bottom pane: Destination + Password + Action ──
        bottom_frame = ttk.Frame(container)
        bottom_frame.pack(fill=tk.X, pady=(0, 5))

        # Destination (full width)
        dest_col = ttk.LabelFrame(bottom_frame, text="Extract to", padding=8)
        dest_col.pack(fill=tk.X, pady=(0, 5))
        dest_row = ttk.Frame(dest_col)
        dest_row.pack(fill=tk.X)
        self.var_restore_dest = tk.StringVar()
        ttk.Entry(dest_row, textvariable=self.var_restore_dest,
                  font=("Consolas", 10)).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(dest_row, text="Browse...",
                    command=lambda: self.var_restore_dest.set(
                        filedialog.askdirectory(parent=self.app.root) or self.var_restore_dest.get())
                    ).pack(side=tk.RIGHT)

        # Password + Action (same row below destination)
        pwd_action_frame = ttk.Frame(bottom_frame)
        pwd_action_frame.pack(fill=tk.X)

        pwd_col = ttk.LabelFrame(pwd_action_frame, text="Password (if encrypted)", padding=8)
        pwd_col.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        pwd_row = ttk.Frame(pwd_col)
        pwd_row.pack(fill=tk.X)
        self.var_restore_password = tk.StringVar()
        self._restore_pwd_entry = ttk.Entry(
            pwd_row, textvariable=self.var_restore_password,
            show="\u2022", font=("Consolas", 11), width=64)
        self._restore_pwd_entry.pack(side=tk.LEFT, padx=(0, 3))
        self.var_restore_show_pwd = tk.BooleanVar(value=False)
        ttk.Checkbutton(pwd_row, text="Show",
                         variable=self.var_restore_show_pwd,
                         command=self._toggle_restore_pwd).pack(side=tk.RIGHT)

        # Action buttons
        action_col = ttk.Frame(pwd_action_frame)
        action_col.pack(side=tk.RIGHT)
        self.btn_restore = ttk.Button(
            action_col, text="\U0001f504 Start restore",
            command=self._run_restore, style="Accent.TButton")
        self.btn_restore.pack(side=tk.TOP, pady=(5, 3))
        self.lbl_restore_status = ttk.Label(
            action_col, text="", font=("Segoe UI", 9))
        self.lbl_restore_status.pack(side=tk.TOP)

        # ── Restore log ──
        log_frame = ttk.LabelFrame(container, text="Restore log", padding=5)
        log_frame.pack(fill=tk.X, pady=(5, 0))

        self.restore_log = tk.Text(
            log_frame, font=("Consolas", 9), height=15,
            bg="#1e1e1e", fg="#00ff00", insertbackground="#00ff00",
            relief=tk.FLAT, state=tk.DISABLED)
        restore_scroll = ttk.Scrollbar(log_frame, command=self.restore_log.yview)
        self.restore_log.configure(yscrollcommand=restore_scroll.set)
        self.restore_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        restore_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    # ── Helpers ────────────────────────────────

    def _toggle_restore_method(self):
        if self.var_restore_method.get() == "available":
            self._restore_browse_frame.pack_forget()
            self._restore_available_frame.pack(fill=tk.X, pady=(5, 0))
            self._refresh_restore_backups()
        else:
            self._restore_available_frame.pack_forget()
            self._restore_browse_frame.pack(fill=tk.X, pady=(5, 0))

    def _refresh_restore_backups(self):
        """List available backups from the current profile's destination."""
        for item in self.restore_backup_tree.get_children():
            self.restore_backup_tree.delete(item)

        if not self.app.current_profile:
            return

        try:
            backend = get_storage_backend(self.app.current_profile.storage)
            backups = backend.list_backups()
        except Exception as e:
            self._restore_log_append(f"\u26a0 Cannot list backups: {e}")
            return

        # Filter by profile name and sort by date (newest first)
        prefix = self.app.current_profile.name
        profile_backups = [
            b for b in backups
            if b.get("name", "").startswith(prefix)
            and not b.get("name", "").endswith(".wbverify")
        ]
        profile_backups.sort(key=lambda b: b.get("modified", 0), reverse=True)

        for b in profile_backups:
            name = b.get("name", "")
            modified = b.get("modified", 0)
            size = b.get("size", 0)

            # Determine type from name
            if "_full_" in name:
                btype = "Full"
            elif "_diff_" in name:
                btype = "Differential"
            elif "_incr_" in name:
                btype = "Incremental"
            else:
                btype = "?"

            date_str = datetime.fromtimestamp(modified).strftime("%d/%m/%Y %H:%M") if modified else "?"
            size_str = self._format_file_size(size) if size else "?"

            self.restore_backup_tree.insert("", tk.END, values=(
                date_str, btype, size_str, name
            ), tags=(name,))

    def _on_restore_backup_select(self, event=None):
        """When a backup is selected from the list, auto-populate file path."""
        sel = self.restore_backup_tree.selection()
        if not sel:
            return
        values = self.restore_backup_tree.item(sel[0], "values")
        if values and len(values) >= 4:
            filename = values[3]
            # Build full path from storage destination
            if self.app.current_profile:
                dest_base = Path(self.app.current_profile.storage.destination_path)
                full_path = dest_base / filename
                self.var_restore_file.set(str(full_path))

    def _toggle_restore_pwd(self):
        show = "" if self.var_restore_show_pwd.get() else "\u2022"
        self._restore_pwd_entry.configure(show=show)

    def _browse_restore_file(self):
        path = filedialog.askopenfilename(
            parent=self.app.root,
            title="Select backup file to restore",
            filetypes=[
                ("Backup files", "*.zip *.wbenc"),
                ("ZIP archives", "*.zip"),
                ("Encrypted backups", "*.wbenc"),
                ("All files", "*.*"),
            ])
        if path:
            self.var_restore_file.set(path)

    def _update_restore_file_info(self, *args):
        path = self.var_restore_file.get()
        if not path or not Path(path).exists():
            self.lbl_restore_file_info.configure(text="")
            return
        p = Path(path)
        size = p.stat().st_size
        size_str = self._format_file_size(size)
        encrypted = "\U0001f510 Encrypted" if path.endswith(".wbenc") else "\U0001f513 Not encrypted"
        self.lbl_restore_file_info.configure(
            text=f"{encrypted}  |  Size: {size_str}  |  Modified: {self._format_mtime(p)}")

    @staticmethod
    def _format_file_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"

    @staticmethod
    def _format_mtime(p: Path) -> str:
        ts = p.stat().st_mtime
        return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")

    def _restore_log_append(self, text: str):
        self.restore_log.configure(state=tk.NORMAL)
        self.restore_log.insert(tk.END, text + "\n")
        self.restore_log.see(tk.END)
        self.restore_log.configure(state=tk.DISABLED)

    def _run_restore(self):
        """Execute the restore operation with optional file selection (non-blocking)."""
        backup_file = self.var_restore_file.get()
        dest_folder = self.var_restore_dest.get()
        password = self.var_restore_password.get()

        # Validation (stays on main thread for immediate feedback)
        if not backup_file or not Path(backup_file).exists():
            messagebox.showerror("Error", "Please select a valid backup file.")
            return
        if not dest_folder:
            messagebox.showerror("Error", "Please select a destination folder.")
            return

        # Pre-check encryption requirements on main thread
        if backup_file.endswith(".wbenc"):
            if not password:
                messagebox.showerror("Error",
                    "This backup is encrypted. Please enter the password.")
                return
            crypto = get_crypto_engine()
            if not crypto.is_available:
                messagebox.showerror("Error",
                    "Decryption module not available.\n"
                    "Install: pip install cryptography")
                return

        # Clear log
        self.restore_log.configure(state=tk.NORMAL)
        self.restore_log.delete("1.0", tk.END)
        self.restore_log.configure(state=tk.DISABLED)

        self.btn_restore.configure(state=tk.DISABLED)
        self.lbl_restore_status.configure(text="Restoring...", foreground=COLORS["warning"])

        root = self.app.root

        def _log(text):
            root.after(0, lambda: self._restore_log_append(text))

        def _set_status(text, color=None):
            if color:
                root.after(0, lambda: self.lbl_restore_status.configure(
                    text=text, foreground=color))
            else:
                root.after(0, lambda: self.lbl_restore_status.configure(text=text))

        def _restore_thread():
            decrypted_path = None
            try:
                Path(dest_folder).mkdir(parents=True, exist_ok=True)
                _log(f"Destination: {dest_folder}")

                actual_file = backup_file

                # Decrypt if encrypted
                if backup_file.endswith(".wbenc"):
                    crypto = get_crypto_engine()
                    _log("\U0001f510 Decrypting backup...")
                    _set_status("Decrypting...")

                    temp_name = f"_restore_{secrets.token_hex(8)}.zip"
                    decrypted_path = Path(dest_folder) / temp_name
                    try:
                        crypto.decrypt_file(
                            source=Path(backup_file),
                            dest=decrypted_path,
                            password=password,
                        )
                        _log("\u2705 Decryption successful")
                        actual_file = str(decrypted_path)
                    except Exception as e:
                        _log(f"\u274c Decryption failed: {e}")
                        _set_status("Failed", COLORS["danger"])
                        root.after(0, lambda: messagebox.showerror("Decryption failed",
                            f"Wrong password or corrupted file:\n{e}"))
                        return

                # Extract ZIP
                if actual_file.endswith(".zip"):
                    _log(f"\U0001f4e6 Extracting ZIP archive...")
                    _set_status("Extracting...")

                    with zipfile.ZipFile(actual_file, 'r') as zf:
                        # ZIP bomb protection
                        MAX_EXTRACT_SIZE = 50 * 1024 * 1024 * 1024  # 50 GB
                        total_size = sum(info.file_size for info in zf.infolist())
                        if total_size > MAX_EXTRACT_SIZE:
                            _log(
                                f"\u274c BLOCKED: Uncompressed size ({total_size / (1024**3):.1f} GB) "
                                f"exceeds 50 GB safety limit.")
                            root.after(0, lambda: messagebox.showerror("Error",
                                f"Archive uncompressed size exceeds safety limit.\n"
                                f"This may be a corrupted or malicious file."))
                            return

                        file_list = zf.namelist()
                        total = len(file_list)
                        _log(f"   {total} file(s) to extract "
                             f"({total_size / (1024**2):.1f} MB)")

                        extracted = 0
                        for i, name in enumerate(file_list):
                            # ZIP Slip protection
                            target = os.path.normpath(os.path.join(dest_folder, name))
                            if not target.startswith(os.path.normpath(dest_folder)):
                                _log(f"\u26a0 Blocked path traversal: {name}")
                                continue
                            zf.extract(name, dest_folder)
                            extracted += 1
                            if (i + 1) % 50 == 0 or i == total - 1:
                                _log(f"   Extracted {i+1}/{total}")

                    _log(f"\u2705 {extracted} file(s) extracted")

                elif Path(actual_file).is_dir():
                    # Flat backup directory — selective copy
                    src_dir = Path(actual_file)
                    all_files = sorted(f for f in src_dir.rglob("*") if f.is_file())

                    total = len(all_files)
                    _log(f"\U0001f4c2 Copying {total} file(s)...")

                    for i, f in enumerate(all_files):
                        rel = f.relative_to(src_dir)
                        target = Path(dest_folder) / rel
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(f, target)
                        if (i + 1) % 50 == 0 or i == total - 1:
                            _log(f"   Copied {i+1}/{total}")

                    _log(f"\u2705 {total} file(s) copied")
                else:
                    # Single file — just copy
                    _log(f"\U0001f4c4 Copying file to {dest_folder}")
                    shutil.copy2(actual_file, dest_folder)
                    _log("\u2705 File copied")

                _log(f"\n{'='*50}")
                _log(f"  RESTORE COMPLETE")
                _log(f"  Destination: {dest_folder}")
                _log(f"{'='*50}")

                _set_status("\u2705 Restore complete!", COLORS["success"])
                root.after(0, lambda: messagebox.showinfo("Restore complete!",
                    f"Backup has been restored to:\n{dest_folder}"))

            except zipfile.BadZipFile as e:
                _log(f"\n\u274c Corrupted ZIP archive: {e}")
                _set_status("\u274c Corrupted archive", COLORS["danger"])
                root.after(0, lambda: messagebox.showerror("Restore failed",
                    f"Corrupted ZIP archive:\n{e}"))

            except Exception as e:
                _log(f"\n\u274c ERROR: {e}")
                _set_status("\u274c Failed", COLORS["danger"])
                root.after(0, lambda: messagebox.showerror("Restore failed",
                    f"An error occurred:\n{e}"))

            finally:
                # Always clean up temp decrypted file
                if decrypted_path and decrypted_path.exists():
                    try:
                        decrypted_path.unlink()
                    except OSError:
                        pass
                root.after(0, lambda: self.btn_restore.configure(state=tk.NORMAL))

        threading.Thread(target=_restore_thread, daemon=True).start()

    # ── Profile load / collect ─────────────────

    def load_profile(self, profile):
        """Refresh available backups when profile is loaded."""
        self._refresh_restore_backups()

    def collect_config(self, profile):
        """No-op: recovery tab has no config to collect."""
        pass
