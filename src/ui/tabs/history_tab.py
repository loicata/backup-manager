"""History tab: browse backup logs."""

import os
import subprocess
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk

from src.ui.theme import Colors, Spacing


class HistoryTab(ttk.Frame):
    """Browse and view backup execution logs."""

    def __init__(self, parent, log_dir: Path = None, **kwargs):
        super().__init__(parent, **kwargs)
        appdata = os.environ.get("APPDATA", "")
        self._log_dir = log_dir or Path(appdata) / "BackupManager" / "logs"
        # iid -> Path of the log file, populated on refresh(). Used by
        # the double-click, context-menu and keyboard shortcuts to act
        # on the row the user selected.
        self._iid_to_path: dict[str, Path] = {}
        self._build_ui()

    def _build_ui(self):
        # Log list
        list_frame = ttk.LabelFrame(self, text="Backup logs", padding=Spacing.PAD)
        list_frame.pack(fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.LARGE)

        self.log_tree = ttk.Treeview(
            list_frame,
            columns=("date", "profile", "status", "size"),
            show="headings",
            height=12,
        )
        self.log_tree.heading("date", text="Date")
        self.log_tree.heading("profile", text="Profile")
        self.log_tree.heading("status", text="Status")
        self.log_tree.heading("size", text="Size")
        self.log_tree.column("date", width=180)
        self.log_tree.column("profile", width=180)
        self.log_tree.column("status", width=110)
        self.log_tree.column("size", width=90)

        # Status tag colors — consistent with Verify tab semantics.
        self.log_tree.tag_configure("success", foreground=Colors.SUCCESS)
        self.log_tree.tag_configure("cancelled", foreground="#f39c12")
        self.log_tree.tag_configure("failed", foreground=Colors.DANGER)
        self.log_tree.tag_configure("unknown", foreground=Colors.TEXT_SECONDARY)

        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self.log_tree.yview)
        self.log_tree.configure(yscrollcommand=scrollbar.set)
        self.log_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Open log on double-click — intuitive default action.
        self.log_tree.bind("<Double-Button-1>", self._on_double_click)
        # Right-click context menu for Open / Copy path / Delete.
        self.log_tree.bind("<Button-3>", self._on_right_click)

        self._context_menu = tk.Menu(self, tearoff=0)
        self._context_menu.add_command(label="Open log", command=self._open_selected)
        self._context_menu.add_command(label="Copy path", command=self._copy_selected_path)
        self._context_menu.add_separator()
        self._context_menu.add_command(label="Delete log", command=self._delete_selected)

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        ttk.Button(btn_frame, text="Refresh", command=self.refresh).pack(side="left")
        ttk.Button(btn_frame, text="Open logs folder", command=self._open_folder).pack(
            side="left", padx=Spacing.MEDIUM
        )

        self.refresh()

    def refresh(self):
        for item in self.log_tree.get_children():
            self.log_tree.delete(item)
        self._iid_to_path.clear()

        if not self._log_dir.exists():
            return

        log_files = sorted(
            (f for f in self._log_dir.glob("backup_*.log") if f.name != "backup_manager.log"),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )

        from datetime import datetime

        for log_file in log_files[:100]:
            profile = self._extract_profile_name(log_file)
            status = self._extract_status(log_file)
            status_display = {
                "success": "Success",
                "cancelled": "Cancelled",
                "failed": "Failed",
                "unknown": "—",
            }.get(status, "—")

            size = log_file.stat().st_size
            size_str = f"{size / 1024:.1f} KB" if size > 1024 else f"{size} B"

            try:
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                date_display = mtime.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                date_display = ""

            iid = self.log_tree.insert(
                "",
                "end",
                values=(date_display, profile, status_display, size_str),
                tags=(status,),
            )
            self._iid_to_path[iid] = log_file

    @staticmethod
    def _extract_profile_name(log_file: Path) -> str:
        """Extract profile name from first line of log file.

        Args:
            log_file: Path to the log file.

        Returns:
            Profile name, or 'Unknown' if not found.
        """
        try:
            with open(log_file, encoding="utf-8") as f:
                first_line = f.readline()
            # Format: "... Starting backup 'ProfileName'..."
            if "'" in first_line:
                return first_line.split("'")[1]
        except Exception:
            pass
        return "Unknown"

    @staticmethod
    def _extract_status(log_file: Path) -> str:
        """Classify the backup outcome recorded in the log.

        Scans the log for the distinctive one-line markers emitted by
        ``BackupEngine.run_backup`` at each exit path. We scan the whole
        file rather than only the last line because the trailing line of
        a cancelled backup is sometimes a tray notification rather than
        the cancellation marker itself.

        Returns:
            One of ``"success"``, ``"cancelled"``, ``"failed"`` or
            ``"unknown"``.
        """
        try:
            with open(log_file, encoding="utf-8") as f:
                content = f.read()
        except OSError:
            return "unknown"

        if "Backup complete:" in content:
            return "success"
        if "Backup cancelled" in content:
            return "cancelled"
        if "Backup failed" in content or "ERROR" in content:
            return "failed"
        return "unknown"

    def _selected_log_path(self) -> Path | None:
        sel = self.log_tree.selection()
        if not sel:
            return None
        return self._iid_to_path.get(sel[0])

    def _on_double_click(self, _event) -> None:
        self._open_selected()

    def _on_right_click(self, event) -> None:
        row = self.log_tree.identify_row(event.y)
        if not row:
            return
        # Make sure the right-clicked row is also the selected one so
        # that the menu operates on what the user is pointing at.
        self.log_tree.selection_set(row)
        try:
            self._context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self._context_menu.grab_release()

    def _open_selected(self) -> None:
        path = self._selected_log_path()
        if path is None or not path.exists():
            return
        # ``os.startfile`` opens the OS-registered handler for the
        # extension — .log on Windows typically routes to Notepad.
        try:
            os.startfile(str(path))
        except OSError as exc:
            messagebox.showwarning("Open log", f"Could not open {path.name}: {exc}")

    def _copy_selected_path(self) -> None:
        path = self._selected_log_path()
        if path is None:
            return
        self.clipboard_clear()
        self.clipboard_append(str(path))
        # ``update`` forces Tk to flush the clipboard so the value
        # survives the app shutting down or the user quickly pasting.
        self.update()

    def _delete_selected(self) -> None:
        path = self._selected_log_path()
        if path is None:
            return
        if not messagebox.askyesno(
            "Delete log",
            f"Delete this log file?\n\n{path.name}\n\n"
            "This only removes the log file, not the backup itself.",
        ):
            return
        try:
            path.unlink()
        except OSError as exc:
            messagebox.showwarning("Delete log", f"Could not delete {path.name}: {exc}")
            return
        self.refresh()

    def _open_folder(self):
        if self._log_dir.exists():
            # Prefer explorer.exe for consistent behaviour and to avoid
            # os.startfile edge cases with paths containing spaces.
            try:
                subprocess.Popen(["explorer", str(self._log_dir)])
            except OSError:
                os.startfile(str(self._log_dir))

    def load_profile(self, profile):
        pass

    def collect_config(self) -> dict:
        return {}
