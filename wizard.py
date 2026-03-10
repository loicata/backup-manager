"""
Backup Manager - Setup Wizard
==============================
First-launch wizard that guides users through creating their first backup profile.
11 steps in a Toplevel dialog:

  1. Restore or New     — restore an existing backup or create fresh profile
  2. Welcome            — profile name
  3. Sources            — folders to back up (with quick-add for common Windows dirs)
  4. Storage            — destination type + credentials
  5. Mirrors            — optional 3-2-1 rule mirrors
  6. Backup Type        — Full / Incremental / Differential
  7. Retention          — Simple (keep N) or GFS (daily/weekly/monthly)
  8. Encryption         — AES-256-GCM password (optional)
  9. Schedule           — manual or automatic (hourly/daily/weekly/monthly)
  10. Email             — SMTP notification config (4 radio presets)
  11. Summary           — review all choices, check disk space, create profile

Toplevel mode: receives parent=_root from gui.py entry point.
Uses grab_set()/grab_release() to be modal. _close() ensures proper cleanup.
Result: wizard.result_profile (BackupProfile or None if skipped).
"""

import os
import tkinter as tk
from tkinter import ttk, filedialog
from pathlib import Path
from typing import Optional

from config import (
    ConfigManager, BackupProfile, StorageConfig, ScheduleConfig,
    RetentionConfig, RetentionPolicy,
    BackupType, StorageType, ScheduleFrequency,
)
from encryption import EncryptionConfig, store_password
from verification import VerificationConfig
from email_notifier import EmailConfig
from storage import check_destination_space, StorageBackend
from installer import get_available_features, FEAT_ENCRYPTION, FEAT_SFTP, FEAT_S3


# ──────────────────────────────────────────────
#  Constants
# ──────────────────────────────────────────────
WIZARD_WIDTH = 800
WIZARD_HEIGHT = 700

COLORS = {
    "bg":      "#f5f6fa",
    "accent":  "#3498db",
    "success": "#27ae60",
    "warning": "#f39c12",
    "text":    "#2c3e50",
    "muted":   "#95a5a6",
    "card_bg": "#ffffff",
}


def should_show_wizard(config_manager: ConfigManager) -> bool:
    """Check if the wizard should be shown (first launch = no profiles)."""
    profiles = config_manager.get_all_profiles()
    return len(profiles) == 0


# ──────────────────────────────────────────────
#  Setup Wizard
# ──────────────────────────────────────────────
class SetupWizard:
    """
    Multi-step configuration wizard shown at first launch.

    Steps:
      1. Welcome
      2. What to back up?
      3. Where to store backups?
      4. Backup type (full vs incremental)
      5. Retention policy (simple vs GFS)
      6. Encryption (yes/no)
      7. Automatic scheduling
      8. Summary + profile creation
    """

    TOTAL_STEPS = 11

    def __init__(self, config_manager: ConfigManager, parent=None):
        self.config = config_manager
        self.features = get_available_features()
        self.result_profile: Optional[BackupProfile] = None
        self._restore_requested = False
        self._parent = parent

        if parent:
            self.root = tk.Toplevel(parent)
        else:
            self.root = tk.Tk()
        self.root.title('Backup Manager — Setup Wizard')
        self.root.geometry(f"{WIZARD_WIDTH}x{WIZARD_HEIGHT}")
        self.root.resizable(False, False)
        self.root.configure(bg=COLORS["bg"])

        if parent:
            self.root.transient(parent)
            self.root.grab_set()

        # Center
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() - WIZARD_WIDTH) // 2
        y = (self.root.winfo_screenheight() - WIZARD_HEIGHT) // 2
        self.root.geometry(f"{WIZARD_WIDTH}x{WIZARD_HEIGHT}+{x}+{y}")

        self.current_step = 0

        # Collected data
        self.data = {
            "name": "My Backup",
            # ── Wizard data: stores all user choices as a flat dict ──
            # Each step reads/writes to this dict via trace callbacks.
            "sources": [],
            "storage_type": StorageType.LOCAL.value,
            "dest_path": "",
            "sftp_host": "", "sftp_user": "", "sftp_password": "",
            "sftp_key_path": "", "sftp_remote": "/backups",
            # S3
            "s3_bucket": "", "s3_prefix": "", "s3_region": "eu-west-1",
            "s3_access_key": "", "s3_secret_key": "",
            "s3_endpoint": "", "s3_provider": "aws",
            # Azure Blob Storage
            "azure_connection_string": "", "azure_container": "", "azure_prefix": "",
            # Google Cloud Storage
            "gcs_bucket": "", "gcs_prefix": "", "gcs_credentials_path": "",
            # Proton Drive
            "proton_username": "", "proton_password": "",
            "proton_2fa": "", "proton_remote_path": "/Backups",
            # Mirror destinations (list of StorageConfig dicts)
            "mirrors": [],
            #
            "backup_type": BackupType.FULL.value,
            "compress": False,
            "retention_policy": RetentionPolicy.SIMPLE.value,
            "max_backups": 10,
            "gfs_daily": 7, "gfs_weekly": 4, "gfs_monthly": 12,
            "encryption_mode": "none",  # "none", "mirrors_only", "all"
            "encrypt_password": "",
            "schedule_enabled": False,
            "schedule_freq": ScheduleFrequency.DAILY.value,
            "schedule_time": "02:00",
            # Email notifications
            "email_trigger": "disabled",  # "disabled", "failure", "success", "always"
            "smtp_host": "", "smtp_port": 587, "smtp_tls": True,
            "smtp_user": "", "smtp_password": "",
            "email_from": "", "email_to": "",
        }

        self._build_layout()
        self._show_step(0)

    def _build_layout(self):
        """Build the fixed wizard layout: header, content, footer."""
        # Header with progress
        header = tk.Frame(self.root, bg=COLORS["accent"], height=80)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        self.lbl_title = tk.Label(
            header, text="", bg=COLORS["accent"], fg="white",
            font=("Segoe UI", 16, "bold"))
        self.lbl_title.pack(pady=(15, 2))

        self.lbl_step = tk.Label(
            header, text="", bg=COLORS["accent"], fg="#bdc3c7",
            font=("Segoe UI", 9))
        self.lbl_step.pack()

        # Progress bar
        self.progress = ttk.Progressbar(
            self.root, maximum=self.TOTAL_STEPS, length=WIZARD_WIDTH,
            mode="determinate")
        self.progress.pack(fill=tk.X)

        # Footer with navigation — packed FIRST with side=BOTTOM
        # so it is always visible regardless of content height
        footer = tk.Frame(self.root, bg=COLORS["bg"])
        footer.pack(side=tk.BOTTOM, fill=tk.X, padx=30, pady=(0, 15))

        self.btn_back = ttk.Button(footer, text='← Back',
                                     command=self._go_back)
        self.btn_back.pack(side=tk.LEFT)

        self.btn_next = ttk.Button(footer, text='Next →',
                                     command=self._go_next)
        self.btn_next.pack(side=tk.RIGHT)

        # Separator above footer
        ttk.Separator(self.root, orient=tk.HORIZONTAL).pack(
            side=tk.BOTTOM, fill=tk.X, padx=30, pady=(5, 0))

        # Content area — scrollable canvas for steps with lots of content
        # Packed AFTER footer so it fills remaining space above
        content_outer = tk.Frame(self.root, bg=COLORS["bg"])
        content_outer.pack(fill=tk.BOTH, expand=True, padx=30, pady=15)

        self._content_canvas = tk.Canvas(
            content_outer, bg=COLORS["bg"], highlightthickness=0)
        self._content_scrollbar = ttk.Scrollbar(
            content_outer, orient=tk.VERTICAL, command=self._content_canvas.yview)
        self.content_frame = tk.Frame(self._content_canvas, bg=COLORS["bg"])

        self.content_frame.bind("<Configure>",
            lambda e: self._content_canvas.configure(
                scrollregion=self._content_canvas.bbox("all")))

        self._content_canvas_window = self._content_canvas.create_window(
            (0, 0), window=self.content_frame, anchor="nw")

        self._content_canvas.bind("<Configure>",
            lambda e: self._content_canvas.itemconfig(
                self._content_canvas_window, width=e.width))

        self._content_canvas.configure(yscrollcommand=self._content_scrollbar.set)
        self._content_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._content_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        def _on_mousewheel(event):
            self._content_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        self._content_canvas.bind_all("<MouseWheel>", _on_mousewheel)

    def _clear_content(self):
        for w in self.content_frame.winfo_children():
            w.destroy()

    # ── Navigation: render a specific wizard step ──
    # Clears the content frame, calls the step method, updates progress bar.
    def _show_step(self, step: int):
        self.current_step = step
        self._clear_content()
        self._content_canvas.yview_moveto(0)
        self.progress["value"] = step + 1

        # ── Ordered list of wizard steps ──
        # Each step returns (title, subtitle) for the header.
        steps = [
            self._step_restore_or_new,   # Step 1: Restore or New?
            self._step_welcome,           # Step 2: Profile name
            self._step_sources,           # Step 3: What to backup
            self._step_storage,           # Step 4: Where to store
            self._step_mirrors,           # Step 5: Mirror destinations
            self._step_backup_type,       # Step 6: Full, incremental, or differential
            self._step_retention,         # Step 7: Retention policy
            self._step_encryption,        # Step 8: Encryption + password
            self._step_schedule,          # Step 9: Scheduling
            self._step_email,             # Step 10: Email notifications
            self._step_summary,           # Step 11: Summary
        ]

        title, subtitle = steps[step]()

        self.lbl_title.configure(text=title)
        self.lbl_step.configure(
            text=f"Step {step + 1} of {self.TOTAL_STEPS} — {subtitle}")

        self.btn_back.configure(
            state=tk.NORMAL if step > 0 else tk.DISABLED)
        if step == self.TOTAL_STEPS - 1:
            self.btn_next.configure(text='✅ Create profile and launch')
        else:
            self.btn_next.configure(text='Next →')

    def _go_next(self):
        # If restore was requested at step 0, skip to restore
        if self.current_step == 0 and self._restore_requested:
            self._do_restore()
            return

        # Validate storage step (step 3): destination must be set for local/network
        if self.current_step == 3:
            stype = self.data["storage_type"]
            if stype == StorageType.LOCAL.value and not self.data["dest_path"].strip():
                # Auto-open browse dialog at This PC
                path = self._browse_folder_thispc("Choose destination folder")
                if path:
                    self.data["dest_path"] = path
                    if hasattr(self, '_wizard_dest_var'):
                        self._wizard_dest_var.set(path)
                else:
                    return  # User cancelled browse
            elif stype == StorageType.NETWORK.value and not self.data["dest_path"].strip():
                from tkinter import messagebox
                messagebox.showwarning("Missing path",
                    "Please enter a network path (e.g. \\\\server\\share\\backups).",
                    parent=self.root)
                return

        # Validate encryption step (step 7) before moving on
        if self.current_step == 7 and self.data["encryption_mode"] != "none":
            pwd = self.data.get("encrypt_password", "")
            confirm = self._wizard_pwd_confirm_var.get() if hasattr(self, '_wizard_pwd_confirm_var') else ""
            if len(pwd) < 16:
                from tkinter import messagebox
                messagebox.showwarning("Password too short",
                    f"The encryption password must be at least 16 characters.\n"
                    f"Currently: {len(pwd)} characters.",
                    parent=self.root)
                return
            if pwd != confirm:
                from tkinter import messagebox
                messagebox.showerror("Password mismatch",
                    "The password and confirmation do not match.",
                    parent=self.root)
                return

        if self.current_step < self.TOTAL_STEPS - 1:
            self._show_step(self.current_step + 1)
        else:
            self._finish()

    def _go_back(self):
        if self.current_step > 0:
            self._show_step(self.current_step - 1)

    def _skip_wizard(self):
        self.result_profile = None
        self._close()

    def _finish(self):
        """Create the profile from collected data and close wizard."""
        self.result_profile = self._build_profile()
        self.config.save_profile(self.result_profile)
        self._close()

    def _close(self):
        """Properly release grab and destroy the wizard window."""
        try:
            self.root.grab_release()
        except Exception:
            pass
        self.root.destroy()

    # ──────────────────────────────────
    #  UI Helpers
    # ──────────────────────────────────
    def _make_card(self, parent, text: str, padx=0, pady=(0, 10)):
        """Create an explanation card."""
        card = tk.Frame(parent, bg=COLORS["card_bg"], padx=15, pady=10,
                         relief=tk.SOLID, bd=1)
        card.pack(fill=tk.X, padx=padx, pady=pady)
        tk.Label(card, text=text, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=650, justify=tk.LEFT).pack(
            anchor="w")
        return card

    def _make_info(self, parent, text: str):
        """Create a light info box."""
        frame = tk.Frame(parent, bg="#d5f5e3", padx=12, pady=6)
        frame.pack(fill=tk.X, pady=(5, 8))
        tk.Label(frame, text=text, bg="#d5f5e3", fg="#1e8449",
                 font=("Segoe UI", 9), wraplength=650, justify=tk.LEFT).pack(
            anchor="w")

    def _make_warning(self, parent, text: str):
        """Create a warning info box."""
        frame = tk.Frame(parent, bg="#ffeaa7", padx=12, pady=6)
        frame.pack(fill=tk.X, pady=(5, 8))
        tk.Label(frame, text=text, bg="#ffeaa7", fg="#856404",
                 font=("Segoe UI", 9), wraplength=650, justify=tk.LEFT).pack(
            anchor="w")

    # ──────────────────────────────────
    #  STEP 0: Restore or New?
    # ──────────────────────────────────
    def _step_restore_or_new(self) -> tuple[str, str]:
        f = self.content_frame

        tk.Label(f, text="Welcome to Backup Manager!",
                 bg=COLORS["bg"], fg=COLORS["text"],
                 font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(0, 10))

        self._make_card(f,
            "What would you like to do? You can set up a new backup profile "
            "or restore files from an existing backup."
        )

        choice_var = tk.StringVar(value="new")

        # Option 1: New backup
        new_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=12,
                              relief=tk.SOLID, bd=1, cursor="hand2")
        new_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(new_frame, text="📦 Set up a new backup",
                         variable=choice_var, value="new",
                         command=lambda: self._set_restore(False)
                         ).pack(anchor="w")
        tk.Label(new_frame, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=680, justify=tk.LEFT,
                 text="Configure a new backup profile step by step: choose sources, "
                      "destination, encryption, scheduling, and more."
                 ).pack(anchor="w", padx=(20, 0), pady=(3, 0))

        # Option 2: Restore
        restore_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=12,
                                  relief=tk.SOLID, bd=1, cursor="hand2")
        restore_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(restore_frame, text="🔄 Restore from an existing backup",
                         variable=choice_var, value="restore",
                         command=lambda: self._set_restore(True)
                         ).pack(anchor="w")
        tk.Label(restore_frame, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=680, justify=tk.LEFT,
                 text="Restore files from a previous Backup Manager backup.\n"
                      "You will need to select the backup file (.zip or .zip.wbenc) "
                      "and choose where to extract it. If the backup is encrypted, "
                      "you will need the original password."
                 ).pack(anchor="w", padx=(20, 0), pady=(3, 0))

        # Restore config area (shown when restore is selected)
        self._restore_config = ttk.LabelFrame(f, text="Restore configuration", padding=10)
        self._restore_config.pack(fill=tk.X, pady=(10, 0))

        ttk.Label(self._restore_config, text="Backup file to restore:").pack(
            anchor="w", pady=(0, 3))
        row1 = ttk.Frame(self._restore_config)
        row1.pack(fill=tk.X, pady=(0, 5))
        self._restore_file_var = tk.StringVar()
        ttk.Entry(row1, textvariable=self._restore_file_var,
                  font=("Consolas", 9)).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(row1, text="Browse...",
                    command=self._browse_restore_file).pack(side=tk.RIGHT)

        ttk.Label(self._restore_config, text="Extract to folder:").pack(
            anchor="w", pady=(0, 3))
        row2 = ttk.Frame(self._restore_config)
        row2.pack(fill=tk.X, pady=(0, 5))
        self._restore_dest_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self._restore_dest_var,
                  font=("Consolas", 9)).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(row2, text="Browse...",
                    command=lambda: self._restore_dest_var.set(
                        filedialog.askdirectory(parent=self.root) or self._restore_dest_var.get())
                    ).pack(side=tk.RIGHT)

        ttk.Label(self._restore_config,
                  text="Password (only if backup is encrypted):",
                  ).pack(anchor="w", pady=(0, 3))
        self._restore_pwd_var = tk.StringVar()
        ttk.Entry(self._restore_config, textvariable=self._restore_pwd_var,
                  show="•", font=("Consolas", 10)).pack(fill=tk.X)

        self._make_info(f,
            "💡 Click 'Next' to proceed. For new backup: the wizard continues. "
            "For restore: the restore starts immediately."
        )

        # Initially hide restore config
        self._set_restore(False)

        return ("🚀 Welcome", "New backup or Restore?")

    def _set_restore(self, restore: bool):
        self._restore_requested = restore
        if restore:
            self._restore_config.pack(fill=tk.X, pady=(10, 0))
        else:
            self._restore_config.pack_forget()

    def _browse_restore_file(self):
        path = filedialog.askopenfilename(
            parent=self.root,
            title="Select backup file",
            filetypes=[
                ("Backup files", "*.zip *.wbenc"),
                ("ZIP archives", "*.zip"),
                ("Encrypted backups", "*.wbenc"),
                ("All files", "*.*"),
            ])
        if path:
            self._restore_file_var.set(path)

    def _do_restore(self):
        """Perform the restore operation."""
        import zipfile
        import shutil

        backup_file = self._restore_file_var.get()
        dest_folder = self._restore_dest_var.get()
        password = self._restore_pwd_var.get()

        if not backup_file or not Path(backup_file).exists():
            from tkinter import messagebox
            messagebox.showerror("Error", "Please select a valid backup file.",
                                  parent=self.root)
            return

        if not dest_folder:
            from tkinter import messagebox
            messagebox.showerror("Error", "Please select a destination folder.",
                                  parent=self.root)
            return

        Path(dest_folder).mkdir(parents=True, exist_ok=True)

        try:
            # Check if encrypted
            if backup_file.endswith(".wbenc"):
                if not password:
                    from tkinter import messagebox
                    messagebox.showerror("Error",
                        "This backup is encrypted. Please enter the password.",
                        parent=self.root)
                    return

                from encryption import get_crypto_engine, ENCRYPTED_EXTENSION
                crypto = get_crypto_engine()
                if not crypto.is_available:
                    from tkinter import messagebox
                    messagebox.showerror("Error",
                        "Decryption module not available. Install: pip install cryptography",
                        parent=self.root)
                    return

                # Decrypt first
                decrypted_path = Path(dest_folder) / "restored_backup.zip"
                crypto.decrypt_file(
                    source=Path(backup_file),
                    dest=decrypted_path,
                    password=password,
                )
                backup_file = str(decrypted_path)

            try:
                # Extract ZIP
                if backup_file.endswith(".zip"):
                    with zipfile.ZipFile(backup_file, 'r') as zf:
                        # ZIP bomb protection: check total uncompressed size
                        MAX_EXTRACT_SIZE = 50 * 1024 * 1024 * 1024  # 50 GB limit
                        total_size = sum(info.file_size for info in zf.infolist())
                        if total_size > MAX_EXTRACT_SIZE:
                            from tkinter import messagebox
                            messagebox.showerror("Error",
                                f"Archive uncompressed size ({total_size / (1024**3):.1f} GB) "
                                f"exceeds safety limit (50 GB).\n"
                                f"This may be a corrupted or malicious file.",
                                parent=self.root)
                            return
                        # ZIP Slip protection: extract file by file with path check
                        for member in zf.infolist():
                            target = os.path.normpath(os.path.join(dest_folder, member.filename))
                            if not target.startswith(os.path.normpath(dest_folder)):
                                continue  # Skip path traversal attempts
                            zf.extract(member, dest_folder)
                else:
                    # Not a ZIP — just copy the file
                    shutil.copy2(backup_file, dest_folder)
            finally:
                # Always clean up decrypted temp file
                temp = Path(dest_folder) / "restored_backup.zip"
                if temp.exists() and str(temp) != backup_file:
                    try:
                        temp.unlink()
                    except OSError:
                        pass

            from tkinter import messagebox
            messagebox.showinfo("Restore complete!",
                f"Backup has been restored to:\n{dest_folder}",
                parent=self.root)

            # Close wizard after restore
            self.result_profile = None
            self._close()

        except Exception as e:
            from tkinter import messagebox
            messagebox.showerror("Restore failed",
                f"An error occurred during restore:\n{e}",
                parent=self.root)

    # ──────────────────────────────────
    #  STEP 1: Welcome
    # ──────────────────────────────────
    # ── Step 1: Choose new backup or restore existing ──
    def _step_welcome(self) -> tuple[str, str]:
        f = self.content_frame

        tk.Label(f, text="Welcome to Backup Manager!",
                 bg=COLORS["bg"], fg=COLORS["text"],
                 font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(0, 10))

        self._make_card(f,
            "This wizard will guide you step by step to configure your "
            "first backup profile. Each screen clearly explains "
            "the available options and their consequences.\n\n"
            "You can change all these settings at any time in the app."
        )

        tk.Label(f, text="Backup profile name:",
                 bg=COLORS["bg"], font=("Segoe UI", 10)).pack(
            anchor="w", pady=(15, 3))

        name_var = tk.StringVar(value=self.data["name"])
        entry = ttk.Entry(f, textvariable=name_var, font=("Segoe UI", 12), width=40)
        entry.pack(anchor="w")
        entry.focus_set()
        name_var.trace_add("write", lambda *a: self.data.update({"name": name_var.get()}))

        self._make_info(f,
            "💡 Choose a descriptive name, for example: 'Office Documents', "
            "'Family Photos', 'Full Server'..."
        )

        return ("🚀 Welcome", "Introduction")

    # ──────────────────────────────────
    #  STEP 2: Sources
    # ──────────────────────────────────
    # ── Step 2: Select folders to back up ──
    # Quick-add buttons for common Windows directories (Desktop, Documents, etc.)
    def _step_sources(self) -> tuple[str, str]:
        f = self.content_frame

        self._make_card(f,
            "Select the folders and files you want to back up. "
            "You can add as many as needed. Folders will be "
            "backed up with their full directory tree (subfolders included)."
        )

        # Source list
        list_frame = ttk.Frame(f)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self._wizard_source_listbox = tk.Listbox(
            list_frame, font=("Consolas", 9), height=6,
            relief=tk.SOLID, bd=1)
        self._wizard_source_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        for src in self.data["sources"]:
            self._wizard_source_listbox.insert(tk.END, src)

        btn_frame = ttk.Frame(f)
        btn_frame.pack(fill=tk.X)
        ttk.Button(btn_frame, text="📁 Add folder...",
                    command=self._wizard_browse_folder).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_frame, text="✕ Remove",
                    command=self._wizard_remove_source).pack(side=tk.LEFT)

        return ("📂 What to back up?", "Source selection")

    def _wizard_add_source(self, path: str):
        if Path(path).exists() and path not in self.data["sources"]:
            self.data["sources"].append(path)
            self._wizard_source_listbox.insert(tk.END, path)

    def _wizard_browse_folder(self):
        path = self._browse_folder_thispc("Add a folder")
        if path:
            self._wizard_add_source(path)

    def _browse_folder_thispc(self, title: str = "Select a folder") -> str:
        """Open a standard folder browser."""
        return filedialog.askdirectory(title=title, parent=self.root) or ""

    def _wizard_remove_source(self):
        sel = self._wizard_source_listbox.curselection()
        if sel:
            idx = sel[0]
            self._wizard_source_listbox.delete(idx)
            self.data["sources"].pop(idx)

    # ──────────────────────────────────
    #  STEP 3: Storage
    # ──────────────────────────────────
    # ── Step 3: Choose destination type and enter credentials ──
    # Dynamic UI: shows/hides credential fields based on storage type.
    def _step_storage(self) -> tuple[str, str]:
        f = self.content_frame

        self._make_card(f,
            "Choose where your backups will be stored. Golden rule: always "
            "store backups on a DIFFERENT medium from the original files."
        )

        storage_var = tk.StringVar(value=self.data["storage_type"])

        options = [
            (StorageType.LOCAL.value,
             "💿 External drive / USB stick",
             "Simple. Plug in a drive, pick a folder."),
            (StorageType.NETWORK.value,
             "🌐 Network folder (NAS / server)",
             "Backup to a NAS or server on your local network."),
            (StorageType.SFTP.value,
             "🔒 Remote server (SFTP)",
             "Encrypted transfer via SSH. Data is off-site."),
            (StorageType.S3.value,
             "☁ Amazon S3 / S3-compatible (Wasabi, OVH, MinIO...)",
             "S3-compatible cloud storage. High availability, geographic redundancy."),
            (StorageType.AZURE.value,
             "☁ Azure Blob Storage",
             "Microsoft Azure cloud storage. Scalable, geo-redundant, enterprise-grade."),
            (StorageType.GCS.value,
             "☁ Google Cloud Storage",
             "Google Cloud object storage. Fast, scalable, integrated with GCP services."),
            (StorageType.PROTON.value,
             "🔒 Proton Drive",
             "End-to-end encrypted cloud via rclone. Requires rclone installed."),
        ]

        for val, title, desc in options:
            row = tk.Frame(f, bg=COLORS["card_bg"], padx=10, pady=4,
                            relief=tk.SOLID, bd=1, cursor="hand2")
            row.pack(fill=tk.X, pady=1)

            rb = ttk.Radiobutton(row, text=title, variable=storage_var, value=val,
                                  command=lambda v=val: [
                                      self.data.update({"storage_type": v}),
                                      self._update_wizard_storage(v)])
            rb.pack(anchor="w")
            tk.Label(row, text=desc, bg=COLORS["card_bg"], fg=COLORS["muted"],
                     font=("Segoe UI", 8), wraplength=700, justify=tk.LEFT).pack(
                anchor="w", padx=(20, 0))

        # Dynamic config area
        self._wizard_storage_config = ttk.LabelFrame(f, text="Configuration", padding=10)
        self._wizard_storage_config.pack(fill=tk.X, pady=(10, 0))
        self._update_wizard_storage(self.data["storage_type"])

        return ("💾 Where to store?", "Primary destination")

    def _update_wizard_storage(self, stype: str):
        for w in self._wizard_storage_config.winfo_children():
            w.destroy()
        frame = self._wizard_storage_config

        if stype == StorageType.LOCAL.value:
            ttk.Label(frame, text="Destination folder:").pack(anchor="w", pady=(0, 3))
            row = ttk.Frame(frame)
            row.pack(fill=tk.X)
            dest_var = tk.StringVar(value=self.data["dest_path"])
            dest_var.trace_add("write", lambda *a: self.data.update({"dest_path": dest_var.get()}))
            self._wizard_dest_var = dest_var
            entry = ttk.Entry(row, textvariable=dest_var, font=("Consolas", 10))
            entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
            ttk.Button(row, text="Browse...",
                        command=lambda: self._wizard_browse_dest(dest_var)
                        ).pack(side=tk.RIGHT)

        elif stype == StorageType.NETWORK.value:
            ttk.Label(frame, text="Network path (e.g. \\\\server\\share\\backups):").pack(anchor="w", pady=(0, 3))
            dest_var = tk.StringVar(value=self.data["dest_path"])
            dest_var.trace_add("write", lambda *a: self.data.update({"dest_path": dest_var.get()}))
            self._wizard_dest_var = dest_var
            ttk.Entry(frame, textvariable=dest_var, font=("Consolas", 10)).pack(fill=tk.X)

        elif stype == StorageType.SFTP.value:
            for label, key, show in [
                ("Host (IP or hostname):", "sftp_host", ""),
                ("Username:", "sftp_user", ""),
                ("Password (leave empty if using SSH key):", "sftp_password", "•"),
                ("Remote path:", "sftp_remote", ""),
            ]:
                ttk.Label(frame, text=label).pack(anchor="w", pady=(4, 1))
                var = tk.StringVar(value=self.data[key])
                var.trace_add("write", lambda *a, k=key, v=var: self.data.update({k: v.get()}))
                kwargs = {"font": ("Consolas", 10)}
                if show:
                    kwargs["show"] = show
                ttk.Entry(frame, textvariable=var, **kwargs).pack(fill=tk.X)

            # SSH key file (optional)
            ttk.Label(frame, text="SSH private key (optional):").pack(anchor="w", pady=(4, 1))
            key_row = ttk.Frame(frame)
            key_row.pack(fill=tk.X)
            key_var = tk.StringVar(value=self.data["sftp_key_path"])
            key_var.trace_add("write", lambda *a: self.data.update({"sftp_key_path": key_var.get()}))
            ttk.Entry(key_row, textvariable=key_var, font=("Consolas", 10)).pack(
                side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
            ttk.Button(key_row, text="Browse...",
                       command=lambda: key_var.set(
                           filedialog.askopenfilename(
                               title="Select SSH private key",
                               filetypes=[("Key files", "*.pem *.key *.ppk *.id_rsa"),
                                          ("All files", "*.*")],
                               parent=self.root) or key_var.get())
                       ).pack(side=tk.RIGHT)
            tk.Label(frame, text="Supports RSA, Ed25519, ECDSA keys. "
                     "Leave empty to use password authentication.",
                     font=("Segoe UI", 8), fg="#95a5a6").pack(anchor="w", pady=(2, 0))

        elif stype == StorageType.S3.value:
            # Provider selector
            ttk.Label(frame, text="Provider:").pack(anchor="w", pady=(0, 1))
            providers = ["aws", "wasabi", "ovh", "scaleway", "minio",
                         "digitalocean", "cloudflare_r2", "backblaze_b2", "other"]
            provider_var = tk.StringVar(value=self.data["s3_provider"])
            provider_var.trace_add("write", lambda *a: self.data.update({"s3_provider": provider_var.get()}))
            ttk.Combobox(frame, textvariable=provider_var,
                         values=providers, state="readonly").pack(fill=tk.X, pady=(0, 5))

            # S3 fields in two columns
            s3_grid = ttk.Frame(frame)
            s3_grid.pack(fill=tk.X)
            s3_grid.columnconfigure(1, weight=1)
            s3_grid.columnconfigure(3, weight=1)

            s3_fields = [
                (0, 0, "Bucket:", "s3_bucket", ""),
                (0, 2, "Prefix (optional):", "s3_prefix", ""),
                (1, 0, "Region:", "s3_region", ""),
                (1, 2, "Endpoint URL (non-AWS):", "s3_endpoint", ""),
                (2, 0, "Access Key:", "s3_access_key", ""),
                (2, 2, "Secret Key:", "s3_secret_key", "•"),
            ]
            for row_i, col, label, key, show in s3_fields:
                ttk.Label(s3_grid, text=label).grid(row=row_i, column=col, sticky="w", pady=2, padx=(0, 3))
                var = tk.StringVar(value=self.data[key])
                var.trace_add("write", lambda *a, k=key, v=var: self.data.update({k: v.get()}))
                kwargs = {"font": ("Consolas", 9)}
                if show:
                    kwargs["show"] = show
                ttk.Entry(s3_grid, textvariable=var, **kwargs).grid(
                    row=row_i, column=col + 1, sticky="ew", pady=2, padx=(0, 10))

            info = tk.Frame(frame, bg="#d5f5e3", padx=10, pady=5)
            info.pack(fill=tk.X, pady=(8, 0))
            tk.Label(info,
                     text="💡 For AWS: leave Endpoint empty. For others (Wasabi, OVH, MinIO...): "
                          "enter the endpoint URL provided by your cloud provider.",
                     bg="#d5f5e3", fg="#1e8449", font=("Segoe UI", 8),
                     wraplength=650, justify=tk.LEFT).pack(anchor="w")

        elif stype == StorageType.AZURE.value:
            ttk.Label(frame, text="Connection string:").pack(anchor="w", pady=(4, 1))
            var_conn = tk.StringVar(value=self.data["azure_connection_string"])
            var_conn.trace_add("write", lambda *a: self.data.update({"azure_connection_string": var_conn.get()}))
            ttk.Entry(frame, textvariable=var_conn, font=("Consolas", 9), show="•").pack(fill=tk.X)

            ttk.Label(frame, text="Container name:").pack(anchor="w", pady=(4, 1))
            var_cont = tk.StringVar(value=self.data["azure_container"])
            var_cont.trace_add("write", lambda *a: self.data.update({"azure_container": var_cont.get()}))
            ttk.Entry(frame, textvariable=var_cont, font=("Consolas", 9)).pack(fill=tk.X)

            ttk.Label(frame, text="Prefix / subfolder (optional):").pack(anchor="w", pady=(4, 1))
            var_prefix = tk.StringVar(value=self.data["azure_prefix"])
            var_prefix.trace_add("write", lambda *a: self.data.update({"azure_prefix": var_prefix.get()}))
            ttk.Entry(frame, textvariable=var_prefix, font=("Consolas", 9)).pack(fill=tk.X)

            info = tk.Frame(frame, bg="#d6eaf8", padx=10, pady=5)
            info.pack(fill=tk.X, pady=(8, 0))
            tk.Label(info,
                     text="💡 Find your connection string in the Azure Portal:\n"
                          "   Storage Account → Access keys → Connection string (click Show, then Copy).\n"
                          "   The container will be created automatically if it doesn't exist.",
                     bg="#d6eaf8", fg="#1a5276", font=("Segoe UI", 8),
                     wraplength=650, justify=tk.LEFT).pack(anchor="w")

        elif stype == StorageType.GCS.value:
            ttk.Label(frame, text="GCS Bucket:").pack(anchor="w", pady=(4, 1))
            var_bucket = tk.StringVar(value=self.data["gcs_bucket"])
            var_bucket.trace_add("write", lambda *a: self.data.update({"gcs_bucket": var_bucket.get()}))
            ttk.Entry(frame, textvariable=var_bucket, font=("Consolas", 9)).pack(fill=tk.X)

            ttk.Label(frame, text="Prefix / subfolder (optional):").pack(anchor="w", pady=(4, 1))
            var_prefix = tk.StringVar(value=self.data["gcs_prefix"])
            var_prefix.trace_add("write", lambda *a: self.data.update({"gcs_prefix": var_prefix.get()}))
            ttk.Entry(frame, textvariable=var_prefix, font=("Consolas", 9)).pack(fill=tk.X)

            ttk.Label(frame, text="Credentials JSON file (service account):").pack(anchor="w", pady=(4, 1))
            cred_row = ttk.Frame(frame)
            cred_row.pack(fill=tk.X)
            var_cred = tk.StringVar(value=self.data["gcs_credentials_path"])
            var_cred.trace_add("write", lambda *a: self.data.update({"gcs_credentials_path": var_cred.get()}))
            ttk.Entry(cred_row, textvariable=var_cred, font=("Consolas", 9)).pack(side=tk.LEFT, fill=tk.X, expand=True)
            ttk.Button(cred_row, text="Browse...",
                        command=lambda: var_cred.set(
                            filedialog.askopenfilename(
                                title="Select credentials JSON",
                                filetypes=[("JSON files", "*.json")],
                                parent=self.root) or var_cred.get()
                        )).pack(side=tk.LEFT, padx=(5, 0))

            info = tk.Frame(frame, bg="#e8f8e8", padx=10, pady=5)
            info.pack(fill=tk.X, pady=(8, 0))
            tk.Label(info,
                     text="💡 If empty, uses the GOOGLE_APPLICATION_CREDENTIALS environment variable\n"
                          "   or the default credentials from gcloud CLI.\n"
                          "   Create a service account in Google Cloud Console → IAM → Service Accounts.",
                     bg="#e8f8e8", fg="#1e6e1e", font=("Segoe UI", 8),
                     wraplength=650, justify=tk.LEFT).pack(anchor="w")

        elif stype == StorageType.PROTON.value:
            # Step-by-step setup guide
            guide = tk.Frame(frame, bg="#eaf2f8", padx=12, pady=8)
            guide.pack(fill=tk.X, pady=(0, 8))
            tk.Label(guide, text="📋 Proton Drive Setup Guide",
                     bg="#eaf2f8", fg="#2c3e50",
                     font=("Segoe UI", 10, "bold")).pack(anchor="w")
            tk.Label(guide, bg="#eaf2f8", fg="#2c3e50",
                     font=("Segoe UI", 8), wraplength=650, justify=tk.LEFT,
                     text="Proton Drive uses rclone (a free open-source tool) to transfer "
                          "your backups. Your files are end-to-end encrypted with the same "
                          "keys as the official Proton apps.\n\n"
                          "Step 1 — Install rclone\n"
                          "   Download from https://rclone.org/install/\n"
                          "   On Windows: download the .zip, extract it, and place rclone.exe\n"
                          "   somewhere in your PATH (e.g. C:\\Windows or C:\\rclone).\n"
                          "   To verify: open a terminal and type: rclone version\n\n"
                          "Step 2 — Log in to Proton web at least once\n"
                          "   Go to https://mail.proton.me and log in with your account.\n"
                          "   This generates the encryption keys needed by rclone.\n"
                          "   Without this step, rclone cannot access your Drive.\n\n"
                          "Step 3 — Fill in the fields below\n"
                          "   • Proton email: your full Proton email (e.g. user@proton.me)\n"
                          "   • Password: your Proton account password\n"
                          "   • 2FA / TOTP secret: ONLY if you have 2-factor authentication\n"
                          "     enabled on your Proton account. This is NOT the 6-digit code\n"
                          "     that changes every 30 seconds in your authenticator app.\n"
                          "     It is the long base32 string (e.g. JBSWY3DPEHPK3PXP) that\n"
                          "     was shown ONCE when you first enabled 2FA.\n\n"
                          "     ⚠ If you don't have this secret anymore:\n"
                          "     1. Log in to https://account.proton.me/u/0/mail/security\n"
                          "     2. Disable 2-factor authentication\n"
                          "     3. Re-enable it — this time SAVE the secret key that appears\n"
                          "        (the text string, not the QR code)\n"
                          "     4. Scan the QR code in your authenticator app as usual\n"
                          "     5. Paste the saved secret key here\n\n"
                          "   • Remote folder: the folder in Proton Drive where backups will go\n"
                          "     (e.g. /Backups). It will be created automatically if it doesn't exist.\n\n"
                          "Step 4 — Test the connection\n"
                          "   After completing the wizard, go to the Storage tab and click\n"
                          "   '🔌 Test connection' to verify everything works."
                     ).pack(anchor="w")

            # Fields
            for label, key, show in [
                ("Proton email:", "proton_username", ""),
                ("Password:", "proton_password", "•"),
                ("2FA TOTP secret (only if 2FA enabled — see guide above):", "proton_2fa", "•"),
                ("Remote folder in Proton Drive:", "proton_remote_path", ""),
            ]:
                ttk.Label(frame, text=label).pack(anchor="w", pady=(4, 1))
                var = tk.StringVar(value=self.data[key])
                var.trace_add("write", lambda *a, k=key, v=var: self.data.update({k: v.get()}))
                kwargs = {"font": ("Consolas", 10)}
                if show:
                    kwargs["show"] = show
                ttk.Entry(frame, textvariable=var, **kwargs).pack(fill=tk.X)

            warn = tk.Frame(frame, bg="#ffeaa7", padx=10, pady=5)
            warn.pack(fill=tk.X, pady=(8, 0))
            tk.Label(warn,
                     text="⚠ Your Proton password is stored securely on this computer using "
                          "Windows DPAPI encryption. It is never transmitted in plain text.",
                     bg="#ffeaa7", fg="#856404", font=("Segoe UI", 8),
                     wraplength=650, justify=tk.LEFT).pack(anchor="w")

    def _wizard_browse_dest(self, var: tk.StringVar):
        path = self._browse_folder_thispc("Destination folder")
        if path:
            var.set(path)
            self.data["dest_path"] = path

    # ──────────────────────────────────
    #  Mirror management (wizard)
    # ──────────────────────────────────
    def _get_mirror_display(self, cfg) -> tuple[str, str]:
        """Get display strings for a mirror StorageConfig."""
        stype = cfg.get("storage_type", "") if isinstance(cfg, dict) else cfg.storage_type
        type_labels = {
            StorageType.LOCAL.value: "💿 External drive",
            StorageType.NETWORK.value: "🌐 Network",
            StorageType.SFTP.value: "🔒 SFTP",
            StorageType.S3.value: "☁ S3",
            StorageType.AZURE.value: "☁ Azure",
            StorageType.GCS.value: "☁ GCS",
            StorageType.PROTON.value: "🔒 Proton Drive",
        }
        type_str = type_labels.get(stype, stype)

        if isinstance(cfg, dict):
            if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
                return type_str, cfg.get("destination_path", "")
            elif stype == StorageType.SFTP.value:
                return type_str, f"{cfg.get('sftp_username', '')}@{cfg.get('sftp_host', '')}:{cfg.get('sftp_remote_path', '')}"
            elif stype == StorageType.S3.value:
                return type_str, f"{cfg.get('s3_provider', '')} / {cfg.get('s3_bucket', '')}"
            elif stype == StorageType.AZURE.value:
                return type_str, cfg.get("azure_container", "")
            elif stype == StorageType.GCS.value:
                return type_str, cfg.get("gcs_bucket", "")
            elif stype == StorageType.PROTON.value:
                return type_str, f"{cfg.get('proton_username', '')} ({cfg.get('proton_remote_path', '')})"
        return type_str, ""

    def _refresh_wizard_mirrors(self):
        """Refresh the mirror treeview."""
        for item in self._wizard_mirror_tree.get_children():
            self._wizard_mirror_tree.delete(item)
        for cfg in self.data["mirrors"]:
            type_str, dest = self._get_mirror_display(cfg)
            self._wizard_mirror_tree.insert("", tk.END, values=(type_str, dest))

    def _wizard_remove_mirror(self):
        """Remove selected mirror."""
        sel = self._wizard_mirror_tree.selection()
        if sel:
            idx = self._wizard_mirror_tree.index(sel[0])
            if idx < len(self.data["mirrors"]):
                self.data["mirrors"].pop(idx)
                self._refresh_wizard_mirrors()

    def _wizard_add_mirror(self):
        """Open dialog to add a mirror destination."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Add mirror destination")
        dialog.geometry("550x420")
        dialog.transient(self.root)
        dialog.grab_set()

        # Center
        dialog.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() - 550) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - 420) // 2
        dialog.geometry(f"550x420+{x}+{y}")

        ttk.Label(dialog, text="Add a mirror destination",
                  font=("Segoe UI", 12, "bold")).pack(padx=15, pady=(10, 3), anchor="w")
        ttk.Label(dialog,
                  text="This destination receives a copy of the backup after it is "
                       "created on the primary destination.",
                  font=("Segoe UI", 9), wraplength=500).pack(padx=15, anchor="w", pady=(0, 8))

        # Type selector
        ttk.Label(dialog, text="Storage type:").pack(padx=15, anchor="w")
        mirror_type_var = tk.StringVar(value=StorageType.LOCAL.value)
        types = [
            StorageType.LOCAL.value, StorageType.NETWORK.value,
            StorageType.SFTP.value, StorageType.S3.value,
            StorageType.AZURE.value, StorageType.GCS.value, StorageType.PROTON.value,
        ]
        ttk.Combobox(dialog, textvariable=mirror_type_var,
                     values=types, state="readonly", width=20).pack(
            padx=15, anchor="w", pady=(2, 8))

        # Fields frame
        fields_outer = ttk.LabelFrame(dialog, text="Configuration", padding=8)
        fields_outer.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 8))

        field_vars = {}

        def update_mirror_fields(*args):
            for w in fields_outer.winfo_children():
                w.destroy()
            field_vars.clear()
            stype = mirror_type_var.get()

            if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
                lbl = "Path:" if stype == StorageType.LOCAL.value else "Network path:"
                ttk.Label(fields_outer, text=lbl).pack(anchor="w")
                row = ttk.Frame(fields_outer)
                row.pack(fill=tk.X)
                v = tk.StringVar()
                field_vars["destination_path"] = v
                ttk.Entry(row, textvariable=v, font=("Consolas", 9)).pack(
                    side=tk.LEFT, fill=tk.X, expand=True)
                if stype == StorageType.LOCAL.value:
                    ttk.Button(row, text="Browse...",
                                command=lambda: v.set(
                                    filedialog.askdirectory(parent=dialog) or v.get())
                                ).pack(side=tk.RIGHT, padx=(5, 0))

            elif stype == StorageType.SFTP.value:
                for lbl, key, show in [("Host:", "sftp_host", ""),
                                        ("Username:", "sftp_username", ""),
                                        ("Password (empty if using key):", "sftp_password", "•"),
                                        ("Remote path:", "sftp_remote_path", "")]:
                    ttk.Label(fields_outer, text=lbl).pack(anchor="w", pady=(3, 0))
                    v = tk.StringVar(value="/backups" if key == "sftp_remote_path" else "")
                    field_vars[key] = v
                    kw = {"font": ("Consolas", 9)}
                    if show:
                        kw["show"] = show
                    ttk.Entry(fields_outer, textvariable=v, **kw).pack(fill=tk.X)

                ttk.Label(fields_outer, text="SSH private key (optional):").pack(anchor="w", pady=(3, 0))
                key_row = ttk.Frame(fields_outer)
                key_row.pack(fill=tk.X)
                kv = tk.StringVar()
                field_vars["sftp_key_path"] = kv
                ttk.Entry(key_row, textvariable=kv, font=("Consolas", 9)).pack(
                    side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
                ttk.Button(key_row, text="Browse...",
                           command=lambda: kv.set(
                               filedialog.askopenfilename(
                                   title="Select SSH private key",
                                   filetypes=[("Key files", "*.pem *.key *.ppk *.id_rsa"),
                                              ("All files", "*.*")],
                                   parent=dialog) or kv.get())
                           ).pack(side=tk.RIGHT)

            elif stype == StorageType.S3.value:
                for lbl, key, show in [("Bucket:", "s3_bucket", ""),
                                        ("Region:", "s3_region", ""),
                                        ("Access Key:", "s3_access_key", ""),
                                        ("Secret Key:", "s3_secret_key", "•"),
                                        ("Endpoint URL:", "s3_endpoint_url", "")]:
                    ttk.Label(fields_outer, text=lbl).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar(value="eu-west-1" if key == "s3_region" else "")
                    field_vars[key] = v
                    kw = {"font": ("Consolas", 9)}
                    if show:
                        kw["show"] = show
                    ttk.Entry(fields_outer, textvariable=v, **kw).pack(fill=tk.X)

            elif stype == StorageType.AZURE.value:
                for lbl, key, show in [("Connection string:", "azure_connection_string", "•"),
                                        ("Container name:", "azure_container", ""),
                                        ("Prefix (optional):", "azure_prefix", "")]:
                    ttk.Label(fields_outer, text=lbl).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar()
                    field_vars[key] = v
                    kw = {"font": ("Consolas", 9)}
                    if show:
                        kw["show"] = show
                    ttk.Entry(fields_outer, textvariable=v, **kw).pack(fill=tk.X)

            elif stype == StorageType.GCS.value:
                for lbl, key, show in [("GCS Bucket:", "gcs_bucket", ""),
                                        ("Prefix (optional):", "gcs_prefix", ""),
                                        ("Credentials JSON path:", "gcs_credentials_path", "")]:
                    ttk.Label(fields_outer, text=lbl).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar()
                    field_vars[key] = v
                    ttk.Entry(fields_outer, textvariable=v, font=("Consolas", 9)).pack(fill=tk.X)

            elif stype == StorageType.PROTON.value:
                for lbl, key, show in [("Proton email:", "proton_username", ""),
                                        ("Password:", "proton_password", "•"),
                                        ("2FA TOTP secret:", "proton_2fa", "•"),
                                        ("Remote folder:", "proton_remote_path", "")]:
                    ttk.Label(fields_outer, text=lbl).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar(value="/Backups" if key == "proton_remote_path" else "")
                    field_vars[key] = v
                    kw = {"font": ("Consolas", 9)}
                    if show:
                        kw["show"] = show
                    ttk.Entry(fields_outer, textvariable=v, **kw).pack(fill=tk.X)

        mirror_type_var.trace_add("write", update_mirror_fields)
        update_mirror_fields()

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=15, pady=(0, 10))

        def on_add():
            cfg_dict = {"storage_type": mirror_type_var.get()}
            for key, var in field_vars.items():
                cfg_dict[key] = var.get()
            self.data["mirrors"].append(cfg_dict)
            self._refresh_wizard_mirrors()
            dialog.destroy()

        ttk.Button(btn_frame, text="Add", command=on_add).pack(side=tk.RIGHT)
        ttk.Button(btn_frame, text="Cancel",
                    command=dialog.destroy).pack(side=tk.RIGHT, padx=(0, 5))

    # ──────────────────────────────────
    #  STEP 5: Mirror Destinations
    # ──────────────────────────────────
    # ── Step 4: Optional mirror destinations (3-2-1 rule) ──
    # Add mirrors for redundancy across different media/locations.
    def _step_mirrors(self) -> tuple[str, str]:
        f = self.content_frame

        self._make_card(f,
            "The 3-2-1 rule recommends keeping at least 3 copies of your data, "
            "on 2 different media types, with 1 copy off-site. Mirror destinations "
            "automatically receive a copy of each backup after it is created on "
            "the primary destination."
        )

        self._make_info(f,
            "💡 This step is optional. You can skip it if you only need one destination. "
            "You can always add mirrors later in the Storage tab of the application."
        )

        # Mirror list
        mirror_cols = ("type", "destination")
        self._wizard_mirror_tree = ttk.Treeview(
            f, columns=mirror_cols, show="headings", height=5)
        self._wizard_mirror_tree.heading("type", text="Type")
        self._wizard_mirror_tree.heading("destination", text="Destination")
        self._wizard_mirror_tree.column("type", width=180)
        self._wizard_mirror_tree.column("destination", width=520)
        self._wizard_mirror_tree.pack(fill=tk.X, pady=(5, 8))

        self._refresh_wizard_mirrors()

        mirror_btns = ttk.Frame(f)
        mirror_btns.pack(fill=tk.X)
        ttk.Button(mirror_btns, text="+ Add a mirror destination...",
                    command=self._wizard_add_mirror).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(mirror_btns, text="✕ Remove selected",
                    command=self._wizard_remove_mirror).pack(side=tk.LEFT)

        # Examples
        examples = tk.Frame(f, bg=COLORS["card_bg"], padx=12, pady=8,
                             relief=tk.SOLID, bd=1)
        examples.pack(fill=tk.X, pady=(15, 0))
        tk.Label(examples, text="Common 3-2-1 setups:",
                 bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9, "bold")).pack(anchor="w")
        tk.Label(examples, bg=COLORS["card_bg"], fg=COLORS["muted"],
                 font=("Segoe UI", 8), wraplength=700, justify=tk.LEFT,
                 text="• Primary: external drive  +  Mirror: cloud S3  (protects against fire/theft)\n"
                      "• Primary: NAS  +  Mirror: SFTP server  (protects against local disaster)\n"
                      "• Primary: external drive  +  Mirror 1: NAS  +  Mirror 2: Proton Drive"
                 ).pack(anchor="w", padx=(10, 0))

        return ("🔄 Mirror Destinations", "3-2-1 Rule (optional)")

    # ──────────────────────────────────
    #  STEP 6: Backup Type
    # ──────────────────────────────────
    # ── Step 5: Full / Incremental / Differential ──
    def _step_backup_type(self) -> tuple[str, str]:
        f = self.content_frame

        self._make_card(f,
            "The backup type determines WHAT IS COPIED each time. "
            "This choice directly impacts backup duration, storage "
            "space used, and ease of restoration."
        )

        type_var = tk.StringVar(value=self.data["backup_type"])

        # Full
        full_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                               relief=tk.SOLID, bd=1)
        full_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(full_frame, text="📦 FULL Backup",
                         variable=type_var, value=BackupType.FULL.value,
                         command=lambda: self.data.update({"backup_type": BackupType.FULL.value})
                         ).pack(anchor="w")
        tk.Label(full_frame, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=640, justify=tk.LEFT,
                 text="Each backup contains ALL your files. This is the safest "
                      "and simplest mode to restore: each backup is self-contained.\n\n"
                      "✅ Pros : easy restore, each backup is independent\n"
                      "❌ Cons : slower, uses more disk space"
                 ).pack(anchor="w", padx=(20, 0), pady=(3, 0))

        # Incremental
        incr_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                               relief=tk.SOLID, bd=1)
        incr_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(incr_frame, text="📊 INCREMENTAL Backup",
                         variable=type_var, value=BackupType.INCREMENTAL.value,
                         command=lambda: self.data.update({"backup_type": BackupType.INCREMENTAL.value})
                         ).pack(anchor="w")
        tk.Label(incr_frame, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=640, justify=tk.LEFT,
                 text="Only MODIFIED or NEW files since the last backup "
                      "are copied. Much faster and space-efficient.\n\n"
                      "✅ Pros : very fast, saves space\n"
                      "❌ Cons : restore requires base backup + ALL increments"
                 ).pack(anchor="w", padx=(20, 0), pady=(3, 0))

        # Differential
        diff_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                               relief=tk.SOLID, bd=1)
        diff_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(diff_frame, text="📈 DIFFERENTIAL Backup",
                         variable=type_var, value=BackupType.DIFFERENTIAL.value,
                         command=lambda: self.data.update({"backup_type": BackupType.DIFFERENTIAL.value})
                         ).pack(anchor="w")
        tk.Label(diff_frame, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=640, justify=tk.LEFT,
                 text="Only files modified since the last FULL backup are copied. "
                      "Grows over time until the next full backup, but restoring "
                      "only needs the last full + last differential.\n\n"
                      "✅ Pros : faster restore than incremental, simpler recovery\n"
                      "❌ Cons : larger than incremental, requires periodic full backups"
                 ).pack(anchor="w", padx=(20, 0), pady=(3, 0))

        self._make_info(f,
            "💡 Recommendation: start with 'Full' for simplicity. "
            "Switch to 'Incremental' or 'Differential' if your backups become too long or large.")

        # Compression option
        compress_var = tk.BooleanVar(value=self.data.get("compress", False))
        ttk.Checkbutton(f, text="📦 Compress backup as ZIP (saves disk space)",
                         variable=compress_var,
                         command=lambda: self.data.update({"compress": compress_var.get()})
                         ).pack(anchor="w", pady=(10, 0))
        tk.Label(f, text="⚠ Compression significantly increases backup time.",
                 font=("Segoe UI", 8), fg="#95a5a6").pack(anchor="w", padx=(20, 0))

        return ("📦 Backup type", "Full, incremental, or differential")

    # ──────────────────────────────────
    #  STEP 5: Retention
    # ──────────────────────────────────
    # ── Step 6: How many backups to keep (Simple or GFS policy) ──
    def _step_retention(self) -> tuple[str, str]:
        f = self.content_frame

        self._make_card(f,
            "The retention policy determines HOW MANY old backups are kept. "
            "The oldest are automatically deleted to free up space. "
            "This is a balance between security (being able to go far back) "
            "and disk space consumed."
        )

        policy_var = tk.StringVar(value=self.data["retention_policy"])

        # Simple
        simple_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                                 relief=tk.SOLID, bd=1)
        simple_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(simple_frame, text="🔢 Simple — Keep the last N",
                         variable=policy_var, value=RetentionPolicy.SIMPLE.value,
                         command=lambda: self.data.update({"retention_policy": RetentionPolicy.SIMPLE.value})
                         ).pack(anchor="w")
        tk.Label(simple_frame, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=640, justify=tk.LEFT,
                 text="Keeps a fixed number of backups. When the limit is reached, "
                      "the oldest is deleted.\n"
                      "Example: keep the last 10 = you can go back "
                      "up to 10 backups back."
                 ).pack(anchor="w", padx=(20, 0), pady=(3, 0))

        simple_row = ttk.Frame(simple_frame)
        simple_row.pack(anchor="w", padx=(20, 0), pady=(5, 0))
        ttk.Label(simple_row, text="Number to keep :").pack(side=tk.LEFT)
        max_var = tk.IntVar(value=self.data["max_backups"])
        max_var.trace_add("write", lambda *a: self.data.update({"max_backups": max_var.get()}))
        ttk.Spinbox(simple_row, from_=1, to=999, textvariable=max_var,
                     width=5).pack(side=tk.LEFT, padx=5)

        # GFS
        gfs_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                              relief=tk.SOLID, bd=1)
        gfs_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(gfs_frame, text="📅 GFS — Grandfather / Father / Son",
                         variable=policy_var, value=RetentionPolicy.GFS.value,
                         command=lambda: self.data.update({"retention_policy": RetentionPolicy.GFS.value})
                         ).pack(anchor="w")
        tk.Label(gfs_frame, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=640, justify=tk.LEFT,
                 text="Professional 3-level strategy. Keeps more recent "
                      "restore points and progressively spaces out in the past :\n\n"
                      "  📅 Daily (Son) — 1 backup per day for the last X days\n"
                      "  📅 Weekly (Father) — 1 backup per week for X weeks\n"
                      "  📅 Monthly (Grandfather) — 1 backup per month for X months\n\n"
                      "Example with 7d + 4w + 12m: you can go back to any "
                      "day of the past week, any week of the past month, "
                      "and any month of the past year."
                 ).pack(anchor="w", padx=(20, 0), pady=(3, 0))

        gfs_grid = ttk.Frame(gfs_frame)
        gfs_grid.pack(anchor="w", padx=(20, 0), pady=(5, 0))
        for i, (label, key, default, max_val) in enumerate([
            ("Days:", "gfs_daily", 7, 365),
            ("Weeks:", "gfs_weekly", 4, 52),
            ("Months:", "gfs_monthly", 12, 120),
        ]):
            ttk.Label(gfs_grid, text=label).grid(row=0, column=i*2, padx=(0, 3))
            var = tk.IntVar(value=self.data[key])
            var.trace_add("write", lambda *a, k=key, v=var: self.data.update({k: v.get()}))
            ttk.Spinbox(gfs_grid, from_=1, to=max_val, textvariable=var,
                         width=4).grid(row=0, column=i*2+1, padx=(0, 10))

        return ("♻ Retention", "How many backups to keep")

    # ──────────────────────────────────
    #  STEP 6: Encryption
    # ──────────────────────────────────
    # ── Step 7: Optional AES-256-GCM encryption ──
    # Password strength is evaluated in real-time.
    def _step_encryption(self) -> tuple[str, str]:
        f = self.content_frame

        self._make_card(f,
            "Encryption protects your backups with a password. Even if someone "
            "accesses the disk or server, they cannot read your files without it."
        )

        mode_var = tk.StringVar(value=self.data["encryption_mode"])

        # Option 1: No encryption
        card1 = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10, relief=tk.SOLID, bd=1)
        card1.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card1, text="🔓 No encryption",
                         variable=mode_var, value="none",
                         command=lambda: [self.data.update({"encryption_mode": "none"}),
                                          self._toggle_pwd_frame(False)]
                         ).pack(anchor="w")
        tk.Label(card1, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=680, justify=tk.LEFT,
                 text="All backups and mirrors are stored in plain text. Fastest option.\n"
                      "✅ Recommended if: local drive at home, non-sensitive data"
                 ).pack(anchor="w", padx=(20, 0))

        # Option 2: Mirrors only
        card2 = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10, relief=tk.SOLID, bd=1)
        card2.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card2, text="🔐 Encrypt mirrors only",
                         variable=mode_var, value="mirrors_only",
                         command=lambda: [self.data.update({"encryption_mode": "mirrors_only"}),
                                          self._toggle_pwd_frame(True)]
                         ).pack(anchor="w")
        tk.Label(card2, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=680, justify=tk.LEFT,
                 text="The primary backup stays plain (fast local restore), but all mirror "
                      "copies are encrypted before upload.\n"
                      "✅ Recommended if: primary on local drive + mirrors on cloud or off-site"
                 ).pack(anchor="w", padx=(20, 0))

        # Option 3: Encrypt everything
        card3 = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10, relief=tk.SOLID, bd=1)
        card3.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card3, text="🔒 Encrypt everything",
                         variable=mode_var, value="all",
                         command=lambda: [self.data.update({"encryption_mode": "all"}),
                                          self._toggle_pwd_frame(True)]
                         ).pack(anchor="w")
        tk.Label(card3, bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9), wraplength=680, justify=tk.LEFT,
                 text="All backups are encrypted — primary destination AND all mirrors.\n"
                      "✅ Recommended if: confidential data, GDPR compliance, shared drives\n"
                      "⚠ WARNING: losing the password = PERMANENT data loss"
                 ).pack(anchor="w", padx=(20, 0))

        # Password entry frame
        self._enc_pwd_frame = ttk.LabelFrame(f, text="Encryption password (16 characters min.)", padding=10)
        self._enc_pwd_frame.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(self._enc_pwd_frame, text="Password:").pack(anchor="w", pady=(0, 2))
        self._wizard_pwd_var = tk.StringVar(value=self.data["encrypt_password"])
        pwd_entry = ttk.Entry(self._enc_pwd_frame, textvariable=self._wizard_pwd_var,
                               show="•", font=("Consolas", 11))
        pwd_entry.pack(fill=tk.X, pady=(0, 3))

        # Counter
        self._wizard_pwd_counter = ttk.Label(self._enc_pwd_frame, text="0 / 16",
                                              font=("Segoe UI", 8))
        self._wizard_pwd_counter.pack(anchor="w")

        def update_pwd_counter(*args):
            n = len(self._wizard_pwd_var.get())
            color = "#e74c3c" if n < 16 else "#27ae60"
            self._wizard_pwd_counter.configure(text=f"{n} / 16 characters", foreground=color)
            self.data["encrypt_password"] = self._wizard_pwd_var.get()
        self._wizard_pwd_var.trace_add("write", update_pwd_counter)
        update_pwd_counter()

        ttk.Label(self._enc_pwd_frame, text="Confirmation:").pack(anchor="w", pady=(5, 2))
        self._wizard_pwd_confirm_var = tk.StringVar()
        ttk.Entry(self._enc_pwd_frame, textvariable=self._wizard_pwd_confirm_var,
                  show="•", font=("Consolas", 11)).pack(fill=tk.X)

        # Show/hide password frame
        self._toggle_pwd_frame(self.data["encryption_mode"] != "none")

        if not self.features.get(FEAT_ENCRYPTION, False):
            self._make_warning(f,
                "⚠ Encryption module (cryptography) is not installed. "
                "You can install it via '📦 Manage modules' in the app.")

        return ("🔐 Encryption", "Data protection")

    def _toggle_pwd_frame(self, show: bool):
        if show:
            self._enc_pwd_frame.pack(fill=tk.X, pady=(8, 0))
        else:
            self._enc_pwd_frame.pack_forget()

    # ──────────────────────────────────
    #  STEP 7: Schedule
    # ──────────────────────────────────
    # ── Step 8: Automatic scheduling (manual / hourly / daily / weekly / monthly) ──
    def _step_schedule(self) -> tuple[str, str]:
        f = self.content_frame

        self._make_card(f,
            "Scheduling automatically runs your backups on a schedule. "
            "You no longer have to think about it: backups run automatically.\n\n"
            "Sans planification, vous devrez lancer chaque backup manuellement "
            "by clicking the '▶ Run backup' button."
        )

        sched_var = tk.BooleanVar(value=self.data["schedule_enabled"])

        # Manual
        man_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                              relief=tk.SOLID, bd=1)
        man_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(man_frame, text="🖱 Manual — I run backups myself",
                         variable=sched_var, value=False,
                         command=lambda: self.data.update({"schedule_enabled": False})
                         ).pack(anchor="w")

        # Automatic
        auto_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                               relief=tk.SOLID, bd=1)
        auto_frame.pack(fill=tk.X, pady=5)
        ttk.Radiobutton(auto_frame, text="🕐 Automatic — Schedule backups",
                         variable=sched_var, value=True,
                         command=lambda: self.data.update({"schedule_enabled": True})
                         ).pack(anchor="w")

        sched_grid = ttk.Frame(auto_frame)
        sched_grid.pack(anchor="w", padx=(20, 0), pady=(8, 0))

        ttk.Label(sched_grid, text="Frequency :").grid(row=0, column=0, sticky="w", pady=3)
        freq_var = tk.StringVar(value=self.data["schedule_freq"])
        freq_var.trace_add("write", lambda *a: self.data.update({"schedule_freq": freq_var.get()}))
        freq_combo = ttk.Combobox(sched_grid, textvariable=freq_var,
                                   values=["daily", "weekly", "monthly"],
                                   state="readonly", width=12)
        freq_combo.grid(row=0, column=1, padx=10, pady=3)

        freq_labels = ttk.Frame(sched_grid)
        freq_labels.grid(row=0, column=2, sticky="w")
        tk.Label(freq_labels, text="daily = every day  |  weekly = every week  |  monthly = every month",
                 fg=COLORS["muted"], font=("Segoe UI", 8)).pack(anchor="w")

        ttk.Label(sched_grid, text="Heure (HH:MM) :").grid(row=1, column=0, sticky="w", pady=3)
        time_var = tk.StringVar(value=self.data["schedule_time"])
        time_var.trace_add("write", lambda *a: self.data.update({"schedule_time": time_var.get()}))
        ttk.Entry(sched_grid, textvariable=time_var, font=("Consolas", 11),
                  width=6).grid(row=1, column=1, sticky="w", padx=10, pady=3)

        self._make_info(f,
            "💡 Recommendation : planifiez vos backups la nuit (ex: 02:00) "
            "to avoid slowing down your computer while you work.")

        return ("🕐 Schedule", "When to back up")

    # ──────────────────────────────────
    #  STEP 9: Email Notifications
    # ──────────────────────────────────
    # ── Step 9: Email notification configuration ──
    # Same 4 radio presets as the Email tab in the main app.
    def _step_email(self) -> tuple[str, str]:
        f = self.content_frame

        self._make_card(f,
            "Receive email reports after scheduled backups — "
            "on success, failure, or both. This is especially useful "
            "for unattended scheduled backups."
        )

        # ── When to send (radio buttons — same as gui.py Email tab) ──
        trigger_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                                  relief=tk.SOLID, bd=1)
        trigger_frame.pack(fill=tk.X, pady=(0, 10))

        tk.Label(trigger_frame, text="When to send emails:",
                 bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(0, 5))

        trigger_var = tk.StringVar(value=self.data["email_trigger"])
        for value, label in [
            ("disabled", "🔕 Disabled — no email notifications"),
            ("failure",  "❌ On failure only — email when a backup fails"),
            ("success",  "✅ On success only — email when a backup succeeds"),
            ("always",   "📧 Always — email after every backup (success or failure)"),
        ]:
            ttk.Radiobutton(trigger_frame, text=label, value=value,
                             variable=trigger_var,
                             command=lambda: self.data.update(
                                 {"email_trigger": trigger_var.get()})
                             ).pack(anchor="w", pady=2)

        # ── SMTP Configuration ──
        smtp_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                               relief=tk.SOLID, bd=1)
        smtp_frame.pack(fill=tk.X, pady=(0, 10))

        tk.Label(smtp_frame, text="SMTP Server:",
                 bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(0, 5))

        def make_row(parent, label, key, width=25, show=""):
            row = tk.Frame(parent, bg=COLORS["card_bg"])
            row.pack(fill=tk.X, pady=2)
            tk.Label(row, text=label, width=12, anchor="e",
                     bg=COLORS["card_bg"], fg=COLORS["text"],
                     font=("Segoe UI", 9)).pack(side=tk.LEFT)
            var = tk.StringVar(value=str(self.data.get(key, "")))
            entry = ttk.Entry(row, textvariable=var, width=width,
                               font=("Consolas", 10))
            if show:
                entry.configure(show=show)
            entry.pack(side=tk.LEFT, padx=(5, 0))
            var.trace_add("write", lambda *a: self.data.update({key: var.get()}))
            return var

        # Row 1: Host
        make_row(smtp_frame, "SMTP Host:", "smtp_host", width=30)

        # Row 2: Port + TLS
        port_row = tk.Frame(smtp_frame, bg=COLORS["card_bg"])
        port_row.pack(fill=tk.X, pady=2)
        tk.Label(port_row, text="Port:", width=12, anchor="e",
                 bg=COLORS["card_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 9)).pack(side=tk.LEFT)
        port_var = tk.IntVar(value=self.data.get("smtp_port", 587))
        ttk.Spinbox(port_row, from_=1, to=65535, width=6,
                      textvariable=port_var).pack(side=tk.LEFT, padx=(5, 10))
        port_var.trace_add("write", lambda *a: self.data.update(
            {"smtp_port": port_var.get()}))
        tls_var = tk.BooleanVar(value=self.data.get("smtp_tls", True))
        ttk.Checkbutton(port_row, text="Use TLS", variable=tls_var).pack(side=tk.LEFT)
        tls_var.trace_add("write", lambda *a: self.data.update(
            {"smtp_tls": tls_var.get()}))

        # Row 3-6: Username, Password, From, To
        make_row(smtp_frame, "Username:", "smtp_user")
        make_row(smtp_frame, "Password:", "smtp_password", show="•")
        make_row(smtp_frame, "From:", "email_from")
        make_row(smtp_frame, "To:", "email_to", width=30)

        self._make_info(f,
            "💡 Gmail: smtp.gmail.com:587 (use app password). "
            "Outlook: smtp.office365.com:587. "
            "You can skip this step and configure email later in the Email tab.")

        return ("📧 Email", "Notifications")

    # ──────────────────────────────────
    #  STEP 10: Summary
    # ──────────────────────────────────
    # ── Step 10: Review configuration and create profile ──
    # Shows a summary of all settings. Checks destination disk space.
    def _step_summary(self) -> tuple[str, str]:
        f = self.content_frame

        tk.Label(f, text="Configuration summary",
                 bg=COLORS["bg"], fg=COLORS["text"],
                 font=("Segoe UI", 12, "bold")).pack(anchor="w", pady=(0, 10))

        d = self.data

        # Build summary
        s3_label = f"☁ S3 : {d['s3_provider']} / {d['s3_bucket']}" if d["s3_bucket"] else "☁ S3 (not configured)"
        storage_labels = {
            StorageType.LOCAL.value: f"💿 Local: {d['dest_path']}",
            StorageType.NETWORK.value: f"🌐 Network : {d['dest_path']}",
            StorageType.SFTP.value: f"🔒 SFTP : {d['sftp_user']}@{d['sftp_host']}:{d['sftp_remote']}",
            StorageType.S3.value: s3_label,
            StorageType.AZURE.value: f"☁ Azure : {d['azure_container']}" if d["azure_container"] else "☁ Azure (not configured)",
            StorageType.GCS.value: f"☁ GCS : {d['gcs_bucket']}" if d["gcs_bucket"] else "☁ GCS (not configured)",
            StorageType.PROTON.value: f"🔒 Proton Drive : {d['proton_username']} ({d['proton_remote_path']})",
        }

        type_labels = {
            BackupType.FULL.value: "📦 Full",
            BackupType.INCREMENTAL.value: "📊 Incremental",
            BackupType.DIFFERENTIAL.value: "📈 Differential",
        }

        if d["retention_policy"] == RetentionPolicy.GFS.value:
            retention_str = f"📅 GFS: {d['gfs_daily']}d + {d['gfs_weekly']}w + {d['gfs_monthly']}m"
        else:
            retention_str = f"🔢 Simple: keep last {d['max_backups']} backups"

        enc_mode = d.get("encryption_mode", "none")
        encrypt_labels = {
            "none":         "🔓 No encryption",
            "mirrors_only": "🔐 Encrypt mirrors only",
            "all":          "🔒 Encrypt everything",
        }
        encrypt_str = encrypt_labels.get(enc_mode, "🔓 No encryption")
        compress_str = "📦 ZIP compressed" if d.get("compress", False) else "📂 No compression"

        if d["schedule_enabled"]:
            sched_str = f"🕐 Automatic — {d['schedule_freq']} at {d['schedule_time']}"
        else:
            sched_str = "🖱 Manual"

        trigger = d.get("email_trigger", "disabled")
        trigger_labels = {
            "disabled": "🔕 Disabled",
            "failure":  "❌ On failure only",
            "success":  "✅ On success only",
            "always":   "📧 Always",
        }
        email_str = trigger_labels.get(trigger, "🔕 Disabled")
        if trigger != "disabled" and d.get("email_to"):
            email_str += f" → {d['email_to']}"

        sources_str = f"{len(d['sources'])} folder(s)"
        if d["sources"]:
            sources_str += " : " + ", ".join(
                Path(s).name for s in d["sources"][:4]
            )
            if len(d["sources"]) > 4:
                sources_str += f"... (+{len(d['sources'])-4})"

        # Mirror summary
        if d["mirrors"]:
            mirror_parts = []
            for m in d["mirrors"]:
                t_str, dest = self._get_mirror_display(m)
                mirror_parts.append(f"{t_str}: {dest}")
            mirror_str = f"🔄 {len(d['mirrors'])} mirror(s): " + " + ".join(mirror_parts)
        else:
            mirror_str = "— None (single destination)"

        lines = [
            ("Profile name",   f"📝 {d['name']}"),
            ("Sources",         f"📂 {sources_str}"),
            ("Destination",     storage_labels.get(d["storage_type"], d["storage_type"])),
            ("Mirrors (3-2-1)", mirror_str),
            ("Backup type",     type_labels.get(d["backup_type"], d["backup_type"])),
            ("Compression",     compress_str),
            ("Retention",       retention_str),
            ("Encryption",      encrypt_str),
            ("Scheduling",      sched_str),
            ("Email",           email_str),
        ]

        summary_frame = tk.Frame(f, bg=COLORS["card_bg"], padx=15, pady=10,
                                  relief=tk.SOLID, bd=1)
        summary_frame.pack(fill=tk.X, pady=5)

        for i, (label, value) in enumerate(lines):
            tk.Label(summary_frame, text=label, bg=COLORS["card_bg"],
                     fg=COLORS["muted"], font=("Segoe UI", 9),
                     width=18, anchor="e").grid(row=i, column=0, sticky="e", pady=2)
            tk.Label(summary_frame, text=value, bg=COLORS["card_bg"],
                     fg=COLORS["text"], font=("Segoe UI", 10),
                     anchor="w").grid(row=i, column=1, sticky="w", padx=(10, 0), pady=2)

        # Disk space check
        source_size = self._estimate_source_size()
        if source_size > 0:
            size_str = StorageBackend.format_size(source_size)
            tk.Label(summary_frame, text="Estimated size", bg=COLORS["card_bg"],
                     fg=COLORS["muted"], font=("Segoe UI", 9),
                     width=18, anchor="e").grid(row=len(lines), column=0, sticky="e", pady=2)
            tk.Label(summary_frame, text=f"📊 ~{size_str}", bg=COLORS["card_bg"],
                     fg=COLORS["text"], font=("Segoe UI", 10),
                     anchor="w").grid(row=len(lines), column=1, sticky="w", padx=(10, 0), pady=2)

        # Check destination space (works for local, network, SFTP, Proton; skips cloud)
        storage_cfg = self._build_temp_storage_config()
        if storage_cfg:
            needed = int(source_size * 1.1) if source_size > 0 else 0
            ok, space_msg = check_destination_space(storage_cfg, needed)
            if ok:
                self._make_info(f, f"💿 Disk space : {space_msg}")
            else:
                self._make_warning(f,
                    f"💿 {space_msg}\n"
                    "Free up space or choose another destination "
                    "before running the backup."
                )

        self._make_info(f,
            "✅ Click 'Create profile and launch' to finish. "
            "You can change all settings at any time in the app."
        )

        return ("✅ Summary", "Final review")

    # ──────────────────────────────────
    #  Disk Space Helpers
    # ──────────────────────────────────
    def _estimate_source_size(self) -> int:
        """Estimate total size of selected source paths."""
        total = 0
        for src in self.data["sources"]:
            p = Path(src)
            if not p.exists():
                continue
            try:
                if p.is_file():
                    total += p.stat().st_size
                else:
                    for f in p.rglob("*"):
                        if f.is_file():
                            try:
                                total += f.stat().st_size
                            except OSError:
                                pass
            except PermissionError:
                pass
        return total

    def _build_temp_storage_config(self):
        """Build a StorageConfig from wizard data for space checking."""
        d = self.data
        cfg = StorageConfig(storage_type=d["storage_type"])
        if d["storage_type"] in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            if d["dest_path"]:
                cfg.destination_path = d["dest_path"]
                return cfg
        elif d["storage_type"] == StorageType.SFTP.value:
            if d["sftp_host"]:
                cfg.sftp_host = d["sftp_host"]
                cfg.sftp_username = d["sftp_user"]
                cfg.sftp_password = d["sftp_password"]
                cfg.sftp_key_path = d["sftp_key_path"]
                cfg.sftp_remote_path = d["sftp_remote"]
                return cfg
        elif d["storage_type"] == StorageType.S3.value:
            if d["s3_bucket"]:
                cfg.s3_bucket = d["s3_bucket"]
                cfg.s3_prefix = d["s3_prefix"]
                cfg.s3_region = d["s3_region"]
                cfg.s3_access_key = d["s3_access_key"]
                cfg.s3_secret_key = d["s3_secret_key"]
                cfg.s3_endpoint_url = d["s3_endpoint"]
                cfg.s3_provider = d["s3_provider"]
                return cfg
        elif d["storage_type"] == StorageType.AZURE.value:
            if d["azure_container"]:
                cfg.azure_connection_string = d["azure_connection_string"]
                cfg.azure_container = d["azure_container"]
                cfg.azure_prefix = d["azure_prefix"]
                return cfg
        elif d["storage_type"] == StorageType.GCS.value:
            if d["gcs_bucket"]:
                cfg.gcs_bucket = d["gcs_bucket"]
                cfg.gcs_prefix = d["gcs_prefix"]
                cfg.gcs_credentials_path = d["gcs_credentials_path"]
                return cfg
        elif d["storage_type"] == StorageType.PROTON.value:
            if d["proton_username"]:
                cfg.proton_username = d["proton_username"]
                cfg.proton_password = d["proton_password"]
                cfg.proton_2fa = d["proton_2fa"]
                cfg.proton_remote_path = d["proton_remote_path"]
                return cfg
        return None

    # ──────────────────────────────────
    #  Build Profile
    # ──────────────────────────────────
    # ── Convert wizard data dict → BackupProfile ──
    # Maps all collected UI data to the proper dataclass fields.
    # Handles storage config, mirrors, encryption, email, schedule.
    def _build_profile(self) -> BackupProfile:
        """Create a BackupProfile from the wizard data."""
        d = self.data

        storage = StorageConfig(storage_type=d["storage_type"])
        if d["storage_type"] in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            storage.destination_path = d["dest_path"]
        elif d["storage_type"] == StorageType.SFTP.value:
            storage.sftp_host = d["sftp_host"]
            storage.sftp_username = d["sftp_user"]
            storage.sftp_password = d["sftp_password"]
            storage.sftp_key_path = d["sftp_key_path"]
            storage.sftp_remote_path = d["sftp_remote"]
        elif d["storage_type"] == StorageType.S3.value:
            storage.s3_bucket = d["s3_bucket"]
            storage.s3_prefix = d["s3_prefix"]
            storage.s3_region = d["s3_region"]
            storage.s3_access_key = d["s3_access_key"]
            storage.s3_secret_key = d["s3_secret_key"]
            storage.s3_endpoint_url = d["s3_endpoint"]
            storage.s3_provider = d["s3_provider"]
        elif d["storage_type"] == StorageType.AZURE.value:
            storage.azure_connection_string = d["azure_connection_string"]
            storage.azure_container = d["azure_container"]
            storage.azure_prefix = d["azure_prefix"]
        elif d["storage_type"] == StorageType.GCS.value:
            storage.gcs_bucket = d["gcs_bucket"]
            storage.gcs_prefix = d["gcs_prefix"]
            storage.gcs_credentials_path = d["gcs_credentials_path"]
        elif d["storage_type"] == StorageType.PROTON.value:
            storage.proton_username = d["proton_username"]
            storage.proton_password = d["proton_password"]
            storage.proton_2fa = d["proton_2fa"]
            storage.proton_remote_path = d["proton_remote_path"]

        schedule = ScheduleConfig(
            enabled=d["schedule_enabled"],
            frequency=d["schedule_freq"],
            time=d["schedule_time"],
        )

        retention = RetentionConfig(
            policy=d["retention_policy"],
            max_backups=d["max_backups"],
            gfs_daily=d["gfs_daily"],
            gfs_weekly=d["gfs_weekly"],
            gfs_monthly=d["gfs_monthly"],
        )

        enc_mode = d.get("encryption_mode", "none")
        encryption = EncryptionConfig(enabled=(enc_mode == "all"))
        if enc_mode != "none" and d["encrypt_password"]:
            encryption.stored_password_b64 = store_password(d["encrypt_password"])

        verification = VerificationConfig(
            auto_verify=True,
        )

        trigger = d.get("email_trigger", "disabled")
        email = EmailConfig(
            enabled=trigger != "disabled",
            smtp_host=d["smtp_host"],
            smtp_port=int(d.get("smtp_port", 587)),
            use_tls=d.get("smtp_tls", True),
            username=d["smtp_user"],
            password=d["smtp_password"],
            from_address=d["email_from"],
            to_address=d["email_to"],
            send_on_success=trigger in ("success", "always"),
            send_on_failure=trigger in ("failure", "always"),
        )

        # Convert mirror dicts to StorageConfig objects
        mirror_configs = []
        for m in d["mirrors"]:
            if isinstance(m, dict):
                mirror_configs.append(StorageConfig(**m))
            else:
                mirror_configs.append(m)

        profile = BackupProfile(
            name=d["name"] or "My backup",
            source_paths=d["sources"],
            backup_type=d["backup_type"],
            compress=d.get("compress", False),
            storage=storage,
            mirror_destinations=mirror_configs,
            schedule=schedule,
            retention=retention,
            encryption=encryption,
            encryption_mode=enc_mode,
            verification=verification,
            email=email,
        )

        return profile

    def run(self) -> Optional[BackupProfile]:
        """Run the wizard. Returns the created profile or None if skipped."""
        self.root.protocol("WM_DELETE_WINDOW", self._skip_wizard)
        if self._parent:
            self._parent.wait_window(self.root)  # Toplevel mode
        else:
            self.root.mainloop()  # Standalone mode
        return self.result_profile
