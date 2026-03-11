"""
Backup Manager - Main Application Window
==========================================
Core BackupManagerApp class. Owns the root Tk window, sidebar, notebook,
status bar, BackupEngine, InAppScheduler, and BackupTray.

Tab UI is delegated to individual tab modules in src/ui/tabs/.
Profile load/save is delegated to each tab's load_profile / collect_config.
"""

import json
import os
import sys
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.core.config import (
    ConfigManager, BackupProfile, StorageConfig, ScheduleConfig,
    RetentionConfig, RetentionPolicy,
    BackupType, StorageType, ScheduleFrequency,
)
from src.core.backup_engine import BackupEngine, BackupStats
from src.storage import get_storage_backend
from src.core.scheduler import InAppScheduler, AutoStart, ScheduleLogEntry
from src.security.encryption import (
    EncryptionConfig, EncryptionAlgorithm, CryptoEngine,
    get_crypto_engine, evaluate_password,
    store_password, retrieve_password,
)
from src.security.verification import (
    VerificationConfig, VerificationEngine,
    VerifyReport, IntegrityManifest, MANIFEST_EXTENSION,
)
from src.notifications.email_notifier import EmailConfig, send_backup_report, send_test_email
try:
    from src.security.integrity_check import verify_integrity, reset_checksums
except Exception:
    def verify_integrity(): return True, "integrity_check unavailable"
    def reset_checksums(): return "unavailable"
try:
    from src.security.secure_memory import secure_clear
except Exception:
    def secure_clear(s): pass
from src.installer import (
    get_available_features, get_unavailable_features_detail,
    check_all,
    FEAT_ENCRYPTION, FEAT_S3, FEAT_AZURE, FEAT_GCS,
    FEAT_SFTP,
)

# System tray (optional — graceful degradation if not installed)
try:
    from src.ui.tray import BackupTray, TrayState, is_tray_available
    HAS_TRAY = is_tray_available()
except Exception:
    HAS_TRAY = False


# ──────────────────────────────────────────────
#  Constants
# ──────────────────────────────────────────────
APP_TITLE = "Backup Manager"
APP_VERSION = "2.2.9"
WINDOW_SIZE = "1120x940"
MIN_SIZE = (1120, 940)

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

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


# ──────────────────────────────────────────────
#  Main Application
# ──────────────────────────────────────────────
class BackupManagerApp:
    """Main application class."""

    def __init__(self, root: Optional[tk.Tk] = None):
        # Use existing root or create new one
        if root:
            self.root = root
        else:
            self.root = tk.Tk()
        self.root.title(f"{APP_TITLE} v{APP_VERSION}")
        self.root.geometry(WINDOW_SIZE)
        self.root.minsize(*MIN_SIZE)
        self.root.resizable(True, True)
        self.root.configure(bg=COLORS["bg"])

        # Window icon (shield matching tray icon)
        try:
            from PIL import Image, ImageDraw, ImageFont, ImageTk
            size = 64
            img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)
            cx, cy = size // 2, size // 2
            points = [
                (cx, 2), (size - 4, cy - 12), (size - 6, cy + 12),
                (cx, size - 2), (6, cy + 12), (4, cy - 12),
            ]
            draw.polygon(points, fill="#3498db", outline="white")
            try:
                font = ImageFont.truetype("arial.ttf", size // 3)
            except (OSError, IOError):
                font = ImageFont.load_default()
            bbox = draw.textbbox((0, 0), "B", font=font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            draw.text((cx - tw // 2, cy - th // 2 - 1), "B",
                      fill="white", font=font)
            self._app_icon = ImageTk.PhotoImage(img)
            self.root.iconphoto(True, self._app_icon)
        except Exception:
            pass

        # Core objects
        self.config = ConfigManager()
        self.engine = BackupEngine(self.config)
        self.profiles: list[BackupProfile] = self.config.get_all_profiles()
        self.current_profile: Optional[BackupProfile] = None
        self._backup_running = False

        # Scheduled backup retry tracking
        self._is_scheduled_run = False
        self._retry_count = 0
        self._retry_profile = None
        self._status_clear_id = None

        # Feature availability (based on installed modules)
        self.features = get_available_features()

        # Scheduler
        self.scheduler = InAppScheduler(self.config, self._scheduled_backup)
        self.scheduler.start()

        # System tray icon
        self.tray = None
        if HAS_TRAY:
            try:
                self.tray = BackupTray(
                    on_show=self._show_from_tray,
                    on_run_backup=lambda: self.root.after(0, self._run_backup),
                    on_quit=lambda: self.root.after(0, self._quit_app),
                    app_version=APP_VERSION,
                )
                self.tray.start()
            except Exception as e:
                print(f"[WARN] System tray unavailable: {e}")
                self.tray = None

        # Build UI
        self._setup_styles()
        self._build_ui()
        self._apply_feature_availability()
        self._refresh_profile_list()

        # Select first profile if exists
        if self.profiles:
            self._select_profile(self.profiles[0])

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _setup_styles(self):
        """Configure ttk styles for a modern look."""
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("TFrame", background=COLORS["bg"])
        style.configure("TLabel", background=COLORS["bg"], foreground=COLORS["text"])
        style.configure("TButton", padding=(12, 6))
        style.configure("Header.TLabel", font=("Segoe UI", 14, "bold"),
                         background=COLORS["bg"], foreground=COLORS["text"])
        style.configure("SubHeader.TLabel", font=("Segoe UI", 10),
                         background=COLORS["bg"], foreground=COLORS["muted"])
        style.configure("Sidebar.TFrame", background=COLORS["sidebar"])
        style.configure("Sidebar.TLabel", background=COLORS["sidebar"],
                         foreground=COLORS["sidebar_fg"], font=("Segoe UI", 10))
        style.configure("SidebarTitle.TLabel", background=COLORS["sidebar"],
                         foreground=COLORS["sidebar_fg"], font=("Segoe UI", 12, "bold"))
        style.configure("Success.TLabel", foreground=COLORS["success"])
        style.configure("Danger.TLabel", foreground=COLORS["danger"])

        style.configure("Accent.TButton", background=COLORS["accent"],
                         foreground="white", font=("Segoe UI", 10, "bold"))
        style.map("Accent.TButton",
                   background=[("active", "#2980b9"), ("disabled", COLORS["muted"])])

        style.configure("Danger.TButton", background=COLORS["danger"],
                         foreground="white")
        style.map("Danger.TButton",
                   background=[("active", "#c0392b")])

        style.configure("Green.Horizontal.TProgressbar",
                         troughcolor="#dfe6e9", background=COLORS["success"])

    # ═══════════════════════════════════════════
    #  Main UI construction: sidebar + notebook (10 tabs) + status bar
    # ═══════════════════════════════════════════
    def _build_ui(self):
        """Build the main application layout."""
        # Import tab modules
        from src.ui.tabs.run_tab import RunTab
        from src.ui.tabs.general_tab import GeneralTab
        from src.ui.tabs.storage_tab import StorageTab
        from src.ui.tabs.mirror_tab import MirrorTab
        from src.ui.tabs.encryption_tab import EncryptionTab
        from src.ui.tabs.retention_tab import RetentionTab
        from src.ui.tabs.schedule_tab import ScheduleTab
        from src.ui.tabs.email_tab import EmailTab
        from src.ui.tabs.history_tab import HistoryTab
        from src.ui.tabs.recovery_tab import RecoveryTab

        # ── Main container ──
        main_pane = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)

        # ── Sidebar ──
        sidebar = tk.Frame(main_pane, bg=COLORS["sidebar"], width=260)
        sidebar.pack_propagate(False)
        main_pane.add(sidebar, weight=0)

        # Sidebar header
        header_frame = tk.Frame(sidebar, bg=COLORS["sidebar"])
        header_frame.pack(fill=tk.X, padx=15, pady=(15, 5))
        tk.Label(header_frame, text=APP_TITLE, bg=COLORS["sidebar"],
                 fg=COLORS["sidebar_fg"], font=("Segoe UI", 14, "bold")).pack(anchor="w")
        tk.Label(header_frame, text=f"v{APP_VERSION}", bg=COLORS["sidebar"],
                 fg=COLORS["muted"], font=("Segoe UI", 9)).pack(anchor="w")

        ttk.Separator(sidebar, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=10, pady=10)

        # Profile list
        tk.Label(sidebar, text='BACKUP PROFILES', bg=COLORS["sidebar"],
                 fg=COLORS["muted"], font=("Segoe UI", 8, "bold")).pack(padx=15, anchor="w")

        self.profile_listbox = tk.Listbox(
            sidebar, bg="#34495e", fg=COLORS["sidebar_fg"],
            selectbackground=COLORS["accent"], selectforeground="white",
            font=("Segoe UI", 10), relief=tk.FLAT, bd=0,
            highlightthickness=0, activestyle="none",
        )
        self.profile_listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.profile_listbox.bind("<<ListboxSelect>>", self._on_profile_select)

        # Sidebar buttons
        btn_frame = tk.Frame(sidebar, bg=COLORS["sidebar"])
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        self.btn_add = ttk.Button(btn_frame, text='+ New profile',
                                   command=self._add_profile)
        self.btn_add.pack(fill=tk.X, pady=2)

        self.btn_delete = ttk.Button(btn_frame, text='🗑 Delete',
                                      command=self._delete_profile, style="Danger.TButton")
        self.btn_delete.pack(fill=tk.X, pady=2)

        # Module manager button (bottom of sidebar)
        ttk.Separator(sidebar, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=10, pady=5)
        modules_frame = tk.Frame(sidebar, bg=COLORS["sidebar"])
        modules_frame.pack(fill=tk.X, padx=10, pady=(0, 10), side=tk.BOTTOM)
        ttk.Button(modules_frame, text='📦 Manage modules',
                    command=self._open_module_manager).pack(fill=tk.X, pady=(0, 3))
        ttk.Button(modules_frame, text='ℹ About',
                    command=self._open_about).pack(fill=tk.X)

        # ── Content Area ──
        self.content_frame = ttk.Frame(main_pane)
        main_pane.add(self.content_frame, weight=1)

        # Status bar (MUST be packed BEFORE notebook — bottom-up allocation)
        self.lbl_status_bar = ttk.Label(
            self.content_frame, text="", font=("Segoe UI", 9),
            anchor="w", padding=(10, 4))
        self.lbl_status_bar.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=(0, 5))

        # Notebook (tabs) — fills remaining space above status bar
        self.notebook = ttk.Notebook(self.content_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=(10, 0))

        # Tab 1: Run / Status
        self.tab_run_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_run_frame, text='  ▶ Run  ')
        self.run_tab = RunTab(self, self.tab_run_frame)

        # Tab 2: General
        self.tab_general_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_general_frame, text='  ⚙ General  ')
        self.general_tab = GeneralTab(self, self.tab_general_frame)

        # Tab 3: Storage
        self.tab_storage_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_storage_frame, text='  💿 Storage  ')
        self.storage_tab = StorageTab(self, self.tab_storage_frame)

        # Tab 4: Mirror Destinations
        self.tab_mirror_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_mirror_frame, text='  🔄 Mirror  ')
        self.mirror_tab = MirrorTab(self, self.tab_mirror_frame)

        # Tab 5: Encryption
        self.tab_encryption_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_encryption_frame, text='  🔐 Encryption  ')
        self.encryption_tab = EncryptionTab(self, self.tab_encryption_frame)

        # Tab 6: Retention Policy
        self.tab_retention_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_retention_frame, text="  ♻ Retention  ")
        self.retention_tab = RetentionTab(self, self.tab_retention_frame)

        # Tab 7: Schedule
        self.tab_schedule_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_schedule_frame, text='  🕐 Schedule  ')
        self.schedule_tab = ScheduleTab(self, self.tab_schedule_frame)

        # Tab 8: Email Notifications
        self.tab_email_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_email_frame, text='  📧 Email  ')
        self.email_tab = EmailTab(self, self.tab_email_frame)

        # Tab 9: History
        self.tab_history_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_history_frame, text='  📋 History  ')
        self.history_tab = HistoryTab(self, self.tab_history_frame)

        # Tab 10: Recovery
        self.tab_recovery_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_recovery_frame, text="  🔄 Recovery  ")
        self.recovery_tab = RecoveryTab(self, self.tab_recovery_frame)

    # ──────────────────────────────────────────
    #  Feature Availability
    # ──────────────────────────────────────────
    def _apply_feature_availability(self):
        """Disable UI elements for features whose modules are not installed."""
        feats = self.features

        # -- Encryption tab --
        if not feats.get(FEAT_ENCRYPTION, False):
            self._disable_widget_tree(self.encryption_tab._encrypt_settings_frame)
            self.encryption_tab.var_encryption_mode.set("none")
            self.encryption_tab.var_encrypt_enabled.set(False)
            self.notebook.tab(self.tab_encryption_frame,
                              text='  🔐 Encryption (unavailable)  ')

        # -- Storage radio buttons --
        for storage_val, (rb_widget, feat_id) in self.storage_tab._storage_radio_buttons.items():
            if feat_id and not feats.get(feat_id, False):
                rb_widget.configure(state=tk.DISABLED)

    @staticmethod
    def _disable_widget_tree(widget):
        """Recursively disable all children of a widget."""
        try:
            widget.configure(state=tk.DISABLED)
        except (tk.TclError, AttributeError):
            pass
        for child in widget.winfo_children():
            BackupManagerApp._disable_widget_tree(child)

    def refresh_features(self):
        """Re-check module availability and update UI."""
        self.features = get_available_features()

        # Re-enable everything first
        for storage_val, (rb_widget, feat_id) in self.storage_tab._storage_radio_buttons.items():
            rb_widget.configure(state=tk.NORMAL)
        self._enable_widget_tree(self.encryption_tab._encrypt_settings_frame)
        self.notebook.tab(self.tab_encryption_frame, text='  🔐 Encryption  ')

        # Then re-apply restrictions
        self._apply_feature_availability()

    @staticmethod
    def _enable_widget_tree(widget):
        """Recursively enable all children of a widget."""
        try:
            widget.configure(state=tk.NORMAL)
        except (tk.TclError, AttributeError):
            pass
        for child in widget.winfo_children():
            BackupManagerApp._enable_widget_tree(child)

    # ──────────────────────────────────────────
    #  Profile Management
    # ──────────────────────────────────────────
    def _refresh_profile_list(self):
        self.profile_listbox.delete(0, tk.END)
        self.profiles = self.config.get_all_profiles()
        for p in self.profiles:
            icons = {
                BackupType.FULL.value: "📦",
                BackupType.INCREMENTAL.value: "📊",
                BackupType.DIFFERENTIAL.value: "📈",
            }
            icon = icons.get(p.backup_type, "📦")
            self.profile_listbox.insert(tk.END, f" {icon}  {p.name}")

    def _on_profile_select(self, event):
        sel = self.profile_listbox.curselection()
        if sel:
            idx = sel[0]
            if idx < len(self.profiles):
                self._select_profile(self.profiles[idx])

    def _select_profile(self, profile: BackupProfile):
        """Load a profile into the editor."""
        self._clear_sensitive_fields()
        self.current_profile = profile
        self._load_profile_to_ui(profile)

    def _load_profile_to_ui(self, p: BackupProfile):
        """Populate all UI fields from profile data — delegates to each tab."""
        # General tab
        self.general_tab.load_profile(p)

        # Retention tab
        self.retention_tab.load_profile(p)

        # Storage tab
        self.storage_tab.load_profile(p)

        # Mirror tab
        self.mirror_tab.load_profile(p)

        # Schedule tab
        self.schedule_tab.load_profile(p)

        # Encryption tab
        self.encryption_tab.load_profile(p)

        # Email tab
        self.email_tab.load_profile(p)

        # Run tab info
        self.run_tab.load_profile(p)

        # History tab
        self.history_tab.load_profile(p)

        # Recovery tab
        self.recovery_tab.load_profile(p)

        # Refresh schedule journal for this profile
        self.schedule_tab._refresh_schedule_journal()

    def _add_profile(self):
        profile = BackupProfile(name="New profile")
        self.config.save_profile(profile)
        self._refresh_profile_list()
        self._select_profile(profile)

    def _delete_profile(self):
        if not self.current_profile:
            return
        confirm = messagebox.askyesno(
            "Confirm deletion",
            f"Delete profile '{self.current_profile.name}'?\n"
            "Existing backups will not be deleted."
        )
        if confirm:
            self.config.delete_profile(self.current_profile.id)
            self.current_profile = None
            self._refresh_profile_list()
            if self.profiles:
                self._select_profile(self.profiles[0])

    def _save_profile(self):
        """Save the current profile from UI fields — delegates to each tab."""
        if not self.current_profile:
            messagebox.showwarning("Warning", "No profile selected.")
            return

        p = self.current_profile

        # Collect from each tab
        self.general_tab.collect_config(p)
        self.retention_tab.collect_config(p)

        # Sources — validate paths exist
        if p.source_paths:
            missing = [s for s in p.source_paths if not Path(s).exists()]
            if missing:
                msg = (f"{len(missing)} source path(s) not found:\n\n"
                       + "\n".join(f"  • {m}" for m in missing[:5]))
                if len(missing) > 5:
                    msg += f"\n  ... and {len(missing) - 5} more"
                msg += "\n\nSave anyway? (paths may be on a disconnected drive)"
                if not messagebox.askyesno("Missing paths", msg):
                    return

        self.storage_tab.collect_config(p)
        self.schedule_tab.collect_config(p)
        self.encryption_tab.collect_config(p)
        self.email_tab.collect_config(p)

        # Verification
        p.verification = VerificationConfig(
            auto_verify=self.email_tab.var_auto_verify.get(),
            alert_on_failure=self.email_tab.var_alert_on_failure.get(),
        )

        self.config.save_profile(p)
        self._refresh_profile_list()
        self._show_status(f"✅ Profile '{p.name}' saved successfully.")

    # ──────────────────────────────────────────
    #  Drive Detection — wait for disconnected local drives
    # ──────────────────────────────────────────
    def _get_missing_local_paths(self) -> list[tuple[str, str]]:
        """Check all local/network destinations. Returns (label, path) for missing drives."""
        missing = []
        if not self.current_profile:
            return missing

        st = self.current_profile.storage
        if st.storage_type in ("local", "network") and st.destination_path:
            root = Path(st.destination_path).anchor
            if root and not Path(root).exists():
                missing.append(("Primary destination", st.destination_path))

        for i, m in enumerate(self.current_profile.mirror_destinations):
            cfg = m if hasattr(m, "storage_type") else StorageConfig(**m)
            if cfg.storage_type in ("local", "network") and cfg.destination_path:
                root = Path(cfg.destination_path).anchor
                if root and not Path(root).exists():
                    missing.append((f"Mirror {i+1}", cfg.destination_path))

        return missing

    def _wait_for_destinations(self) -> bool:
        """If any local destination drive is disconnected, show a waiting dialog."""
        missing = self._get_missing_local_paths()
        if not missing:
            return True

        drive_letters = set()
        details = []
        for label, path in missing:
            root = Path(path).anchor
            drive_letters.add(root.rstrip("\\"))
            details.append(f"  {label}: {path}")
        drives_str = ", ".join(sorted(drive_letters))
        details_str = "\n".join(details)

        dialog = tk.Toplevel(self.root)
        dialog.title("Drive not connected")
        dialog.geometry("500x280")
        dialog.resizable(False, False)
        dialog.transient(self.root)
        dialog.grab_set()

        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() - 500) // 2
        y = (dialog.winfo_screenheight() - 280) // 2
        dialog.geometry(f"500x280+{x}+{y}")

        ttk.Label(dialog, text="💿 Drive not connected",
                  font=("Segoe UI", 14, "bold")).pack(padx=20, pady=(15, 5), anchor="w")

        ttk.Label(dialog,
                  text=f"The backup destination requires drive {drives_str} "
                       f"which is not currently connected.\n\n"
                       f"Please connect the drive. The backup will start automatically.",
                  font=("Segoe UI", 10), wraplength=460, justify=tk.LEFT
                  ).pack(padx=20, anchor="w")

        ttk.Label(dialog, text=details_str,
                  font=("Consolas", 9), foreground="#666666"
                  ).pack(padx=20, pady=(10, 0), anchor="w")

        status_var = tk.StringVar(value="⏳ Waiting for drive...")
        ttk.Label(dialog, textvariable=status_var,
                  font=("Segoe UI", 10)).pack(padx=20, pady=(15, 0), anchor="w")

        result = {"proceed": False, "cancelled": False}

        def close_dialog():
            try:
                dialog.grab_release()
            except Exception:
                pass
            dialog.destroy()

        def on_cancel():
            result["cancelled"] = True
            close_dialog()

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=20, pady=(15, 15))
        ttk.Button(btn_frame, text="Cancel backup",
                    command=on_cancel).pack(side=tk.RIGHT)

        dialog.protocol("WM_DELETE_WINDOW", on_cancel)

        if self.tray:
            self.tray.notify(
                "Drive not connected",
                f"Please connect drive {drives_str} to start the backup."
            )

        dots = [0]

        def check_drive():
            if result["cancelled"]:
                return
            current_missing = self._get_missing_local_paths()
            if not current_missing:
                result["proceed"] = True
                status_var.set("✅ Drive detected! Starting backup...")
                dialog.after(800, close_dialog)
                return
            dots[0] = (dots[0] + 1) % 4
            status_var.set(f"⏳ Waiting for drive{'.' * (dots[0] + 1)}")
            dialog.after(2000, check_drive)

        dialog.after(100, check_drive)
        self.root.wait_window(dialog)

        if result["proceed"] and self.tray:
            self.tray.notify("Drive connected", "Backup starting now.")

        return result["proceed"]

    # ──────────────────────────────────────────
    #  Backup Execution
    # ──────────────────────────────────────────
    def _run_backup(self):
        if not self.current_profile:
            messagebox.showwarning("Warning", "Please select a profile first.")
            return
        if self._backup_running:
            messagebox.showinfo("Info", "A backup is already running.")
            return

        self._save_profile()

        if not self._wait_for_destinations():
            return

        enc_mode = self.current_profile.encryption_mode
        if enc_mode != "none":
            crypto = get_crypto_engine()
            if not crypto.is_available:
                messagebox.showerror(
                    "❌ Encryption unavailable",
                    "Encryption is enabled for this profile but the library "
                    "'cryptography' is not installed on this computer.\n\n"
                    "The backup will NOT be started to avoid creating an "
                    "unencrypted copy.\n\n"
                    "Solutions:\n"
                    "• Install the module: pip install cryptography\n"
                    "• Or set encryption to 'No encryption' in the Encryption tab",
                    parent=self.root,
                )
                return

        if enc_mode != "none":
            password = self._prompt_encryption_password()
            if password is None:
                return
            self.engine.set_encryption_password(password)

        self._backup_running = True
        self.run_tab.btn_run.configure(state=tk.DISABLED)
        self.run_tab.btn_cancel.configure(state=tk.NORMAL)
        self.run_tab.progress_var.set(0)
        self.run_tab._clear_log()

        if self.tray:
            self.tray.set_state(TrayState.BACKUP_RUNNING)
            self.tray.notify(
                "Backup started",
                f"Profile: {self.current_profile.name}"
            )

        # Switch to run tab
        self.notebook.select(self.tab_run_frame)

        self.engine.set_callbacks(
            progress_callback=self._on_progress,
            status_callback=self._on_status,
        )

        thread = threading.Thread(
            target=self._backup_thread, args=(self.current_profile,), daemon=True
        )
        thread.start()

    def _prompt_encryption_password(self) -> Optional[str]:
        """Get the encryption password. Priority: stored > tab > env var > dialog."""
        if not self.current_profile:
            return None

        # 1. Stored password in profile
        stored = retrieve_password(self.current_profile.encryption.stored_password_b64)
        if stored:
            return stored

        # 2. Password in the Encryption tab
        tab_password = self.encryption_tab.var_enc_password.get()
        if tab_password:
            confirm = self.encryption_tab.var_enc_password_confirm.get()
            if tab_password != confirm:
                messagebox.showerror(
                    "Password mismatch",
                    "The password and confirmation do not match.\n"
                    "Please correct them in the Encryption tab."
                )
                return None
            if len(tab_password) < 16:
                messagebox.showerror(
                    "Password too short",
                    f"The password must be at least 16 characters.\n"
                    f"Currently: {len(tab_password)} characters."
                )
                return None
            self.current_profile.encryption.stored_password_b64 = store_password(tab_password)
            self.config.save_profile(self.current_profile)
            return tab_password

        # 3. Environment variable
        env_var = self.current_profile.encryption.key_env_variable
        if env_var:
            env_password = os.environ.get(env_var, "")
            if env_password:
                if len(env_password) < 16:
                    messagebox.showerror(
                        "Password too short",
                        f"Password in environment variable '{env_var}' "
                        f"must be at least 16 characters ({len(env_password)} currently)."
                    )
                    return None
                return env_password

        # 4. Fallback: prompt dialog
        return self._show_password_dialog()

    def _show_password_dialog(self, title="Encryption password",
                                for_restore=False) -> Optional[str]:
        """Show a password entry dialog."""
        dialog = tk.Toplevel(self.root)
        dialog.title(f"🔐 {title}")
        dialog.geometry("480x260")
        dialog.resizable(False, False)
        dialog.transient(self.root)
        dialog.grab_set()

        result = {"password": None}

        if for_restore:
            ttk.Label(dialog, text="Enter the password used to encrypt this backup:",
                      font=("Segoe UI", 10)).pack(padx=20, pady=(20, 5), anchor="w")
        else:
            ttk.Label(dialog, text="Set encryption password (16 characters min.):",
                      font=("Segoe UI", 10)).pack(padx=20, pady=(20, 5), anchor="w")

        pwd_var = tk.StringVar()
        pwd_entry = ttk.Entry(dialog, textvariable=pwd_var, show="•",
                               font=("Consolas", 12), width=35)
        pwd_entry.pack(padx=20, pady=(0, 3))
        pwd_entry.focus_set()

        counter_label = ttk.Label(dialog, text="0 / 16 characters",
                                   font=("Segoe UI", 8))
        counter_label.pack(padx=20, anchor="w")

        def update_counter(*args):
            n = len(pwd_var.get())
            color = COLORS["danger"] if n < 16 else COLORS["success"]
            counter_label.configure(text=f"{n} / 16 characters", foreground=color)
        pwd_var.trace_add("write", update_counter)

        if not for_restore:
            ttk.Label(dialog, text="Confirmation:",
                      font=("Segoe UI", 10)).pack(padx=20, pady=(5, 5), anchor="w")
            confirm_var = tk.StringVar()
            ttk.Entry(dialog, textvariable=confirm_var, show="•",
                      font=("Consolas", 12), width=35).pack(padx=20, pady=(0, 10))
        else:
            confirm_var = pwd_var

        def on_ok():
            if len(pwd_var.get()) < 16:
                messagebox.showwarning("Password too short",
                    f"Minimum 16 characters.\nCurrently: {len(pwd_var.get())}.",
                    parent=dialog)
                return
            if not for_restore and pwd_var.get() != confirm_var.get():
                messagebox.showerror("Error", "Passwords do not match.", parent=dialog)
                return
            result["password"] = pwd_var.get()
            dialog.destroy()

        def on_cancel():
            dialog.destroy()

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=(0, 15))
        ttk.Button(btn_frame, text="OK", command=on_ok,
                    style="Accent.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=on_cancel).pack(side=tk.LEFT, padx=5)

        pwd_entry.bind("<Return>", lambda e: on_ok())
        dialog.protocol("WM_DELETE_WINDOW", on_cancel)
        self.root.wait_window(dialog)
        return result["password"]

    def _backup_thread(self, profile: BackupProfile):
        """Background thread for running backup."""
        try:
            stats = self.engine.run_backup(profile)
            self.root.after(0, self._backup_finished, stats)
        except Exception as e:
            self.root.after(0, self._backup_error, str(e))

    def _backup_finished(self, stats: BackupStats):
        """Called on the main thread when backup completes. Protected against crashes."""
        try:
            self._backup_finished_impl(stats)
        except Exception as e:
            self._append_log(f"\n⚠ Post-backup error (app stays open): {e}")
            self._backup_running = False
            self.run_tab.btn_run.configure(state=tk.NORMAL)
            self.run_tab.btn_cancel.configure(state=tk.DISABLED)

    def _backup_finished_impl(self, stats: BackupStats):
        """Implementation of backup completion handling."""
        self._backup_running = False
        self.run_tab.btn_run.configure(state=tk.NORMAL)
        self.run_tab.btn_cancel.configure(state=tk.DISABLED)
        self.run_tab.progress_var.set(100)

        was_cancelled = self.engine._cancel_requested

        # Update tray icon + notification
        if self.tray:
            if was_cancelled:
                self.tray.set_state(TrayState.IDLE)
                self.tray.notify("Backup cancelled",
                                 f"{stats.files_copied} file(s) copied before cancellation.")
            elif stats.errors:
                self.tray.set_state(TrayState.BACKUP_ERROR)
                self.tray.notify("Backup completed with errors",
                                 f"{stats.files_copied} file(s), {len(stats.errors)} error(s)")
            elif stats.verification_status == "failed":
                self.tray.set_state(TrayState.BACKUP_ERROR)
                self.tray.notify("⚠ Integrity not verified",
                                 f"Backup done but verification failed!")
            else:
                self.tray.set_state(TrayState.BACKUP_SUCCESS)
                self.tray.notify("Backup successful!",
                                 f"{stats.files_copied} file(s) — "
                                 f"{stats.size_str(stats.total_size)} in {stats.duration_str}")

        # Verification status line
        verify_icon = {
            "passed": "✅ PASSED", "warning": "⚠ WARNINGS",
            "failed": "❌ FAILED", "not_run": "⏭ Not run",
            "error": "⛔ ERROR",
        }.get(stats.verification_status, "❓ Unknown")

        verify_detail = ""
        if stats.verification_report:
            r = stats.verification_report
            verify_detail = (
                f"  ({r.verified_ok}/{r.total_files} OK, "
                f"{r.mismatches} mismatches, "
                f"{r.missing} missing, {r.errors} errors, "
                f"duration: {r.duration_str})"
            )

        status_label = "CANCELLED" if was_cancelled else "BACKUP SUMMARY"
        summary = (
            f"\n{'='*50}\n"
            f"  {status_label}\n"
            f"{'='*50}\n"
            f"  Profile       : {stats.profile_name}\n"
            f"  Type         : {stats.backup_type}\n"
            f"  Destination  : {stats.destination}\n"
            f"  Files     : {stats.files_copied} copied, "
            f"{stats.files_skipped} skipped, {stats.files_failed} failed\n"
            f"  Size         : {stats.size_str(stats.total_size)}\n"
            f"  Duration        : {stats.duration_str}\n"
        )

        if not was_cancelled:
            summary += (
                f"  Compression  : {stats.compression_ratio:.1f}%\n"
                f"  Verification : {verify_icon}{verify_detail}\n"
            )

        if stats.errors:
            summary += f"\n  ⚠ {len(stats.errors)} error(s):\n"
            for err in stats.errors[:10]:
                summary += f"    - {err}\n"

        self._append_log(summary)
        self.history_tab._refresh_history()

        # Update schedule journal
        if was_cancelled:
            journal_status = "cancelled"
            journal_detail = f"Cancelled after {stats.files_copied} file(s)"
        elif stats.errors:
            journal_status = "failed"
            journal_detail = f"{len(stats.errors)} error(s) — {stats.errors[0][:60]}"
        else:
            journal_status = "success"
            journal_detail = f"{stats.files_copied} file(s), {stats.size_str(stats.total_size)}"
        self.scheduler.update_journal_status(
            status=journal_status,
            detail=journal_detail,
            files_count=stats.files_copied,
            duration=stats.duration_seconds,
        )
        self.schedule_tab._refresh_schedule_journal()

        # Final dialog / retry logic
        backup_failed = was_cancelled or bool(stats.errors) or stats.verification_status == "failed"

        if backup_failed and self._should_retry():
            self._schedule_retry(
                f"{len(stats.errors)} error(s)" if stats.errors else "verification failed"
            )
            return

        # Send email notification
        email_summary = (
            f"Files: {stats.files_copied} copied, {stats.files_skipped} skipped, "
            f"{stats.files_failed} failed\n"
            f"Size: {stats.size_str(stats.total_size)}\n"
            f"Duration: {stats.duration_str}"
        )
        if stats.verification_report:
            r = stats.verification_report
            email_summary += (
                f"\nVerification: {r.verified_ok}/{r.total_files} OK, "
                f"{r.mismatches} mismatches"
            )
        self._send_backup_email(
            profile_name=stats.profile_name,
            success=not backup_failed,
            summary=email_summary,
            details="\n".join(stats.errors[:10]) if stats.errors else "",
        )

        # Reset scheduled tracking
        self._is_scheduled_run = False
        self._retry_count = 0
        self._retry_profile = None

        if was_cancelled:
            messagebox.showinfo(
                "Backup cancelled",
                f"Backup was cancelled.\n\n"
                f"{stats.files_copied} file(s) copied before cancellation.\n"
                f"Duration: {stats.duration_str}",
                parent=self.root,
            )
        elif stats.verification_status == "failed":
            messagebox.showerror(
                "⚠ INTEGRITY NOT VERIFIED",
                f"Backup completed but verification FAILED!\n\n"
                f"{stats.files_copied} file(s) backed up\n"
                f"Verification: {stats.verification_report.mismatches} file(s) corrupted\n\n"
                "Check the Verification tab for details.",
                parent=self.root,
            )
        elif stats.errors:
            messagebox.showwarning(
                "Backup complete with errors",
                f"{stats.files_copied} file(s) backed up, "
                f"{stats.files_failed} error(s).",
                parent=self.root,
            )
        else:
            verify_msg = ""
            if stats.verification_status == "passed":
                verify_msg = "\n✅ Integrity verified successfully !"
            messagebox.showinfo(
                "Backup successful !",
                f"{stats.files_copied} file(s) backed up "
                f"({stats.size_str(stats.total_size)}) in {stats.duration_str}.{verify_msg}",
                parent=self.root,
            )

    def _backup_error(self, error_msg: str):
        self._backup_running = False
        self.run_tab.btn_run.configure(state=tk.NORMAL)
        self.run_tab.btn_cancel.configure(state=tk.DISABLED)
        self._append_log(f"\n❌ ERROR CRITIQUE: {error_msg}")

        if self.tray:
            self.tray.set_state(TrayState.BACKUP_ERROR)
            self.tray.notify("Backup failed!", error_msg[:200])

        if self._should_retry():
            self._schedule_retry(error_msg[:100])
            return

        self._is_scheduled_run = False
        self._retry_count = 0
        self._retry_profile = None

        profile_name = self.current_profile.name if self.current_profile else "Unknown"
        self._send_backup_email(
            profile_name=profile_name,
            success=False,
            summary=f"Critical error: {error_msg}",
        )

        messagebox.showerror("Error", f"The backup failed:\n{error_msg}",
                             parent=self.root)

    def _cancel_backup(self):
        if self._backup_running:
            self.engine.cancel()
            self.run_tab.btn_cancel.configure(state=tk.DISABLED)
            self.run_tab.lbl_progress.configure(text="⏹ Cancellation requested...")
            self._is_scheduled_run = False
            self._retry_count = 0
            self._retry_profile = None

    # ──────────────────────────────────────────
    #  Automatic Retry (scheduled backups only)
    # ──────────────────────────────────────────
    def _should_retry(self) -> bool:
        if not self._is_scheduled_run or not self._retry_profile:
            return False
        sched = self._retry_profile.schedule
        if not sched.retry_enabled:
            return False
        return self._retry_count < sched.retry_max_attempts

    def _schedule_retry(self, reason: str):
        profile = self._retry_profile
        sched = profile.schedule
        self._retry_count += 1

        delays = sched.retry_delay_minutes or [2, 10, 30]
        delay_index = min(self._retry_count - 1, len(delays) - 1)
        delay_minutes = delays[delay_index]
        delay_ms = delay_minutes * 60 * 1000

        remaining = sched.retry_max_attempts - self._retry_count
        retry_msg = (
            f"⏳ Retry {self._retry_count}/{sched.retry_max_attempts} "
            f"in {delay_minutes} min — {reason}"
        )

        self._append_log(f"\n{retry_msg}")
        self._append_log(
            f"   ({remaining} attempt(s) remaining after this one)"
        )

        self.scheduler.update_journal_status(
            status="retry_pending",
            detail=retry_msg,
        )
        self.schedule_tab._refresh_schedule_journal()

        if self.tray:
            self.tray.notify(
                f"Backup failed — retry in {delay_minutes} min",
                f"{profile.name}: attempt {self._retry_count}/{sched.retry_max_attempts}"
            )

        self.root.after(delay_ms, self._run_retry)

    def _run_retry(self):
        profile = self._retry_profile
        if not profile:
            return

        attempt = self._retry_count
        max_attempts = profile.schedule.retry_max_attempts

        self._append_log(
            f"\n🔄 RETRY {attempt}/{max_attempts} — "
            f"Retrying backup '{profile.name}'..."
        )

        if self.tray:
            self.tray.notify(
                f"Retry {attempt}/{max_attempts}",
                f"Retrying backup '{profile.name}'..."
            )

        self.scheduler.journal.add(ScheduleLogEntry(
                timestamp=datetime.now().isoformat(),
                profile_id=profile.id,
                profile_name=profile.name,
                trigger="retry",
                status="started",
                detail=f"Retry attempt {attempt}/{max_attempts}",
            )
        )
        self.schedule_tab._refresh_schedule_journal()

        self._select_profile(profile)
        self._run_backup()

    def _scheduled_backup(self, profile: BackupProfile):
        """Callback for the in-app scheduler."""
        self.root.after(0, self._run_scheduled, profile)

    def _run_scheduled(self, profile: BackupProfile):
        self._is_scheduled_run = True
        self._retry_count = 0
        self._retry_profile = profile
        self._select_profile(profile)
        self._run_backup()

    # ──────────────────────────────────────────
    #  Progress & Log
    # ──────────────────────────────────────────
    def _on_progress(self, current: int, total: int, filename: str = ""):
        pct = min((current / total * 100) if total else 0, 100)
        self.root.after(0, lambda: self.run_tab.update_progress(current, total, filename))

    def _on_status(self, message: str):
        self.root.after(0, lambda: self.run_tab.lbl_progress.configure(text=message))
        self.root.after(0, lambda: self._append_log(message))

    def _show_status(self, message: str, duration: int = 3000):
        """Show a temporary status message in the status bar."""
        self.lbl_status_bar.configure(text=message, foreground=COLORS["success"])
        if hasattr(self, '_status_clear_id') and self._status_clear_id:
            self.root.after_cancel(self._status_clear_id)
        self._status_clear_id = self.root.after(
            duration, lambda: self.lbl_status_bar.configure(text=""))

    def _append_log(self, text: str):
        """Append text to the run tab log."""
        self.run_tab._append_log(text)

    def _clear_log(self):
        self.run_tab._clear_log()

    # ──────────────────────────────────────────
    #  Email Notifications
    # ──────────────────────────────────────────
    def _send_backup_email(self, profile_name: str, success: bool,
                           summary: str, details: str = ""):
        """Send backup report email in a background thread."""
        if not self.current_profile:
            return
        email_config = self.email_tab._build_email_config()
        if not email_config.enabled:
            return

        def _send():
            try:
                send_backup_report(email_config, profile_name, success, summary, details)
            except Exception as e:
                self.root.after(0, lambda: self._append_log(f"⚠ Email error: {e}"))

        threading.Thread(target=_send, daemon=True).start()

    # ──────────────────────────────────────────
    #  Security: zero passwords in memory
    # ──────────────────────────────────────────
    def _clear_sensitive_fields(self):
        """Clear all password/credential fields from memory."""
        sensitive_vars = [
            ('encryption_tab', 'var_enc_password'),
            ('encryption_tab', 'var_enc_password_confirm'),
            ('email_tab', 'var_smtp_password'),
            ('storage_tab', 'var_s3_secret_key'),
            ('storage_tab', 'var_s3_access_key'),
            ('storage_tab', 'var_azure_conn'),
            ('storage_tab', 'var_sftp_password'),
            ('storage_tab', 'var_proton_password'),
            ('recovery_tab', 'var_restore_password'),
        ]
        for tab_name, var_name in sensitive_vars:
            tab = getattr(self, tab_name, None)
            if tab:
                var = getattr(tab, var_name, None)
                if var and isinstance(var, tk.StringVar):
                    old_value = var.get()
                    if old_value:
                        secure_clear(old_value)
                        var.set("")

    # ──────────────────────────────────────────
    #  About
    # ──────────────────────────────────────────
    def _open_about(self):
        dialog = tk.Toplevel(self.root)
        dialog.title(f"About — Backup Manager v{APP_VERSION}")
        dialog.resizable(False, False)
        dialog.transient(self.root)
        dialog.grab_set()

        dialog.update_idletasks()
        w, h = 480, 280
        x = self.root.winfo_x() + (self.root.winfo_width() - w) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - h) // 2
        dialog.geometry(f"{w}x{h}+{x}+{y}")

        frame = tk.Frame(dialog, padx=25, pady=20)
        frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(frame, text=f"💾 Backup Manager v{APP_VERSION}",
                 font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(0, 10))

        info = (
            "Backup Manager is a free and open-source application.\n"
            "It is licensed under the MIT License.\n\n"
            "Source code:\n"
            "https://github.com/loicata/backup-manager\n\n"
            "Developed by Loïc Ader\n"
            "loic@loicata.com"
        )
        tk.Label(frame, text=info, font=("Segoe UI", 10),
                 justify=tk.LEFT, wraplength=430).pack(anchor="w")

        ttk.Button(frame, text="Close", command=dialog.destroy).pack(
            anchor="e", pady=(15, 0))

    # ──────────────────────────────────────────
    #  Module Manager
    # ──────────────────────────────────────────
    def _open_module_manager(self):
        from src.installer import check_all, is_frozen

        dialog = tk.Toplevel(self.root)
        dialog.title("📦 Module Manager — Backup Manager")
        dialog.geometry("650x490")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="📦 Python Modules",
                  font=("Segoe UI", 13, "bold")).pack(padx=15, pady=(10, 5), anchor="w")

        if is_frozen():
            info_frame = tk.Frame(dialog, bg="#d5f5e3", padx=12, pady=6)
            info_frame.pack(fill=tk.X, padx=15, pady=(0, 5))
            tk.Label(info_frame,
                     text="Running as standalone .exe — all bundled modules are shown below.\n"
                          "Module installation is not available in .exe mode.",
                     bg="#d5f5e3", fg="#1e8449", font=("Segoe UI", 9),
                     justify=tk.LEFT).pack(anchor="w")

        installed, missing = check_all()

        columns = ("module", "description", "status")
        tree = ttk.Treeview(dialog, columns=columns, show="headings", height=14)
        tree.heading("module", text="Module")
        tree.heading("description", text="Description")
        tree.heading("status", text="Status")
        tree.column("module", width=180)
        tree.column("description", width=280)
        tree.column("status", width=130)
        tree.pack(fill=tk.BOTH, expand=True, padx=15, pady=5)

        for dep in installed:
            tree.insert("", tk.END, values=(dep.pip_name, dep.description, "✅ Installed"),
                         tags=("installed",))
        for dep in missing:
            tree.insert("", tk.END, values=(dep.pip_name, dep.description, "❌ Manquant"),
                         tags=("missing",))

        tree.tag_configure("installed", foreground="#27ae60")
        tree.tag_configure("missing", foreground="#e74c3c")

        status_label = ttk.Label(dialog,
                                  text=f"{len(installed)} installed, {len(missing)} missing",
                                  font=("Segoe UI", 9))
        status_label.pack(padx=15, anchor="w")

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=15, pady=10)
        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT)

    # ──────────────────────────────────────────
    #  App Lifecycle
    # ──────────────────────────────────────────
    def _auto_start_first_backup(self):
        """Called after wizard: select first profile, switch to Run tab, start backup."""
        self._refresh_profile_list()
        if self.profiles:
            self.profile_listbox.selection_set(0)
            self._select_profile(self.profiles[0])
            self.notebook.select(self.tab_run_frame)
            self.root.after(300, self._run_backup)

    def _on_close(self):
        if self.tray:
            self._minimize_to_tray()
        else:
            self._quit_app()

    def _minimize_to_tray(self):
        self.root.withdraw()
        if self.tray and not self._backup_running:
            self.tray.notify(
                "Backup Manager",
                "The application is still running in the notification area."
            )

    def _show_from_tray(self):
        self.root.after(0, self._restore_window)

    def _restore_window(self):
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()
        self.root.state("normal")

    def _quit_app(self):
        if self._backup_running:
            if not messagebox.askyesno(
                "Backup running",
                "A backup is currently running.\n"
                "Are you sure you want to quit?",
                parent=self.root,
            ):
                return
            self.engine.cancel()

        if self.tray:
            self.tray.stop()
            self.tray = None

        self.scheduler.stop()
        self._clear_sensitive_fields()
        self.root.destroy()

    def run(self):
        """Start the application."""
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self._quit_app()
