"""
Backup Manager - Graphical User Interface
==========================================
Main application window. Entry point: python gui.py

Architecture:
  BackupManagerApp (single instance)
    ├── root (tk.Tk)         — single persistent Tk root for entire app lifetime
    ├── sidebar              — profile list + action buttons
    ├── notebook (10 tabs)   — Run, General, Storage, Mirror, Encryption,
    │                          Retention, Schedule, Email, History, Recovery
    ├── status_bar           — save confirmation, visible from all tabs
    ├── BackupEngine         — runs backups in background thread
    ├── InAppScheduler       — checks every 30s for scheduled backups
    ├── BackupTray           — system tray icon (optional, pystray)
    └── crash.log            — written on any startup exception

Startup sequence (entry point at bottom of file):
  1. DPI awareness (Windows)
  2. Create Tk root with splash "Starting..."
  3. auto_install_all() — install missing pip packages (Toplevel dialog)
  4. SetupWizard — first-launch 11-step wizard (Toplevel dialog)
  5. verify_integrity() — SHA-256 checksums of app files
  6. BackupManagerApp(root=_root) — takes over the root window

Threading model:
  - Backups run in daemon threads (_backup_thread)
  - All UI updates via root.after(0, callback) from background threads
  - Scheduler runs in its own daemon thread
  - Tray icon runs in its own daemon thread
  - Email sending runs in daemon threads

Key safety rules:
  - messagebox calls MUST include parent=self.root (otherwise kills mainloop)
  - _backup_finished wrapped in try/except (post-backup errors don't crash app)
  - Sensitive fields cleared from memory on profile switch and app quit
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

from config import (
    ConfigManager, BackupProfile, StorageConfig, ScheduleConfig,
    RetentionConfig, RetentionPolicy,
    BackupType, StorageType, ScheduleFrequency,
)
from backup_engine import BackupEngine, BackupStats
from storage import get_storage_backend
from scheduler import InAppScheduler, AutoStart, ScheduleLogEntry
from encryption import (
    EncryptionConfig, EncryptionAlgorithm, CryptoEngine,
    get_crypto_engine, evaluate_password,
    store_password, retrieve_password,
)
from verification import (
    VerificationConfig, VerificationEngine,
    VerifyReport, IntegrityManifest, MANIFEST_EXTENSION,
)
from email_notifier import EmailConfig, send_backup_report, send_test_email
try:
    from integrity_check import verify_integrity, reset_checksums
except Exception:
    def verify_integrity(): return True, "integrity_check unavailable"
    def reset_checksums(): return "unavailable"
try:
    from secure_memory import secure_clear
except Exception:
    def secure_clear(s): pass
from installer import (
    get_available_features, get_unavailable_features_detail,
    check_all,
    FEAT_ENCRYPTION, FEAT_S3, FEAT_AZURE, FEAT_GCS,
    FEAT_SFTP,
)

# System tray (optional — graceful degradation if not installed)
try:
    from tray import BackupTray, TrayState, is_tray_available
    HAS_TRAY = is_tray_available()
except Exception:
    HAS_TRAY = False


# ──────────────────────────────────────────────
#  Constants
# ──────────────────────────────────────────────
APP_TITLE = "Backup Manager"
APP_VERSION = "2.2.8"
WINDOW_SIZE = "1120x920"
MIN_SIZE = (1120, 920)

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
        self._is_scheduled_run = False       # True if current backup was triggered by scheduler
        self._retry_count = 0                # Current retry attempt (0 = first run)
        self._retry_profile = None           # Profile being retried
        self._status_clear_id = None         # Timer ID for status bar auto-clear

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
        tk.Label(header_frame, text="💾 " + APP_TITLE, bg=COLORS["sidebar"],
                 fg=COLORS["sidebar_fg"], font=("Segoe UI", 14, "bold")).pack(anchor="w")
        tk.Label(header_frame, text=f"v{APP_VERSION}", bg=COLORS["sidebar"],
                 fg=COLORS["muted"], font=("Segoe UI", 9)).pack(anchor="w")

        ttk.Separator(sidebar, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=10, pady=10)

        # Profilee list
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

        # Tab 1: Run / Status (first for quick access)
        self.tab_run = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_run, text='  ▶ Run  ')
        self._build_run_tab()

        # Tab 2: General
        self.tab_general = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_general, text='  ⚙ General  ')
        self._build_general_tab()

        # Tab 3: Storage
        self.tab_storage = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_storage, text='  💿 Storage  ')
        self._build_storage_tab()

        # Tab 4: Mirror Destinations
        self.tab_mirror = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_mirror, text='  🔄 Mirror  ')
        self._build_mirror_tab()

        # Tab 4: Encryption
        self.tab_encryption = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_encryption, text='  🔐 Encryption  ')
        self._build_encryption_tab()

        # Tab 5: Retention Policy
        self.tab_retention = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_retention, text="  ♻ Retention  ")
        self._build_retention_tab()

        # Tab 6: Schedule
        self.tab_schedule = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_schedule, text='  🕐 Schedule  ')
        self._build_schedule_tab()

        # Tab 7: Email Notifications
        self.tab_email = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_email, text='  📧 Email  ')
        self._build_email_tab()

        # Tab 8: History
        self.tab_history = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_history, text='  📋 History  ')
        self._build_history_tab()

        # Tab 9: Recovery
        self.tab_recovery = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_recovery, text="  🔄 Recovery  ")
        self._build_recovery_tab()

    # ──────────────────────────────────────────
    #  Feature Availability
    # ──────────────────────────────────────────
    def _apply_feature_availability(self):
        """
        Disable and gray out UI elements for features whose required
        modules are not installed. Called once at startup.
        """
        feats = self.features

        # -- Encryption tab --
        if not feats.get(FEAT_ENCRYPTION, False):
            self._disable_widget_tree(self._encrypt_settings_frame)
            self.var_encryption_mode.set("none")
            self.var_encrypt_enabled.set(False)
            self.notebook.tab(self.tab_encryption,
                              text='  🔐 Encryption (unavailable)  ')

        # -- Storage radio buttons --
        for storage_val, (rb_widget, feat_id) in self._storage_radio_buttons.items():
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
        """
        Re-check module availability and update UI.
        Called after installing modules via the module manager.
        """
        self.features = get_available_features()

        # Re-enable everything first
        for storage_val, (rb_widget, feat_id) in self._storage_radio_buttons.items():
            rb_widget.configure(state=tk.NORMAL)
        self._encrypt_checkbox.configure(state=tk.NORMAL)
        self._enable_widget_tree(self._encrypt_settings_frame)
        self.notebook.tab(self.tab_encryption, text='  🔐 Encryption  ')

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
    #  TAB: General
    # ──────────────────────────────────────────
    def _build_general_tab(self):
        container = ttk.Frame(self.tab_general)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        # Profilee name
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
                  text="⚠ Compression significantly increases backup time.",
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
        ttk.Button(src_btn_frame, text="📁 Add folder",
                    command=self._add_source_folder).pack(side=tk.LEFT, padx=(0, 3))
        ttk.Button(src_btn_frame, text="🗂 Multiple selection...",
                    command=self._open_multi_folder_dialog).pack(side=tk.LEFT, padx=(0, 3))
        ttk.Button(src_btn_frame, text="📄 Add files",
                    command=self._add_source_file).pack(side=tk.LEFT, padx=(0, 3))
        ttk.Button(src_btn_frame, text="⬆", width=3,
                    command=self._move_source_up).pack(side=tk.LEFT, padx=(10, 1))
        ttk.Button(src_btn_frame, text="⬇", width=3,
                    command=self._move_source_down).pack(side=tk.LEFT, padx=(0, 3))
        ttk.Button(src_btn_frame, text="✕ Remove selected",
                    command=self._remove_source).pack(side=tk.LEFT, padx=(10, 3))
        ttk.Button(src_btn_frame, text="🗑 Clear all",
                    command=self._clear_sources).pack(side=tk.LEFT)

        # Exclusions
        ttk.Label(container, text="Exclusion patterns (one per line)").grid(
            row=9, column=0, sticky="w", pady=(10, 3))
        self.exclude_text = tk.Text(container, font=("Consolas", 9), height=3,
                                     relief=tk.SOLID, bd=1, wrap=tk.WORD)
        self.exclude_text.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(0, 10))

        # Save button
        ttk.Button(container, text='💾 Save profile',
                    command=self._save_profile, style="Accent.TButton").grid(
            row=11, column=0, columnspan=2, sticky="e", pady=(20, 0))

        container.columnconfigure(0, weight=1)

    # ──────────────────────────────────────────
    #  TAB: Retention Policy
    # ──────────────────────────────────────────
    def _build_retention_tab(self):
        container = ttk.Frame(self.tab_retention)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text="Retention policy",
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))

        # Explanation
        info = tk.Frame(container, bg="#f0f4f8", padx=15, pady=10, relief=tk.SOLID, bd=1)
        info.pack(fill=tk.X, pady=(0, 10))
        tk.Label(info, bg="#f0f4f8", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="The retention policy determines HOW MANY old backups are kept.\n"
                      "The oldest are automatically deleted to free up space.\n"
                      "This is a balance between security (being able to go far back) "
                      "and disk space consumed."
                 ).pack(anchor="w")

        # Variables
        self.var_retention_policy = tk.StringVar(value=RetentionPolicy.SIMPLE.value)
        self.var_max_backups = tk.IntVar(value=10)
        self.var_gfs_daily = tk.IntVar(value=7)
        self.var_gfs_weekly = tk.IntVar(value=4)
        self.var_gfs_monthly = tk.IntVar(value=12)

        # ── Simple option ──
        simple_card = tk.Frame(container, bg="white", padx=15, pady=12,
                                relief=tk.SOLID, bd=1)
        simple_card.pack(fill=tk.X, pady=5)

        ttk.Radiobutton(simple_card, text="🔢 Simple — Keep the last N backups",
                         variable=self.var_retention_policy,
                         value=RetentionPolicy.SIMPLE.value).pack(anchor="w")
        tk.Label(simple_card, bg="white", fg="#7f8c8d",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="Keeps a fixed number of backups. When the limit is reached, "
                      "the oldest is deleted.\n"
                      "Example: keep the last 10 = you can go back up to 10 backups."
                 ).pack(anchor="w", padx=(20, 0), pady=(2, 5))

        simple_row = ttk.Frame(simple_card)
        simple_row.pack(anchor="w", padx=(20, 0))
        ttk.Label(simple_row, text="Number to keep:").pack(side=tk.LEFT)
        ttk.Spinbox(simple_row, from_=1, to=999, textvariable=self.var_max_backups,
                     width=6).pack(side=tk.LEFT, padx=5)

        # ── GFS option ──
        gfs_card = tk.Frame(container, bg="white", padx=15, pady=12,
                             relief=tk.SOLID, bd=1)
        gfs_card.pack(fill=tk.X, pady=5)

        ttk.Radiobutton(gfs_card, text="📅 GFS — Grandfather / Father / Son",
                         variable=self.var_retention_policy,
                         value=RetentionPolicy.GFS.value).pack(anchor="w")
        tk.Label(gfs_card, bg="white", fg="#7f8c8d",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="Professional 3-level strategy. Keeps more recent restore points "
                      "and progressively spaces out in the past:\n\n"
                      "  📅 Daily (Son) — 1 backup per day for the last X days\n"
                      "  📅 Weekly (Father) — 1 backup per week for X weeks\n"
                      "  📅 Monthly (Grandfather) — 1 backup per month for X months\n\n"
                      "Example with 7d + 4w + 12m: you can go back to any day of the "
                      "past week, any week of the past month, and any month of the past year."
                 ).pack(anchor="w", padx=(20, 0), pady=(2, 8))

        gfs_grid = ttk.Frame(gfs_card)
        gfs_grid.pack(anchor="w", padx=(20, 0), pady=(0, 5))

        ttk.Label(gfs_grid, text="📅 Days:").grid(row=0, column=0, padx=(0, 3))
        ttk.Spinbox(gfs_grid, from_=1, to=365, textvariable=self.var_gfs_daily,
                     width=5).grid(row=0, column=1, padx=(0, 20))
        ttk.Label(gfs_grid, text="📅 Weeks:").grid(row=0, column=2, padx=(0, 3))
        ttk.Spinbox(gfs_grid, from_=1, to=52, textvariable=self.var_gfs_weekly,
                     width=5).grid(row=0, column=3, padx=(0, 20))
        ttk.Label(gfs_grid, text="📅 Months:").grid(row=0, column=4, padx=(0, 3))
        ttk.Spinbox(gfs_grid, from_=1, to=120, textvariable=self.var_gfs_monthly,
                     width=5).grid(row=0, column=5)

        # GFS live summary
        self.lbl_gfs_tab_summary = ttk.Label(gfs_card, text="",
                                              font=("Segoe UI", 10, "bold"))
        self.lbl_gfs_tab_summary.pack(anchor="w", padx=(20, 0), pady=(3, 0))

        def update_gfs_summary(*args):
            try:
                d = self.var_gfs_daily.get()
                w = self.var_gfs_weekly.get()
                m = self.var_gfs_monthly.get()
                total = d + w + m
                self.lbl_gfs_tab_summary.configure(
                    text=f"💡 Up to ~{total} backups kept ({d}d + {w}w + {m}m)")
            except (tk.TclError, ValueError):
                pass

        self.var_gfs_daily.trace_add("write", update_gfs_summary)
        self.var_gfs_weekly.trace_add("write", update_gfs_summary)
        self.var_gfs_monthly.trace_add("write", update_gfs_summary)
        update_gfs_summary()

        # Save button
        ttk.Button(container, text='💾 Save',
                    command=self._save_profile, style="Accent.TButton").pack(
            anchor="e", pady=(15, 0))

    # ──────────────────────────────────────────
    #  TAB: Storage
    # ──────────────────────────────────────────
    def _build_storage_tab(self):
        container = ttk.Frame(self.tab_storage)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Storage destination',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 10))

        # Storage type selector — scrollable frame for many options
        type_frame = ttk.LabelFrame(container, text="Storage type", padding=10)
        type_frame.pack(fill=tk.X, pady=(0, 10))

        self.var_storage_type = tk.StringVar(value=StorageType.LOCAL.value)

        # All storage options in a single column
        all_options = [
            (StorageType.LOCAL.value,   "💿 External drive / USB stick",  None),
            (StorageType.NETWORK.value, "🌐 Network folder (UNC)",        None),
            (StorageType.SFTP.value,    "🔒 SFTP (SSH)",                  FEAT_SFTP),
            (StorageType.S3.value,      "☁ Amazon S3 / S3-compatible",           FEAT_S3),
            (StorageType.AZURE.value,   "☁ Azure Blob Storage",           FEAT_AZURE),
            (StorageType.GCS.value,     "☁ Google Cloud Storage",          FEAT_GCS),
            (StorageType.PROTON.value,  "🔒 Proton Drive",                None),
        ]

        self._storage_radio_buttons = {}

        for val, label, feat_id in all_options:
            rb = ttk.Radiobutton(
                type_frame, text=label, variable=self.var_storage_type,
                value=val, command=self._update_storage_fields,
            )
            rb.pack(anchor="w", pady=1)
            self._storage_radio_buttons[val] = (rb, feat_id)

        # Dynamic fields container
        self.storage_fields_frame = ttk.LabelFrame(container, text="Configuration", padding=10)
        self.storage_fields_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # All field variables — existing
        self.var_dest_path = tk.StringVar()
        self.var_s3_bucket = tk.StringVar()
        self.var_s3_prefix = tk.StringVar()
        self.var_s3_region = tk.StringVar(value="eu-west-1")
        self.var_s3_access_key = tk.StringVar()
        self.var_s3_secret_key = tk.StringVar()
        self.var_s3_endpoint = tk.StringVar()
        self.var_s3_provider = tk.StringVar(value="aws")
        self.var_azure_conn = tk.StringVar()
        self.var_azure_container = tk.StringVar()
        self.var_azure_prefix = tk.StringVar()
        # SFTP / FTP
        self.var_sftp_host = tk.StringVar()
        self.var_sftp_port = tk.IntVar(value=22)
        self.var_sftp_username = tk.StringVar()
        self.var_sftp_password = tk.StringVar()
        self.var_sftp_key_path = tk.StringVar()
        self.var_sftp_remote_path = tk.StringVar(value="/backups")
        # GCS
        self.var_gcs_bucket = tk.StringVar()
        self.var_gcs_prefix = tk.StringVar()
        self.var_gcs_credentials = tk.StringVar()
        # Proton Drive
        self.var_proton_username = tk.StringVar()
        self.var_proton_password = tk.StringVar()
        self.var_proton_2fa = tk.StringVar()
        self.var_proton_remote_path = tk.StringVar(value="/Backups")
        self.var_proton_rclone_path = tk.StringVar()

        self._update_storage_fields()

        # ── Bandwidth Limit ──
        bw_frame = ttk.LabelFrame(container, text="Bandwidth limit", padding=10)
        bw_frame.pack(fill=tk.X, pady=(10, 5))

        bw_row = ttk.Frame(bw_frame)
        bw_row.pack(fill=tk.X)
        self.var_bandwidth_limit = tk.IntVar(value=0)
        ttk.Label(bw_row, text="Max upload speed:").pack(side=tk.LEFT)
        ttk.Spinbox(bw_row, from_=0, to=1000000, width=8,
                      textvariable=self.var_bandwidth_limit).pack(side=tk.LEFT, padx=(5, 5))
        ttk.Label(bw_row, text="KB/s   (0 = unlimited)",
                  font=("Segoe UI", 9)).pack(side=tk.LEFT, padx=(0, 15))

        # Buttons
        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X)
        ttk.Button(btn_frame, text='🔌 Test connection',
                    command=self._test_storage).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_frame, text='💾 Save',
                    command=self._save_profile, style="Accent.TButton").pack(side=tk.RIGHT)

    def _add_field(self, frame, label: str, var, show: str = "", browse: str = ""):
        """Helper to add a labeled entry field to a frame."""
        ttk.Label(frame, text=label).pack(anchor="w", pady=(5, 0))
        entry_frame = ttk.Frame(frame)
        entry_frame.pack(fill=tk.X, pady=(2, 0))
        kwargs = {"font": ("Consolas", 10)}
        if show:
            kwargs["show"] = show
        ttk.Entry(entry_frame, textvariable=var, **kwargs).pack(
            side=tk.LEFT, fill=tk.X, expand=True)
        if browse:
            ttk.Button(entry_frame, text="Browse...",
                        command=lambda: self._browse_for_var(var, browse)
                        ).pack(side=tk.RIGHT, padx=(5, 0))

    def _browse_for_var(self, var: tk.StringVar, mode: str):
        """Browse for a file or directory and set the variable."""
        if mode == "dir":
            path = filedialog.askdirectory(title="Choose a folder")
        else:
            path = filedialog.askopenfilename(
                title="Choose a file",
                filetypes=[(mode, mode), ("All files", "*.*")],
            )
        if path:
            var.set(path)

    def _update_storage_fields(self):
        """Update visible storage fields based on selected type."""
        for widget in self.storage_fields_frame.winfo_children():
            widget.destroy()

        stype = self.var_storage_type.get()
        frame = self.storage_fields_frame

        if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            label = "Network path (e.g. \\\\server\\share)" if stype == StorageType.NETWORK.value else "Destination path"
            ttk.Label(frame, text=label).pack(anchor="w")
            path_frame = ttk.Frame(frame)
            path_frame.pack(fill=tk.X, pady=(3, 0))
            ttk.Entry(path_frame, textvariable=self.var_dest_path,
                      font=("Consolas", 10)).pack(side=tk.LEFT, fill=tk.X, expand=True)
            if stype == StorageType.LOCAL.value:
                ttk.Button(path_frame, text="Browse...",
                           command=self._browse_dest).pack(side=tk.RIGHT, padx=(5, 0))

        elif stype == StorageType.S3.value:
            # S3 provider selector
            ttk.Label(frame, text="S3 Provider").pack(anchor="w", pady=(5, 0))
            provider_frame = ttk.Frame(frame)
            provider_frame.pack(fill=tk.X, pady=(2, 0))

            s3_providers = [
                ("aws",          "Amazon AWS S3"),
                ("minio",        "MinIO (self-hosted)"),
                ("wasabi",       "Wasabi"),
                ("ovh",          "OVH Object Storage"),
                ("scaleway",     "Scaleway Object Storage"),
                ("digitalocean", "DigitalOcean Spaces"),
                ("cloudflare",   "Cloudflare R2"),
                ("backblaze_s3", "Backblaze B2 (mode S3)"),
                ("other",        "Other (custom endpoint)"),
            ]
            provider_combo = ttk.Combobox(
                provider_frame, textvariable=self.var_s3_provider,
                values=[p[0] for p in s3_providers],
                state="readonly", width=20,
            )
            provider_combo.pack(side=tk.LEFT)

            # Display label for selected provider
            provider_labels = {p[0]: p[1] for p in s3_providers}
            lbl_provider_name = ttk.Label(
                provider_frame,
                text=f"  — {provider_labels.get(self.var_s3_provider.get(), '')}",
                style="SubHeader.TLabel",
            )
            lbl_provider_name.pack(side=tk.LEFT, padx=5)

            def on_provider_change(*args):
                prov = self.var_s3_provider.get()
                lbl_provider_name.configure(
                    text=f"  — {provider_labels.get(prov, '')}"
                )
                # Auto-fill endpoint template
                from storage import S3Storage
                template = S3Storage.PROVIDER_ENDPOINTS.get(prov, "")
                if template and prov != "aws":
                    region = self.var_s3_region.get() or "us-east-1"
                    self.var_s3_endpoint.set(
                        template.format(region=region, account_id="")
                    )
                elif prov == "aws":
                    self.var_s3_endpoint.set("")

            self.var_s3_provider.trace_add("write", on_provider_change)

            # Standard S3 fields
            for label, var in [
                ("Bucket", self.var_s3_bucket),
                ("Prefix (subfolder)", self.var_s3_prefix),
                ("Region", self.var_s3_region),
                ("Access Key ID", self.var_s3_access_key),
            ]:
                self._add_field(frame, label, var)
            self._add_field(frame, "Secret Access Key", self.var_s3_secret_key, show="•")

            # Custom endpoint URL
            self._add_field(frame, "Endpoint URL (empty = AWS default)", self.var_s3_endpoint)
            ttk.Label(frame,
                      text="💡 Ex: https://s3.eu-west-1.wasabisys.com, http://minio:9000, ...",
                      style="SubHeader.TLabel").pack(anchor="w", pady=(2, 0))

        elif stype == StorageType.AZURE.value:
            self._add_field(frame, "Connection String", self.var_azure_conn, show="•")
            self._add_field(frame, "Container", self.var_azure_container)
            self._add_field(frame, "Prefix (subfolder)", self.var_azure_prefix)

        elif stype == StorageType.SFTP.value:
            self.var_sftp_port.set(22)
            self._add_field(frame, "Host SFTP", self.var_sftp_host)

            ttk.Label(frame, text="Port").pack(anchor="w", pady=(5, 0))
            ttk.Spinbox(frame, from_=1, to=65535, textvariable=self.var_sftp_port,
                         width=8).pack(anchor="w", pady=(2, 0))

            self._add_field(frame, "Username", self.var_sftp_username)
            self._add_field(frame, "Password (leave empty if using SSH key)",
                            self.var_sftp_password, show="•")
            self._add_field(frame, "SSH private key (optional — replaces password)",
                            self.var_sftp_key_path, browse="*.pem *.key *.ppk *.id_rsa")
            tk.Label(frame, text="Supports RSA, Ed25519, ECDSA keys (.pem, .key, .ppk, id_rsa).",
                     font=("Segoe UI", 8), fg="#95a5a6").pack(anchor="w", pady=(0, 5))
            self._add_field(frame, "Remote path", self.var_sftp_remote_path)

        elif stype == StorageType.GCS.value:
            self._add_field(frame, "GCS Bucket", self.var_gcs_bucket)
            self._add_field(frame, "Prefix (subfolder)", self.var_gcs_prefix)
            self._add_field(frame, "Credentials JSON file (service account)",
                            self.var_gcs_credentials, browse="*.json")
            ttk.Label(frame,
                      text="💡 If empty, uses GOOGLE_APPLICATION_CREDENTIALS or gcloud CLI.",
                      style="SubHeader.TLabel").pack(anchor="w", pady=(3, 0))

        elif stype == StorageType.PROTON.value:
            # Step-by-step setup guide (scrollable, compact)
            tk.Label(frame, text="📋 Proton Drive Setup Guide",
                     fg="#2c3e50", font=("Segoe UI", 10, "bold")).pack(anchor="w")

            guide_frame = tk.Frame(frame)
            guide_frame.pack(fill=tk.X, pady=(2, 8))

            guide_text = tk.Text(guide_frame, wrap=tk.WORD, font=("Segoe UI", 8),
                                  bg="#eaf2f8", fg="#2c3e50", relief=tk.SOLID, bd=1,
                                  height=7, padx=10, pady=6, cursor="arrow")
            guide_scroll = ttk.Scrollbar(guide_frame, orient=tk.VERTICAL,
                                          command=guide_text.yview)
            guide_text.configure(yscrollcommand=guide_scroll.set)
            guide_text.pack(side=tk.LEFT, fill=tk.X, expand=True)
            guide_scroll.pack(side=tk.RIGHT, fill=tk.Y)

            guide_content = (
                "Proton Drive uses rclone (a free open-source tool) to transfer your backups.\n"
                "Your files are end-to-end encrypted with the same keys as the official Proton apps.\n\n"
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
                "Step 4 — Click 'Test connection' below to verify everything works.\n\n"
                "🔒 Security: Your Proton password is stored securely on this computer\n"
                "   using Windows DPAPI encryption. It is never transmitted in plain text."
            )
            guide_text.insert("1.0", guide_content)
            guide_text.configure(state=tk.DISABLED)  # Read-only

            self._add_field(frame, "Proton email", self.var_proton_username)
            self._add_field(frame, "Password Proton", self.var_proton_password, show="•")
            self._add_field(frame, "2FA TOTP secret (only if 2FA enabled — see guide above)",
                            self.var_proton_2fa, show="•")
            self._add_field(frame, "Remote folder in Proton Drive", self.var_proton_remote_path)
            self._add_field(frame, "Path to rclone (empty = auto-detect)",
                            self.var_proton_rclone_path, browse="*.exe")

    def _browse_dest(self):
        path = filedialog.askdirectory(title="Choose destination folder")
        if path:
            self.var_dest_path.set(path)

    def _test_storage(self):
        """Test the storage connection."""
        config = self._build_storage_config()
        try:
            backend = get_storage_backend(config)
            success, message = backend.test_connection()
            if success:
                messagebox.showinfo("Connection test", message)
            else:
                messagebox.showwarning("Connection test", message)
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _test_storage(self):
        """Test the storage connection."""
        config = self._build_storage_config()
        try:
            backend = get_storage_backend(config)
            success, message = backend.test_connection()
            if success:
                messagebox.showinfo("Connection test", message)
            else:
                messagebox.showwarning("Connection test", message)
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # ──────────────────────────────────────────
    #  TAB: Mirror Destinations (3-2-1 Rule)
    # ──────────────────────────────────────────
    def _build_mirror_tab(self):
        container = ttk.Frame(self.tab_mirror)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text="Mirror Destinations — 3-2-1 Rule",
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))

        # Explanation card
        info = tk.Frame(container, bg="#f0f4f8", padx=15, pady=10, relief=tk.SOLID, bd=1)
        info.pack(fill=tk.X, pady=(0, 10))
        tk.Label(info, bg="#f0f4f8", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="The 3-2-1 rule recommends keeping at least 3 copies of your data, "
                      "on 2 different media types, with 1 copy off-site.\n"
                      "Mirror destinations automatically receive a copy of each backup "
                      "after it is created on the primary destination (configured in the Storage tab).\n\n"
                      "Common 3-2-1 setups:\n"
                      "  • Primary: external drive  +  Mirror: cloud S3  (protects against fire/theft)\n"
                      "  • Primary: NAS  +  Mirror: SFTP server  (protects against local disaster)\n"
                      "  • Primary: external drive  +  Mirror 1: NAS  +  Mirror 2: Proton Drive"
                 ).pack(anchor="w")

        # Mirror treeview
        mirror_cols = ("type", "destination", "detail")
        self.mirror_tree = ttk.Treeview(
            container, columns=mirror_cols, show="headings", height=6)
        self.mirror_tree.heading("type", text="Type")
        self.mirror_tree.heading("destination", text="Destination")
        self.mirror_tree.heading("detail", text="Detail")
        self.mirror_tree.column("type", width=150)
        self.mirror_tree.column("destination", width=350)
        self.mirror_tree.column("detail", width=250)
        self.mirror_tree.pack(fill=tk.BOTH, expand=True, pady=(0, 8))

        # Buttons
        mirror_btn_frame = ttk.Frame(container)
        mirror_btn_frame.pack(fill=tk.X)
        ttk.Button(mirror_btn_frame, text="+ Add a mirror destination...",
                    command=self._add_mirror_destination).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(mirror_btn_frame, text="✕ Remove selected",
                    command=self._remove_mirror_destination).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(mirror_btn_frame, text="💾 Save",
                    command=self._save_profile, style="Accent.TButton").pack(side=tk.RIGHT)

    # ──────────────────────────────────────────
    #  Mirror Helpers
    # ──────────────────────────────────────────
    def _get_mirror_label(self, cfg) -> tuple[str, str, str]:
        """Get display strings for a mirror StorageConfig."""
        type_labels = {
            StorageType.LOCAL.value:   "💿 External drive",
            StorageType.NETWORK.value: "🌐 Network",
            StorageType.SFTP.value:    "🔒 SFTP",
            StorageType.S3.value:      "☁ S3",
            StorageType.AZURE.value:   "☁ Azure",
            StorageType.GCS.value:     "☁ GCS",
            StorageType.PROTON.value:  "🔒 Proton Drive",
        }
        stype = cfg.storage_type if hasattr(cfg, "storage_type") else cfg.get("storage_type", "")
        type_str = type_labels.get(stype, stype)

        if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            dest = cfg.destination_path if hasattr(cfg, "destination_path") else cfg.get("destination_path", "")
            return type_str, dest, ""
        elif stype == StorageType.SFTP.value:
            host = cfg.sftp_host if hasattr(cfg, "sftp_host") else cfg.get("sftp_host", "")
            rpath = cfg.sftp_remote_path if hasattr(cfg, "sftp_remote_path") else cfg.get("sftp_remote_path", "")
            return type_str, host, rpath
        elif stype == StorageType.S3.value:
            bucket = cfg.s3_bucket if hasattr(cfg, "s3_bucket") else cfg.get("s3_bucket", "")
            return type_str, bucket, ""
        elif stype == StorageType.AZURE.value:
            container = cfg.azure_container if hasattr(cfg, "azure_container") else cfg.get("azure_container", "")
            return type_str, container, ""
        elif stype == StorageType.GCS.value:
            bucket = cfg.gcs_bucket if hasattr(cfg, "gcs_bucket") else cfg.get("gcs_bucket", "")
            return type_str, bucket, ""
        elif stype == StorageType.PROTON.value:
            user = cfg.proton_username if hasattr(cfg, "proton_username") else cfg.get("proton_username", "")
            rpath = cfg.proton_remote_path if hasattr(cfg, "proton_remote_path") else cfg.get("proton_remote_path", "")
            return type_str, user, rpath
        return type_str, "", ""

    def _refresh_mirror_tree(self):
        """Refresh the mirror destinations treeview from current profile."""
        for item in self.mirror_tree.get_children():
            self.mirror_tree.delete(item)
        if not self.current_profile:
            return
        for cfg in self.current_profile.mirror_destinations:
            type_str, dest, detail = self._get_mirror_label(cfg)
            self.mirror_tree.insert("", tk.END, values=(type_str, dest, detail))

    def _add_mirror_destination(self):
        """Open a dialog to configure a new mirror destination."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Add a mirror destination")
        dialog.geometry("550x400")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Configure mirror destination",
                  font=("Segoe UI", 12, "bold")).pack(padx=15, pady=(10, 5), anchor="w")

        ttk.Label(dialog,
                  text="This destination will receive a copy of the backup after creation "
                       "on the primary destination.",
                  font=("Segoe UI", 9), wraplength=500).pack(padx=15, anchor="w", pady=(0, 10))

        # Type selector
        ttk.Label(dialog, text="Storage type :").pack(padx=15, anchor="w")
        mirror_type_var = tk.StringVar(value=StorageType.LOCAL.value)
        types = [
            (StorageType.LOCAL.value, "💿 External drive / USB stick"),
            (StorageType.NETWORK.value, "🌐 Network folder"),
            (StorageType.SFTP.value, "🔒 SFTP"),
            (StorageType.S3.value, "☁ Amazon S3 / S3-compatible"),
            (StorageType.AZURE.value, "☁ Azure Blob"),
            (StorageType.GCS.value, "☁ Google Cloud Storage"),
            (StorageType.PROTON.value, "🔒 Proton Drive"),
        ]
        type_combo = ttk.Combobox(
            dialog, textvariable=mirror_type_var,
            values=[t[0] for t in types], state="readonly", width=20)
        type_combo.pack(padx=15, anchor="w", pady=(3, 10))

        # Dynamic config fields
        fields_frame = ttk.LabelFrame(dialog, text="Configuration", padding=10)
        fields_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 10))

        field_vars: dict[str, tk.StringVar] = {}

        def update_fields(*args):
            for w in fields_frame.winfo_children():
                w.destroy()
            field_vars.clear()
            stype = mirror_type_var.get()

            if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
                label = "Network path:" if stype == StorageType.NETWORK.value else "Path:"
                ttk.Label(fields_frame, text=label).pack(anchor="w")
                row = ttk.Frame(fields_frame)
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
                for label, key in [("Host :", "sftp_host"), ("Username :", "sftp_username"),
                                    ("Password :", "sftp_password"), ("Remote path :", "sftp_remote_path")]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(3, 0))
                    v = tk.StringVar(value="/backups" if key == "sftp_remote_path" else "")
                    field_vars[key] = v
                    show = "•" if "password" in key else ""
                    ttk.Entry(fields_frame, textvariable=v, font=("Consolas", 9),
                              show=show).pack(fill=tk.X)

            elif stype == StorageType.S3.value:
                for label, key in [("Bucket:", "s3_bucket"), ("Prefix:", "s3_prefix"),
                                    ("Region :", "s3_region"), ("Access Key :", "s3_access_key"),
                                    ("Secret Key :", "s3_secret_key")]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar(value="eu-west-1" if key == "s3_region" else "")
                    field_vars[key] = v
                    show = "•" if "secret" in key else ""
                    ttk.Entry(fields_frame, textvariable=v, font=("Consolas", 9),
                              show=show).pack(fill=tk.X)

            elif stype == StorageType.AZURE.value:
                for label, key in [("Connection String :", "azure_connection_string"),
                                    ("Container :", "azure_container")]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar()
                    field_vars[key] = v
                    show = "•" if "string" in key.lower() else ""
                    ttk.Entry(fields_frame, textvariable=v, font=("Consolas", 9),
                              show=show).pack(fill=tk.X)

            elif stype == StorageType.GCS.value:
                for label, key in [("Bucket:", "gcs_bucket"), ("Credentials JSON :", "gcs_credentials_path")]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar()
                    field_vars[key] = v
                    ttk.Entry(fields_frame, textvariable=v, font=("Consolas", 9)).pack(fill=tk.X)

            elif stype == StorageType.PROTON.value:
                for label, key, show in [
                    ("Proton email:", "proton_username", ""),
                    ("Password :", "proton_password", "•"),
                    ("2FA TOTP secret:", "proton_2fa", "•"),
                    ("Remote folder:", "proton_remote_path", ""),
                ]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar(value="/Backups" if key == "proton_remote_path" else "")
                    field_vars[key] = v
                    kwargs = {"font": ("Consolas", 9)}
                    if show:
                        kwargs["show"] = show
                    ttk.Entry(fields_frame, textvariable=v, **kwargs).pack(fill=tk.X)
                ttk.Label(fields_frame,
                          text="💡 Requires rclone installed (rclone.org)",
                          foreground="#95a5a6", font=("Segoe UI", 8)).pack(anchor="w", pady=(3, 0))

        mirror_type_var.trace_add("write", update_fields)
        update_fields()

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=15, pady=(0, 10))

        def on_add():
            cfg = StorageConfig(storage_type=mirror_type_var.get())
            for key, var in field_vars.items():
                if hasattr(cfg, key):
                    val = var.get()
                    # Encrypt passwords with DPAPI
                    if key in ("sftp_password", "proton_password") and val:
                        val = store_password(val)
                    setattr(cfg, key, val)

            if self.current_profile:
                self.current_profile.mirror_destinations.append(cfg)
                self._refresh_mirror_tree()
            dialog.destroy()

        ttk.Button(btn_frame, text="✅ Add", command=on_add).pack(side=tk.RIGHT)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.RIGHT, padx=(0, 5))

    def _remove_mirror_destination(self):
        """Remove the selected mirror destination."""
        selected = self.mirror_tree.selection()
        if not selected or not self.current_profile:
            return
        idx = self.mirror_tree.index(selected[0])
        if idx < len(self.current_profile.mirror_destinations):
            self.current_profile.mirror_destinations.pop(idx)
            self._refresh_mirror_tree()

    # ──────────────────────────────────────────
    #  TAB: Encryption
    # ──────────────────────────────────────────
    def _build_encryption_tab(self):
        container = ttk.Frame(self.tab_encryption)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Backup encryption',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))

        # Crypto availability check
        crypto = get_crypto_engine()
        if not crypto.is_available:
            warning_frame = tk.Frame(container, bg="#fff3cd", padx=15, pady=10)
            warning_frame.pack(fill=tk.X, pady=(0, 10))
            tk.Label(warning_frame, text="⚠ 'cryptography' library not installed.",
                     bg="#fff3cd", fg="#856404", font=("Segoe UI", 10, "bold")).pack(anchor="w")
            tk.Label(warning_frame,
                     text="Install it with: pip install cryptography",
                     bg="#fff3cd", fg="#856404", font=("Consolas", 9)).pack(anchor="w", pady=(3, 0))

        # Encryption mode — 3 radio buttons
        self.var_encryption_mode = tk.StringVar(value="none")
        self.var_enc_algo = tk.StringVar(value=EncryptionAlgorithm.AES_256_GCM.value)
        self.var_enc_env_var = tk.StringVar()  # Hidden, kept for compatibility
        # Hidden var for backward compat with encryption.enabled
        self.var_encrypt_enabled = tk.BooleanVar(value=False)

        # Option 1: No encryption
        card1 = tk.Frame(container, bg="white", padx=15, pady=10, relief=tk.SOLID, bd=1)
        card1.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card1, text="🔓 No encryption",
                         variable=self.var_encryption_mode, value="none",
                         command=lambda: self._toggle_enc_pwd_frame(False)
                         ).pack(anchor="w")
        tk.Label(card1, bg="white", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="All backups and mirrors are stored in plain text. Fastest option.\n"
                      "✅ Recommended if: local drive at home, non-sensitive data"
                 ).pack(anchor="w", padx=(20, 0))

        # Option 2: Mirrors only
        card2 = tk.Frame(container, bg="white", padx=15, pady=10, relief=tk.SOLID, bd=1)
        card2.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card2, text="🔐 Encrypt mirrors only",
                         variable=self.var_encryption_mode, value="mirrors_only",
                         command=lambda: self._toggle_enc_pwd_frame(True)
                         ).pack(anchor="w")
        tk.Label(card2, bg="white", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="The primary backup stays plain (fast local restore), but all mirror copies "
                      "are encrypted before upload.\n"
                      "✅ Recommended if: primary on local drive + mirrors on cloud or off-site\n"
                      "Best of both worlds: fast local access + secure remote copies"
                 ).pack(anchor="w", padx=(20, 0))

        # Option 3: Encrypt everything
        card3 = tk.Frame(container, bg="white", padx=15, pady=10, relief=tk.SOLID, bd=1)
        card3.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card3, text="🔒 Encrypt everything",
                         variable=self.var_encryption_mode, value="all",
                         command=lambda: self._toggle_enc_pwd_frame(True)
                         ).pack(anchor="w")
        tk.Label(card3, bg="white", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="All backups are encrypted — primary destination AND all mirrors.\n"
                      "✅ Recommended if: confidential data, GDPR compliance, shared drives\n"
                      "⚠ WARNING: losing the password = PERMANENT data loss"
                 ).pack(anchor="w", padx=(20, 0))

        # Password entry frame
        self._encrypt_settings_frame = ttk.LabelFrame(
            container, text="Encryption password (16 characters min.)", padding=10)
        self._encrypt_settings_frame.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(self._encrypt_settings_frame, text="Password:").pack(anchor="w", pady=(0, 2))
        self.var_enc_password = tk.StringVar()
        self.entry_enc_password = ttk.Entry(
            self._encrypt_settings_frame, textvariable=self.var_enc_password,
            show="•", font=("Consolas", 11))
        self.entry_enc_password.pack(fill=tk.X, pady=(0, 3))

        # Character counter
        self.lbl_password_strength = ttk.Label(
            self._encrypt_settings_frame, text="0 / 16 characters", font=("Segoe UI", 8))
        self.lbl_password_strength.pack(anchor="w")
        self.var_enc_password.trace_add("write", self._update_password_strength)

        ttk.Label(self._encrypt_settings_frame, text="Confirmation:").pack(anchor="w", pady=(5, 2))
        self.var_enc_password_confirm = tk.StringVar()
        ttk.Entry(
            self._encrypt_settings_frame, textvariable=self.var_enc_password_confirm,
            show="•", font=("Consolas", 11)).pack(fill=tk.X)

        # Initially hidden
        self._toggle_enc_pwd_frame(False)

        # Save button
        self._enc_save_btn = ttk.Button(container, text='💾 Save',
                    command=self._save_profile, style="Accent.TButton")
        self._enc_save_btn.pack(anchor="e", pady=(10, 0))

    def _toggle_enc_pwd_frame(self, show: bool):
        """Show or hide the password entry frame."""
        if show:
            self._encrypt_settings_frame.pack(fill=tk.X, pady=(8, 0))
            # Re-pack save button at the end
            if hasattr(self, '_enc_save_btn'):
                self._enc_save_btn.pack_forget()
                self._enc_save_btn.pack(anchor="e", pady=(10, 0))
        else:
            self._encrypt_settings_frame.pack_forget()

    def _update_password_strength(self, *args):
        """Update the password character counter."""
        password = self.var_enc_password.get()
        n = len(password)
        feedback = evaluate_password(password)
        color = "#e74c3c" if n < 16 else "#27ae60"
        text = f"{n} / 16 characters"
        if feedback:
            text += f" — {feedback}"
        self.lbl_password_strength.configure(text=text, foreground=color)

    # ──────────────────────────────────────────
    #  TAB: Schedule
    # ──────────────────────────────────────────
    def _build_schedule_tab(self):
        container = ttk.Frame(self.tab_schedule)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Automatic scheduling',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 10))

        # Enable toggle — store reference
        self.var_sched_enabled = tk.BooleanVar(value=False)
        self._sched_checkbox = ttk.Checkbutton(
            container, text="Enable automatic scheduling",
            variable=self.var_sched_enabled)
        self._sched_checkbox.pack(anchor="w", pady=(0, 10))

        # Store settings frame reference
        settings_frame = ttk.LabelFrame(container, text="Settings", padding=15)
        settings_frame.pack(fill=tk.X, pady=(0, 15))
        self._sched_settings_frame = settings_frame

        # Frequency
        ttk.Label(settings_frame, text="Frequency").grid(row=0, column=0, sticky="w", pady=5)
        self.var_frequency = tk.StringVar(value=ScheduleFrequency.DAILY.value)
        freq_combo = ttk.Combobox(
            settings_frame, textvariable=self.var_frequency,
            values=[
                ScheduleFrequency.HOURLY.value,
                ScheduleFrequency.DAILY.value,
                ScheduleFrequency.WEEKLY.value,
                ScheduleFrequency.MONTHLY.value,
            ],
            state="readonly", width=15,
        )
        freq_combo.grid(row=0, column=1, sticky="w", padx=10, pady=5)

        # Time
        ttk.Label(settings_frame, text="Time (HH:MM)").grid(row=1, column=0, sticky="w", pady=5)
        self.var_time = tk.StringVar(value="02:00")
        ttk.Entry(settings_frame, textvariable=self.var_time, width=8,
                  font=("Consolas", 11)).grid(row=1, column=1, sticky="w", padx=10, pady=5)

        # Day of week (for weekly)
        ttk.Label(settings_frame, text="Day (weekly)").grid(
            row=2, column=0, sticky="w", pady=5)

        self.var_day_of_week = tk.StringVar(value=DAYS[0])
        ttk.Combobox(settings_frame, textvariable=self.var_day_of_week,
                      values=DAYS, state="readonly", width=15).grid(
            row=2, column=1, sticky="w", padx=10, pady=5)

        # Day of month (for monthly)
        ttk.Label(settings_frame, text="Day of month (monthly)").grid(
            row=3, column=0, sticky="w", pady=5)
        self.var_day_of_month = tk.IntVar(value=1)
        ttk.Spinbox(settings_frame, from_=1, to=28, textvariable=self.var_day_of_month,
                     width=5).grid(row=3, column=1, sticky="w", padx=10, pady=5)

        # Auto-start with Windows
        autostart_frame = ttk.LabelFrame(container, text="Start with Windows", padding=15)
        autostart_frame.pack(fill=tk.X, pady=(0, 10))

        self.var_autostart = tk.BooleanVar(value=AutoStart.is_enabled())
        ttk.Checkbutton(
            autostart_frame,
            text="Launch Backup Manager automatically when Windows starts",
            variable=self.var_autostart,
            command=self._toggle_autostart,
        ).pack(anchor="w")

        tk.Label(autostart_frame, wraplength=1100, justify=tk.LEFT,
                 font=("Segoe UI", 8), fg="#7f8c8d",
                 text="When enabled, Backup Manager starts with Windows and runs scheduled "
                      "backups in the background.\n"
                      "If your computer was asleep or shut down during a scheduled backup, "
                      "the missed backup will run automatically when the application restarts."
                 ).pack(anchor="w", pady=(5, 0))

        # ── Retry on failure ──
        retry_frame = ttk.LabelFrame(container, text="Automatic retry on failure", padding=10)
        retry_frame.pack(fill=tk.X, pady=(10, 0))

        self.var_retry_enabled = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            retry_frame, text="Automatically retry failed scheduled backups",
            variable=self.var_retry_enabled,
        ).pack(anchor="w")

        retry_settings = ttk.Frame(retry_frame)
        retry_settings.pack(fill=tk.X, pady=(5, 0))

        ttk.Label(retry_settings, text="Max attempts:").pack(side=tk.LEFT, padx=(0, 5))
        self.var_retry_max = tk.IntVar(value=3)
        ttk.Spinbox(retry_settings, from_=1, to=8, width=4,
                      textvariable=self.var_retry_max,
                      command=self._sync_retry_delays).pack(side=tk.LEFT, padx=(0, 15))

        ttk.Label(retry_settings, text="Delays (min):").pack(side=tk.LEFT, padx=(0, 5))
        self.var_retry_delays = tk.StringVar(value="2, 10, 30")
        ttk.Entry(retry_settings, textvariable=self.var_retry_delays,
                  width=40, font=("Consolas", 10)).pack(side=tk.LEFT)

        ttk.Label(retry_frame,
                  text="💡 Delays are auto-adjusted when you change max attempts. "
                       "Each new delay doubles the previous one.",
                  font=("Segoe UI", 8), foreground="#95a5a6"
                  ).pack(anchor="w", pady=(5, 0))

        # Save
        ttk.Button(container, text='💾 Save',
                    command=self._save_profile, style="Accent.TButton").pack(
            anchor="e", pady=(10, 0))

        # ── Schedule execution journal ──
        journal_frame = ttk.LabelFrame(container, text="Scheduled execution journal", padding=10)
        journal_frame.pack(fill=tk.BOTH, expand=True, pady=(15, 0))

        # Next run info
        self.lbl_next_run = ttk.Label(journal_frame, text="",
                                       font=("Segoe UI", 9))
        self.lbl_next_run.pack(anchor="w", pady=(0, 5))

        # Journal treeview
        journal_cols = ("date", "profile", "status", "detail", "duration")
        self.schedule_journal_tree = ttk.Treeview(
            journal_frame, columns=journal_cols, show="headings", height=4)
        self.schedule_journal_tree.heading("date", text="Date")
        self.schedule_journal_tree.heading("profile", text="Profile")
        self.schedule_journal_tree.heading("status", text="Status")
        self.schedule_journal_tree.heading("detail", text="Detail")
        self.schedule_journal_tree.heading("duration", text="Duration")
        self.schedule_journal_tree.column("date", width=140)
        self.schedule_journal_tree.column("profile", width=120)
        self.schedule_journal_tree.column("status", width=80)
        self.schedule_journal_tree.column("detail", width=200)
        self.schedule_journal_tree.column("duration", width=70)

        journal_scroll = ttk.Scrollbar(journal_frame, orient=tk.VERTICAL,
                                        command=self.schedule_journal_tree.yview)
        self.schedule_journal_tree.configure(yscrollcommand=journal_scroll.set)
        self.schedule_journal_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        journal_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Journal buttons
        journal_btn_frame = ttk.Frame(container)
        journal_btn_frame.pack(fill=tk.X, pady=(5, 0))
        ttk.Button(journal_btn_frame, text='🔄 Refresh',
                    command=self._refresh_schedule_journal).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(journal_btn_frame, text="🗑 Clear journal",
                    command=self._clear_schedule_journal).pack(side=tk.LEFT)

    # ──────────────────────────────────────────
    #  TAB: Email Notifications
    # ──────────────────────────────────────────
    def _build_email_tab(self):
        container = ttk.Frame(self.tab_email)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Email notifications',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))
        ttk.Label(container,
                  text="Receive email reports after scheduled backups — on success, failure, or both.",
                  style="SubHeader.TLabel").pack(anchor="w", pady=(0, 15))

        # ── When to send ──
        trigger_frame = ttk.LabelFrame(container, text="When to send emails", padding=15)
        trigger_frame.pack(fill=tk.X, pady=(0, 10))

        self.var_email_trigger = tk.StringVar(value="disabled")
        for value, label in [
            ("disabled",    "🔕 Disabled — no email notifications"),
            ("failure",     "❌ On failure only — email when a backup fails"),
            ("success",     "✅ On success only — email when a backup succeeds"),
            ("always",      "📧 Always — email after every backup (success or failure)"),
        ]:
            ttk.Radiobutton(trigger_frame, text=label, value=value,
                             variable=self.var_email_trigger).pack(anchor="w", pady=2)

        # Hidden vars mapped from radio selection (for EmailConfig compatibility)
        self.var_email_enabled = tk.BooleanVar(value=False)
        self.var_email_on_success = tk.BooleanVar(value=True)
        self.var_email_on_failure = tk.BooleanVar(value=True)

        # ── SMTP Configuration ──
        smtp_frame = ttk.LabelFrame(container, text="SMTP Server", padding=15)
        smtp_frame.pack(fill=tk.X, pady=(0, 10))

        # Row 1: Host + Port + TLS
        row1 = ttk.Frame(smtp_frame)
        row1.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row1, text="SMTP Host:").pack(side=tk.LEFT)
        self.var_smtp_host = tk.StringVar()
        ttk.Entry(row1, textvariable=self.var_smtp_host, width=30,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 15))
        ttk.Label(row1, text="Port:").pack(side=tk.LEFT)
        self.var_smtp_port = tk.IntVar(value=587)
        ttk.Spinbox(row1, from_=1, to=65535, width=6,
                      textvariable=self.var_smtp_port).pack(side=tk.LEFT, padx=(5, 15))
        self.var_smtp_tls = tk.BooleanVar(value=True)
        ttk.Checkbutton(row1, text="Use TLS", variable=self.var_smtp_tls).pack(side=tk.LEFT)

        # Row 2: Username + Password
        row2 = ttk.Frame(smtp_frame)
        row2.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row2, text="Username:").pack(side=tk.LEFT)
        self.var_smtp_user = tk.StringVar()
        ttk.Entry(row2, textvariable=self.var_smtp_user, width=25,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 15))
        ttk.Label(row2, text="Password:").pack(side=tk.LEFT)
        self.var_smtp_password = tk.StringVar()
        ttk.Entry(row2, textvariable=self.var_smtp_password, show="•", width=25,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 0))

        # Row 3: From + To
        row3 = ttk.Frame(smtp_frame)
        row3.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row3, text="From:").pack(side=tk.LEFT)
        self.var_email_from = tk.StringVar()
        ttk.Entry(row3, textvariable=self.var_email_from, width=25,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 15))
        ttk.Label(row3, text="To:").pack(side=tk.LEFT)
        self.var_email_to = tk.StringVar()
        ttk.Entry(row3, textvariable=self.var_email_to, width=30,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 0))

        ttk.Label(smtp_frame,
                  text="💡 Common SMTP: Gmail → smtp.gmail.com:587 | Outlook → smtp.office365.com:587 | "
                       "ProtonMail Bridge → 127.0.0.1:1025",
                  font=("Segoe UI", 8), foreground="#95a5a6"
                  ).pack(anchor="w", pady=(5, 0))

        # ── Buttons ──
        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X, pady=(5, 10))
        ttk.Button(btn_frame, text="📧 Send test email",
                    command=self._send_test_email).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(btn_frame, text="💾 Save",
                    command=self._save_profile, style="Accent.TButton").pack(side=tk.LEFT)
        self.lbl_email_status = ttk.Label(btn_frame, text="", font=("Segoe UI", 9))
        self.lbl_email_status.pack(side=tk.LEFT, padx=(15, 0))

        # ── DPAPI Security Warning ──
        self._email_dpapi_warning = ttk.Label(
            container, text="", font=("Segoe UI", 8), foreground=COLORS["warning"])
        self._email_dpapi_warning.pack(anchor="w")
        self._check_dpapi_warning()

        # Hidden vars for backward compatibility (verification always on)
        self.var_auto_verify = tk.BooleanVar(value=True)
        self.var_alert_on_failure = tk.BooleanVar(value=True)

    def _check_dpapi_warning(self):
        """Show a warning if password storage is not secured by DPAPI."""
        from encryption import store_password
        test = store_password("test_check")
        if test.startswith("b64:"):
            self._email_dpapi_warning.configure(
                text="⚠ Warning: Passwords are stored with BASE64 encoding only (DPAPI unavailable). "
                     "This offers NO real protection. Consider using environment variables for sensitive credentials.")
        else:
            self._email_dpapi_warning.configure(text="")

    def _send_test_email(self):
        """Send a test email with current SMTP settings."""
        config = self._build_email_config()
        if not config.smtp_host:
            messagebox.showwarning("Email", "Please fill in the SMTP settings first.")
            return
        self.lbl_email_status.configure(text="Sending...", foreground=COLORS["warning"])
        self.root.update()

        def run():
            success, msg = send_test_email(config)
            self.root.after(0, lambda: self.lbl_email_status.configure(
                text=msg,
                foreground=COLORS["success"] if success else COLORS["danger"]
            ))
        threading.Thread(target=run, daemon=True).start()

    def _build_email_config(self) -> EmailConfig:
        """Build EmailConfig from current UI fields."""
        trigger = self.var_email_trigger.get()
        enabled = trigger != "disabled"
        on_success = trigger in ("success", "always")
        on_failure = trigger in ("failure", "always")

        return EmailConfig(
            enabled=enabled,
            smtp_host=self.var_smtp_host.get().strip(),
            smtp_port=self.var_smtp_port.get(),
            use_tls=self.var_smtp_tls.get(),
            username=self.var_smtp_user.get().strip(),
            password=self.var_smtp_password.get(),
            from_address=self.var_email_from.get().strip(),
            to_address=self.var_email_to.get().strip(),
            send_on_success=on_success,
            send_on_failure=on_failure,
        )

    def _send_backup_email(self, profile_name: str, success: bool,
                            summary: str, details: str = ""):
        """Send backup report email in background thread."""
        if not self.current_profile:
            return
        config = self.current_profile.email
        if not config.enabled:
            return

        def run():
            ok, msg = send_backup_report(config, profile_name, success, summary, details)
            if ok:
                self.root.after(0, lambda: self._show_status(f"📧 Email sent to {config.to_address}"))
            else:
                self.root.after(0, lambda: self._show_status(f"📧 Email failed: {msg}"))
        threading.Thread(target=run, daemon=True).start()

    # ──────────────────────────────────────────
    #  TAB: Run / Execution
    # ──────────────────────────────────────────
    def _build_run_tab(self):
        container = ttk.Frame(self.tab_run)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Run backup',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))

        self.lbl_run_profile = ttk.Label(container, text="No profile selected",
                                          style="SubHeader.TLabel")
        self.lbl_run_profile.pack(anchor="w", pady=(0, 15))

        # Progress
        progress_frame = ttk.LabelFrame(container, text="Progress", padding=15)
        progress_frame.pack(fill=tk.X, pady=(0, 10))

        self.progress_var = tk.DoubleVar(value=0)
        self.progressbar = ttk.Progressbar(
            progress_frame, variable=self.progress_var,
            maximum=100, length=500, mode="determinate",
            style="Green.Horizontal.TProgressbar",
        )
        self.progressbar.pack(fill=tk.X, pady=(0, 5))

        self.lbl_progress = ttk.Label(progress_frame, text="Waiting...",
                                       font=("Segoe UI", 9))
        self.lbl_progress.pack(anchor="w")

        self.lbl_progress_pct = ttk.Label(progress_frame, text="0%",
                                           font=("Segoe UI", 10, "bold"))
        self.lbl_progress_pct.pack(anchor="e")

        # Status log
        log_frame = ttk.LabelFrame(container, text="Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.log_text = tk.Text(log_frame, font=("Consolas", 9), height=10,
                                 state=tk.DISABLED, bg="#2d2d2d", fg="#00ff00",
                                 relief=tk.FLAT, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        log_scroll = ttk.Scrollbar(self.log_text, command=self.log_text.yview)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=log_scroll.set)

        # Buttons
        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X)

        self.btn_run = ttk.Button(btn_frame, text='▶ Start backup',
                                    command=self._run_backup, style="Accent.TButton")
        self.btn_run.pack(side=tk.LEFT, padx=(0, 5))

        self.btn_cancel = ttk.Button(btn_frame, text='⏹ Cancel',
                                       command=self._cancel_backup, state=tk.DISABLED,
                                       style="Danger.TButton")
        self.btn_cancel.pack(side=tk.LEFT)

    # ──────────────────────────────────────────
    #  TAB: History
    # ──────────────────────────────────────────
    def _build_history_tab(self):
        container = ttk.Frame(self.tab_history)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Backup history',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 10))

        # Treeview for log files
        columns = ("date", "profile", "size")
        self.history_tree = ttk.Treeview(container, columns=columns, show="headings", height=12)
        self.history_tree.heading("date", text="Date")
        self.history_tree.heading("profile", text="Profile")
        self.history_tree.heading("size", text="Log size")
        self.history_tree.column("date", width=200)
        self.history_tree.column("profile", width=200)
        self.history_tree.column("size", width=100)
        self.history_tree.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X)
        ttk.Button(btn_frame, text='🔄 Refresh',
                    command=self._refresh_history).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_frame, text="📂 Open logs folder",
                    command=self._open_logs_folder).pack(side=tk.LEFT)

        self._refresh_history()

    # ──────────────────────────────────────────
    #  TAB: Recovery
    # ──────────────────────────────────────────
    def _build_recovery_tab(self):
        container = ttk.Frame(self.tab_recovery)
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
        ttk.Button(bk_btn_row, text="🔄 Refresh list",
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
                        filedialog.askdirectory(parent=self.root) or self.var_restore_dest.get())
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
            show="•", font=("Consolas", 11), width=64)
        self._restore_pwd_entry.pack(side=tk.LEFT, padx=(0, 3))
        self.var_restore_show_pwd = tk.BooleanVar(value=False)
        ttk.Checkbutton(pwd_row, text="Show",
                         variable=self.var_restore_show_pwd,
                         command=self._toggle_restore_pwd).pack(side=tk.RIGHT)

        # Action buttons
        action_col = ttk.Frame(pwd_action_frame)
        action_col.pack(side=tk.RIGHT)
        self.btn_restore = ttk.Button(
            action_col, text="🔄 Start restore",
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

    # ── Recovery: toggle between browse / available ──
    def _toggle_restore_method(self):
        if self.var_restore_method.get() == "available":
            self._restore_browse_frame.pack_forget()
            self._restore_available_frame.pack(fill=tk.X, pady=(5, 0))
            self._refresh_restore_backups()
        else:
            self._restore_available_frame.pack_forget()
            self._restore_browse_frame.pack(fill=tk.X, pady=(5, 0))

    # ── Recovery: list available backups ──
    def _refresh_restore_backups(self):
        """List available backups from the current profile's destination."""
        for item in self.restore_backup_tree.get_children():
            self.restore_backup_tree.delete(item)

        if not self.current_profile:
            return

        try:
            backend = get_storage_backend(self.current_profile.storage)
            backups = backend.list_backups()
        except Exception as e:
            self._restore_log_append(f"⚠ Cannot list backups: {e}")
            return

        # Filter by profile name and sort by date (newest first)
        prefix = self.current_profile.name
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
            if self.current_profile:
                dest_base = Path(self.current_profile.storage.destination_path)
                full_path = dest_base / filename
                self.var_restore_file.set(str(full_path))

    def _toggle_restore_pwd(self):
        show = "" if self.var_restore_show_pwd.get() else "•"
        self._restore_pwd_entry.configure(show=show)

    def _browse_restore_file(self):
        path = filedialog.askopenfilename(
            parent=self.root,
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
        encrypted = "🔐 Encrypted" if path.endswith(".wbenc") else "🔓 Not encrypted"
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
        from datetime import datetime
        ts = p.stat().st_mtime
        return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")

    def _restore_log_append(self, text: str):
        self.restore_log.configure(state=tk.NORMAL)
        self.restore_log.insert(tk.END, text + "\n")
        self.restore_log.see(tk.END)
        self.restore_log.configure(state=tk.DISABLED)

    # ── Restore: extract a backup file to a destination folder ──
    # Handles: .zip, .wbenc (encrypted), directories.
    # Decryption password prompted if the backup is encrypted.
    def _run_restore(self):
        """Execute the restore operation with optional file selection."""
        import zipfile
        import shutil
        from encryption import get_crypto_engine

        backup_file = self.var_restore_file.get()
        dest_folder = self.var_restore_dest.get()
        password = self.var_restore_password.get()

        # Validation
        if not backup_file or not Path(backup_file).exists():
            messagebox.showerror("Error", "Please select a valid backup file.")
            return
        if not dest_folder:
            messagebox.showerror("Error", "Please select a destination folder.")
            return

        # Clear log
        self.restore_log.configure(state=tk.NORMAL)
        self.restore_log.delete("1.0", tk.END)
        self.restore_log.configure(state=tk.DISABLED)

        self.btn_restore.configure(state=tk.DISABLED)
        self.lbl_restore_status.configure(text="Restoring...", foreground=COLORS["warning"])
        self.root.update()

        try:
            Path(dest_folder).mkdir(parents=True, exist_ok=True)
            self._restore_log_append(f"Destination: {dest_folder}")

            actual_file = backup_file
            decrypted_path = None

            # Decrypt if encrypted
            if backup_file.endswith(".wbenc"):
                if not password:
                    messagebox.showerror("Error",
                        "This backup is encrypted. Please enter the password.")
                    self.btn_restore.configure(state=tk.NORMAL)
                    self.lbl_restore_status.configure(text="")
                    return

                crypto = get_crypto_engine()
                if not crypto.is_available:
                    messagebox.showerror("Error",
                        "Decryption module not available.\n"
                        "Install: pip install cryptography")
                    self.btn_restore.configure(state=tk.NORMAL)
                    self.lbl_restore_status.configure(text="")
                    return

                self._restore_log_append("🔐 Decrypting backup...")
                self.lbl_restore_status.configure(text="Decrypting...")
                self.root.update()

                import secrets
                temp_name = f"_restore_{secrets.token_hex(8)}.zip"
                decrypted_path = Path(dest_folder) / temp_name
                try:
                    crypto.decrypt_file(
                        source=Path(backup_file),
                        dest=decrypted_path,
                        password=password,
                    )
                    self._restore_log_append("✅ Decryption successful")
                    actual_file = str(decrypted_path)
                except Exception as e:
                    self._restore_log_append(f"❌ Decryption failed: {e}")
                    messagebox.showerror("Decryption failed",
                        f"Wrong password or corrupted file:\n{e}")
                    self.btn_restore.configure(state=tk.NORMAL)
                    self.lbl_restore_status.configure(text="Failed", foreground=COLORS["danger"])
                    return

            # Extract ZIP
            if actual_file.endswith(".zip"):
                self._restore_log_append(f"📦 Extracting ZIP archive...")
                self.lbl_restore_status.configure(text="Extracting...")
                self.root.update()

                with zipfile.ZipFile(actual_file, 'r') as zf:
                    # ZIP bomb protection: check total uncompressed size
                    MAX_EXTRACT_SIZE = 50 * 1024 * 1024 * 1024  # 50 GB
                    total_size = sum(info.file_size for info in zf.infolist())
                    if total_size > MAX_EXTRACT_SIZE:
                        self._restore_log_append(
                            f"❌ BLOCKED: Uncompressed size ({total_size / (1024**3):.1f} GB) "
                            f"exceeds 50 GB safety limit.")
                        messagebox.showerror("Error",
                            f"Archive uncompressed size exceeds safety limit.\n"
                            f"This may be a corrupted or malicious file.")
                        return

                    file_list = zf.namelist()

                    total = len(file_list)
                    self._restore_log_append(
                        f"   {total} file(s) to extract "
                        f"({total_size / (1024**2):.1f} MB)")

                    extracted = 0
                    for i, name in enumerate(file_list):
                        # ZIP Slip protection: reject paths that escape dest_folder
                        target = os.path.normpath(os.path.join(dest_folder, name))
                        if not target.startswith(os.path.normpath(dest_folder)):
                            self._restore_log_append(f"⚠ Blocked path traversal: {name}")
                            continue
                        zf.extract(name, dest_folder)
                        extracted += 1
                        if (i + 1) % 50 == 0 or i == total - 1:
                            self._restore_log_append(f"   Extracted {i+1}/{total}")
                            self.root.update()

                self._restore_log_append(f"✅ {extracted} file(s) extracted")

            elif Path(actual_file).is_dir():
                # Flat backup directory — selective copy
                src_dir = Path(actual_file)
                all_files = sorted(f for f in src_dir.rglob("*") if f.is_file())

                total = len(all_files)
                self._restore_log_append(f"📂 Copying {total} file(s)...")

                for i, f in enumerate(all_files):
                    rel = f.relative_to(src_dir)
                    target = Path(dest_folder) / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, target)
                    if (i + 1) % 50 == 0 or i == total - 1:
                        self._restore_log_append(f"   Copied {i+1}/{total}")
                        self.root.update()

                self._restore_log_append(f"✅ {total} file(s) copied")
            else:
                # Single file — just copy
                self._restore_log_append(f"📄 Copying file to {dest_folder}")
                shutil.copy2(actual_file, dest_folder)
                self._restore_log_append("✅ File copied")

            self._restore_log_append(f"\n{'='*50}")
            self._restore_log_append(f"  RESTORE COMPLETE")
            self._restore_log_append(f"  Destination: {dest_folder}")
            self._restore_log_append(f"{'='*50}")

            self.lbl_restore_status.configure(
                text="✅ Restore complete!", foreground=COLORS["success"])

            messagebox.showinfo("Restore complete!",
                f"Backup has been restored to:\n{dest_folder}")

        except zipfile.BadZipFile as e:
            self._restore_log_append(f"\n❌ Corrupted ZIP archive: {e}")
            self.lbl_restore_status.configure(
                text="❌ Corrupted archive", foreground=COLORS["danger"])
            messagebox.showerror("Restore failed", f"Corrupted ZIP archive:\n{e}")

        except Exception as e:
            self._restore_log_append(f"\n❌ ERROR: {e}")
            self.lbl_restore_status.configure(
                text="❌ Failed", foreground=COLORS["danger"])
            messagebox.showerror("Restore failed", f"An error occurred:\n{e}")

        finally:
            # Always clean up temp decrypted file
            if decrypted_path and decrypted_path.exists():
                try:
                    decrypted_path.unlink()
                except OSError:
                    pass
            self.btn_restore.configure(state=tk.NORMAL)

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

    # ── Switch to a different profile ──
    # Clears sensitive fields from memory before loading new profile.
    def _select_profile(self, profile: BackupProfile):
        """Load a profile into the editor."""
        # Clear sensitive fields from previous profile before loading new one
        self._clear_sensitive_fields()
        self.current_profile = profile
        self._load_profile_to_ui(profile)

    # ── Populate all UI fields from a profile ──
    # Reverse of _save_profile: reads BackupProfile, sets all tkinter vars.
    def _load_profile_to_ui(self, p: BackupProfile):
        """Populate all UI fields from profile data."""
        self.var_name.set(p.name)
        self.var_backup_type.set(p.backup_type)
        self.var_compress.set(p.compress)

        # Retention
        ret = p.retention
        self.var_retention_policy.set(ret.policy)
        self.var_max_backups.set(ret.max_backups)
        self.var_gfs_daily.set(ret.gfs_daily)
        self.var_gfs_weekly.set(ret.gfs_weekly)
        self.var_gfs_monthly.set(ret.gfs_monthly)

        # Sources
        self._clear_sources()
        for src in p.source_paths:
            self._insert_source_item(src)

        # Exclusions
        self.exclude_text.delete("1.0", tk.END)
        self.exclude_text.insert("1.0", "\n".join(p.exclude_patterns))

        # Storage
        s = p.storage
        self.var_storage_type.set(s.storage_type)
        self.var_dest_path.set(s.destination_path)
        self.var_s3_bucket.set(s.s3_bucket)
        self.var_s3_prefix.set(s.s3_prefix)
        self.var_s3_region.set(s.s3_region)
        self.var_s3_access_key.set(s.s3_access_key)
        self.var_s3_secret_key.set(s.s3_secret_key)
        self.var_s3_endpoint.set(s.s3_endpoint_url)
        self.var_s3_provider.set(s.s3_provider)
        self.var_azure_conn.set(s.azure_connection_string)
        self.var_azure_container.set(s.azure_container)
        self.var_azure_prefix.set(s.azure_prefix)
        self.var_sftp_host.set(s.sftp_host)
        self.var_sftp_port.set(s.sftp_port)
        self.var_sftp_username.set(s.sftp_username)
        self.var_sftp_password.set(s.sftp_password)
        self.var_sftp_key_path.set(s.sftp_key_path)
        self.var_sftp_remote_path.set(s.sftp_remote_path)
        self.var_gcs_bucket.set(s.gcs_bucket)
        self.var_gcs_prefix.set(s.gcs_prefix)
        self.var_gcs_credentials.set(s.gcs_credentials_path)
        self.var_proton_username.set(s.proton_username)
        self.var_proton_password.set(s.proton_password)
        self.var_proton_2fa.set(s.proton_2fa)
        self.var_proton_remote_path.set(s.proton_remote_path)
        self.var_proton_rclone_path.set(s.proton_rclone_path)
        self._update_storage_fields()
        self.var_bandwidth_limit.set(p.bandwidth_limit_kbps)

        # Mirror destinations
        self._refresh_mirror_tree()

        # Schedule
        sc = p.schedule
        self.var_sched_enabled.set(sc.enabled)
        self.var_frequency.set(sc.frequency)
        self.var_time.set(sc.time)
        self.var_day_of_week.set(DAYS[sc.day_of_week])
        self.var_day_of_month.set(sc.day_of_month)
        self.var_retry_enabled.set(sc.retry_enabled)
        self.var_retry_max.set(sc.retry_max_attempts)
        self.var_retry_delays.set(", ".join(str(d) for d in (sc.retry_delay_minutes or [2, 10, 30])))

        # Encryption
        enc = p.encryption
        self.var_encryption_mode.set(p.encryption_mode)
        self.var_encrypt_enabled.set(enc.enabled)  # backward compat
        self.var_enc_algo.set(enc.algorithm)
        self.var_enc_env_var.set(enc.key_env_variable)
        self.var_enc_password.set("")  # Never load password (not stored)
        self.var_enc_password_confirm.set("")
        self._toggle_enc_pwd_frame(p.encryption_mode != "none")

        # Verification
        vf = p.verification
        self.var_auto_verify.set(vf.auto_verify)
        self.var_alert_on_failure.set(vf.alert_on_failure)

        # Email notifications
        em = p.email
        # Map config fields to radio selection
        if not em.enabled:
            self.var_email_trigger.set("disabled")
        elif em.send_on_success and em.send_on_failure:
            self.var_email_trigger.set("always")
        elif em.send_on_failure:
            self.var_email_trigger.set("failure")
        elif em.send_on_success:
            self.var_email_trigger.set("success")
        else:
            self.var_email_trigger.set("disabled")
        self.var_smtp_host.set(em.smtp_host)
        self.var_smtp_port.set(em.smtp_port)
        self.var_smtp_tls.set(em.use_tls)
        self.var_smtp_user.set(em.username)
        self.var_smtp_password.set(em.password)
        self.var_email_from.set(em.from_address)
        self.var_email_to.set(em.to_address)

        # Run tab info
        last = p.last_backup or "Never"
        run_info = f"Profile: {p.name}  |  Type: {p.backup_type}  |  Last backup: {last}"
        if p.backup_type == BackupType.DIFFERENTIAL.value:
            last_full = p.last_full_backup or "Never"
            run_info += f"  |  Last full: {last_full}"
        self.lbl_run_profile.configure(text=run_info)

        # Refresh schedule journal for this profile
        self._refresh_schedule_journal()

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

    # ── Collect all UI fields and save to disk ──
    # Reads every tab's fields, builds a BackupProfile, saves as JSON.
    # Shows status bar confirmation.
    def _save_profile(self):
        """Save the current profile from UI fields."""
        if not self.current_profile:
            messagebox.showwarning("Warning", "No profile selected.")
            return

        p = self.current_profile
        p.name = self.var_name.get().strip() or "Unnamed"
        p.backup_type = self.var_backup_type.get()
        p.compress = self.var_compress.get()

        # Retention
        p.retention = RetentionConfig(
            policy=self.var_retention_policy.get(),
            max_backups=self.var_max_backups.get(),
            gfs_daily=self.var_gfs_daily.get(),
            gfs_weekly=self.var_gfs_weekly.get(),
            gfs_monthly=self.var_gfs_monthly.get(),
        )

        # Sources — validate paths exist
        p.source_paths = self._get_all_source_paths()
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

        # Exclusions
        raw = self.exclude_text.get("1.0", tk.END).strip()
        p.exclude_patterns = [line.strip() for line in raw.split("\n") if line.strip()]

        # Storage
        p.storage = self._build_storage_config()
        p.bandwidth_limit_kbps = self.var_bandwidth_limit.get()

        # Schedule
        # Parse retry delays from comma-separated string
        try:
            retry_delays = [
                int(d.strip()) for d in self.var_retry_delays.get().split(",")
                if d.strip().isdigit()
            ]
        except (ValueError, AttributeError):
            retry_delays = [2, 10, 30]

        p.schedule = ScheduleConfig(
            enabled=self.var_sched_enabled.get(),
            frequency=self.var_frequency.get(),
            time=self.var_time.get(),
            day_of_week=DAYS.index(self.var_day_of_week.get()) if self.var_day_of_week.get() in DAYS else 0,
            day_of_month=self.var_day_of_month.get(),
            retry_enabled=self.var_retry_enabled.get(),
            retry_max_attempts=self.var_retry_max.get(),
            retry_delay_minutes=retry_delays or [2, 10, 30],
        )

        # Encryption — store password if entered in tab
        stored_pwd = p.encryption.stored_password_b64 if p.encryption else ""
        tab_pwd = self.var_enc_password.get()
        tab_confirm = self.var_enc_password_confirm.get()
        if tab_pwd and tab_pwd == tab_confirm and len(tab_pwd) >= 16:
            stored_pwd = store_password(tab_pwd)
        enc_mode = self.var_encryption_mode.get()
        p.encryption_mode = enc_mode
        p.encryption = EncryptionConfig(
            enabled=(enc_mode == "all"),  # Only "all" encrypts the primary backup
            algorithm=self.var_enc_algo.get(),
            key_env_variable=self.var_enc_env_var.get(),
            stored_password_b64=stored_pwd,
        )

        # Verification
        p.verification = VerificationConfig(
            auto_verify=self.var_auto_verify.get(),
            alert_on_failure=self.var_alert_on_failure.get(),
        )

        # Email notifications
        p.email = self._build_email_config()

        self.config.save_profile(p)
        self._refresh_profile_list()

        # Show confirmation in status bar (visible from all tabs)
        self._show_status(f"✅ Profile '{p.name}' saved successfully.")

    def _build_storage_config(self) -> StorageConfig:
        return StorageConfig(
            storage_type=self.var_storage_type.get(),
            destination_path=self.var_dest_path.get(),
            # S3
            s3_bucket=self.var_s3_bucket.get(),
            s3_prefix=self.var_s3_prefix.get(),
            s3_region=self.var_s3_region.get(),
            s3_access_key=self.var_s3_access_key.get(),
            s3_secret_key=self.var_s3_secret_key.get(),
            s3_endpoint_url=self.var_s3_endpoint.get(),
            s3_provider=self.var_s3_provider.get(),
            # Azure
            azure_connection_string=self.var_azure_conn.get(),
            azure_container=self.var_azure_container.get(),
            azure_prefix=self.var_azure_prefix.get(),
            # SFTP / FTP
            sftp_host=self.var_sftp_host.get(),
            sftp_port=self.var_sftp_port.get(),
            sftp_username=self.var_sftp_username.get(),
            sftp_password=self.var_sftp_password.get(),
            sftp_key_path=self.var_sftp_key_path.get(),
            sftp_remote_path=self.var_sftp_remote_path.get(),
            # GCS
            gcs_bucket=self.var_gcs_bucket.get(),
            gcs_prefix=self.var_gcs_prefix.get(),
            gcs_credentials_path=self.var_gcs_credentials.get(),
            # Proton Drive
            proton_username=self.var_proton_username.get(),
            proton_password=self.var_proton_password.get(),
            proton_2fa=self.var_proton_2fa.get(),
            proton_remote_path=self.var_proton_remote_path.get(),
            proton_rclone_path=self.var_proton_rclone_path.get(),
        )

    # ──────────────────────────────────────────
    #  Source Management (Enhanced)
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
            return "⚠ Not found", "—"
        if p.is_file():
            return "📄 File", self._format_size(p.stat().st_size)
        if p.is_dir():
            try:
                count = sum(1 for _ in p.rglob("*") if _.is_file())
                return "📁 Folder", f"~{count} files"
            except PermissionError:
                return "📁 Folder", "limited access"
        return "❓", "—"

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
        dialog = tk.Toplevel(self.root)
        dialog.title("🗂 Multiple folder selection")
        dialog.geometry("700x550")
        dialog.transient(self.root)
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
            ("🏠 Home", str(self._get_user_home())),
            ("💻 C:\\", "C:\\"),
            ("💻 D:\\", "D:\\"),
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
                    ttk.Button(quick_nav, text=f"💻 {drive}", width=6,
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
                tree.item(item, text="☐ " + Path(path_str).name)
            else:
                checked_paths.add(path_str)
                tree.item(item, text="☑ " + Path(path_str).name)
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
                        icon = "☐ "
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
                                    icon = "☑ " if str(entry) in checked_paths else "☐ "
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

        ttk.Button(bottom, text="✅ Add selection",
                    command=on_confirm, style="Accent.TButton").pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(bottom, text="Cancel",
                    command=dialog.destroy).pack(side=tk.RIGHT)

        # Initial population
        populate_tree(str(self._get_user_home()))

    # ──────────────────────────────────────────
    #  Drive Detection — wait for disconnected local drives
    # ──────────────────────────────────────────

    def _get_missing_local_paths(self) -> list[tuple[str, str]]:
        """
        Check all local/network destinations (primary + mirrors).
        Returns list of (label, path) for destinations whose root drive doesn't exist.
        """
        missing = []
        if not self.current_profile:
            return missing

        # Check primary
        st = self.current_profile.storage
        if st.storage_type in ("local", "network") and st.destination_path:
            root = Path(st.destination_path).anchor  # "D:\\" or "\\\\"
            if root and not Path(root).exists():
                missing.append(("Primary destination", st.destination_path))

        # Check mirrors
        for i, m in enumerate(self.current_profile.mirror_destinations):
            cfg = m if hasattr(m, "storage_type") else StorageConfig(**m)
            if cfg.storage_type in ("local", "network") and cfg.destination_path:
                root = Path(cfg.destination_path).anchor
                if root and not Path(root).exists():
                    missing.append((f"Mirror {i+1}", cfg.destination_path))

        return missing

    def _wait_for_destinations(self) -> bool:
        """
        If any local destination drive is disconnected, show a waiting dialog.
        Polls every 2 seconds. Returns True to proceed, False if user cancelled.
        """
        missing = self._get_missing_local_paths()
        if not missing:
            return True  # All drives connected

        # Build description of missing drives
        drive_letters = set()
        details = []
        for label, path in missing:
            root = Path(path).anchor
            drive_letters.add(root.rstrip("\\"))
            details.append(f"  {label}: {path}")
        drives_str = ", ".join(sorted(drive_letters))
        details_str = "\n".join(details)

        # Show waiting dialog
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

        # Tray notification
        if self.tray:
            self.tray.notify(
                "Drive not connected",
                f"Please connect drive {drives_str} to start the backup."
            )

        # Poll every 2 seconds
        dots = [0]

        def check_drive():
            if result["cancelled"]:
                return
            current_missing = self._get_missing_local_paths()
            if not current_missing:
                # Drive detected!
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

        # Save profile first
        self._save_profile()

        # Check if local destinations are reachable (drive connected?)
        if not self._wait_for_destinations():
            return  # User cancelled

        # Block if encryption needed but module not installed
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

        # Prompt for encryption password if any encryption is needed
        if enc_mode != "none":
            password = self._prompt_encryption_password()
            if password is None:
                return  # User cancelled
            self.engine.set_encryption_password(password)

        self._backup_running = True
        self.btn_run.configure(state=tk.DISABLED)
        self.btn_cancel.configure(state=tk.NORMAL)
        self.progress_var.set(0)
        self._clear_log()

        # Update tray icon → running
        if self.tray:
            self.tray.set_state(TrayState.BACKUP_RUNNING)
            self.tray.notify(
                "Backup started",
                f"Profile: {self.current_profile.name}"
            )

        # Switch to run tab
        self.notebook.select(self.tab_run)

        # Setup callbacks
        self.engine.set_callbacks(
            progress_callback=self._on_progress,
            status_callback=self._on_status,
        )

        # Run in background thread
        thread = threading.Thread(
            target=self._backup_thread, args=(self.current_profile,), daemon=True
        )
        thread.start()

    def _prompt_encryption_password(self) -> Optional[str]:
        """
        Get the encryption password. Priority order:
        1. Stored password in profile (set during wizard or settings)
        2. Password entered in the Encryption tab
        3. Environment variable
        4. Prompt dialog (for restore or if nothing is stored)
        """
        if not self.current_profile:
            return None

        # 1. Stored password in profile
        stored = retrieve_password(self.current_profile.encryption.stored_password_b64)
        if stored:
            return stored

        # 2. Password in the Encryption tab
        tab_password = self.var_enc_password.get()
        if tab_password:
            confirm = self.var_enc_password_confirm.get()
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
            # Save it to profile for next time
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
        """Show a password entry dialog. Used for restore or first-time entry."""
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
            confirm_var = pwd_var  # No confirmation needed for restore

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

    # ── Background thread: runs the backup engine ──
    # IMPORTANT: This runs in a daemon thread. All UI updates
    # MUST go through self.root.after(0, callback) to be thread-safe.
    def _backup_thread(self, profile: BackupProfile):
        """Background thread for running backup."""
        try:
            stats = self.engine.run_backup(profile)
            self.root.after(0, self._backup_finished, stats)
        except Exception as e:
            self.root.after(0, self._backup_error, str(e))

    # ── Post-backup handler (main thread) ──
    # Protected by try/except: a crash here MUST NOT kill the app.
    # All messageboxes MUST have parent=self.root to avoid killing mainloop.
    def _backup_finished(self, stats: BackupStats):
        """Called on the main thread when backup completes. Protected against crashes."""
        try:
            self._backup_finished_impl(stats)
        except Exception as e:
            # NEVER let a post-backup error kill the application
            self._append_log(f"\n⚠ Post-backup error (app stays open): {e}")
            self._backup_running = False
            self.btn_run.configure(state=tk.NORMAL)
            self.btn_cancel.configure(state=tk.DISABLED)

    def _backup_finished_impl(self, stats: BackupStats):
        """Implementation of backup completion handling."""
        self._backup_running = False
        self.btn_run.configure(state=tk.NORMAL)
        self.btn_cancel.configure(state=tk.DISABLED)
        self.progress_var.set(100)

        # Check if backup was cancelled
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
        self._refresh_history()

        # Update schedule journal (for scheduled runs)
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
        self._refresh_schedule_journal()

        # Final dialog / retry logic
        backup_failed = was_cancelled or bool(stats.errors) or stats.verification_status == "failed"

        if backup_failed and self._should_retry():
            # Scheduled backup failed → schedule automatic retry
            self._schedule_retry(
                f"{len(stats.errors)} error(s)" if stats.errors else "verification failed"
            )
            return  # Skip messagebox — retry will handle notification

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

        # Reset scheduled tracking on completion (success or final failure)
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
        self.btn_run.configure(state=tk.NORMAL)
        self.btn_cancel.configure(state=tk.DISABLED)
        self._append_log(f"\n❌ ERROR CRITIQUE: {error_msg}")

        # Update tray
        if self.tray:
            self.tray.set_state(TrayState.BACKUP_ERROR)
            self.tray.notify("Backup failed!", error_msg[:200])

        # Scheduled backup → automatic retry
        if self._should_retry():
            self._schedule_retry(error_msg[:100])
            return

        # Reset scheduled tracking
        self._is_scheduled_run = False
        self._retry_count = 0
        self._retry_profile = None

        # Send failure email
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
            self.btn_cancel.configure(state=tk.DISABLED)
            self.lbl_progress.configure(text="⏹ Cancellation requested...")
            # Cancel any pending retry
            self._is_scheduled_run = False
            self._retry_count = 0
            self._retry_profile = None

    # ──────────────────────────────────────────
    #  Automatic Retry (scheduled backups only)
    # ──────────────────────────────────────────
    def _should_retry(self) -> bool:
        """Check if the current failed backup should be retried."""
        if not self._is_scheduled_run or not self._retry_profile:
            return False
        sched = self._retry_profile.schedule
        if not sched.retry_enabled:
            return False
        return self._retry_count < sched.retry_max_attempts

    # ── Schedule the next retry attempt with increasing delay ──
    def _schedule_retry(self, reason: str):
        """Schedule an automatic retry after a failed scheduled backup."""
        profile = self._retry_profile
        sched = profile.schedule
        self._retry_count += 1

        # Get delay for this attempt (use last value if beyond list length)
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

        # Update journal
        self.scheduler.update_journal_status(
            status="retry_pending",
            detail=retry_msg,
        )
        self._refresh_schedule_journal()

        # Tray notification
        if self.tray:
            self.tray.notify(
                f"Backup failed — retry in {delay_minutes} min",
                f"{profile.name}: attempt {self._retry_count}/{sched.retry_max_attempts}"
            )

        # Schedule the retry
        self.root.after(delay_ms, self._run_retry)

    def _run_retry(self):
        """Execute a retry attempt for a failed scheduled backup."""
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

        # Update journal
        self.scheduler.journal.add(ScheduleLogEntry(
                timestamp=datetime.now().isoformat(),
                profile_id=profile.id,
                profile_name=profile.name,
                trigger="retry",
                status="started",
                detail=f"Retry attempt {attempt}/{max_attempts}",
            )
        )
        self._refresh_schedule_journal()

        # Re-run the backup (keep _is_scheduled_run = True for further retries)
        self._select_profile(profile)
        self._run_backup()

    # ── Callback from InAppScheduler when a profile is due ──
    # Runs on main thread via root.after(0, ...). Sets _is_scheduled_run flag.
    def _scheduled_backup(self, profile: BackupProfile):
        """Callback for the in-app scheduler."""
        self.root.after(0, self._run_scheduled, profile)

    def _run_scheduled(self, profile: BackupProfile):
        """Execute a scheduled backup (triggered from scheduler thread)."""
        self._is_scheduled_run = True
        self._retry_count = 0
        self._retry_profile = profile
        self._select_profile(profile)
        self._run_backup()

    # ──────────────────────────────────────────
    #  Auto-Start with Windows
    # ──────────────────────────────────────────
    def _toggle_autostart(self):
        """Enable or disable auto-start with Windows."""
        enabled = self.var_autostart.get()
        AutoStart.set_enabled(enabled)

    def _sync_retry_delays(self):
        """Auto-adjust the delays list when max attempts changes.
        Adds new delays (doubling the last one) or trims excess delays."""
        try:
            max_att = self.var_retry_max.get()
        except (tk.TclError, ValueError):
            return

        # Parse current delays
        try:
            delays = [
                int(d.strip()) for d in self.var_retry_delays.get().split(",")
                if d.strip().isdigit()
            ]
        except (ValueError, AttributeError):
            delays = [2, 10, 30]

        if not delays:
            delays = [2]

        # Extend if too few delays
        while len(delays) < max_att:
            delays.append(min(delays[-1] * 2, 1440))  # Double, cap at 24h

        # Trim if too many delays
        delays = delays[:max_att]

        self.var_retry_delays.set(", ".join(str(d) for d in delays))

    # ──────────────────────────────────────────
    #  Schedule Journal
    # ──────────────────────────────────────────
    def _refresh_schedule_journal(self):
        """Refresh the schedule journal treeview."""
        for item in self.schedule_journal_tree.get_children():
            self.schedule_journal_tree.delete(item)

        profile_filter = self.current_profile.id if self.current_profile else ""
        entries = self.scheduler.journal.get_entries(limit=50, profile_id=profile_filter)

        for entry in entries:
            ts = entry.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(ts)
                date_str = dt.strftime("%d/%m/%Y %H:%M")
            except (ValueError, TypeError):
                date_str = ts[:16] if ts else "—"

            status = entry.get("status", "")
            status_icons = {
                "started": "🔄 In progress",
                "success": "✅ Passed",
                "failed": "❌ Failed",
                "skipped": "⏭ Skipped",
                "cancelled": "⏹ Cancelled",
                "retry_pending": "⏳ Retry pending",
            }
            status_str = status_icons.get(status, status)

            duration = entry.get("duration_seconds", 0)
            if duration > 0:
                m, s = divmod(int(duration), 60)
                dur_str = f"{m}m{s:02d}s"
            else:
                dur_str = "—"

            self.schedule_journal_tree.insert("", tk.END, values=(
                date_str,
                entry.get("profile_name", ""),
                status_str,
                entry.get("detail", ""),
                dur_str,
            ))

        # Update next run info
        if self.current_profile:
            info = self.scheduler.get_next_run_info(self.current_profile)
            self.lbl_next_run.configure(text=f"🕐 {info}")
        else:
            self.lbl_next_run.configure(text="")

    def _clear_schedule_journal(self):
        """Clear the schedule execution journal."""
        confirm = messagebox.askyesno(
            "Clear journal",
            "Delete all scheduled execution history ?"
        )
        if confirm:
            self.scheduler.journal.clear()
            self._refresh_schedule_journal()

    # ──────────────────────────────────────────
    #  Log & History
    # ──────────────────────────────────────────
    def _on_progress(self, current: int, total: int):
        pct = min((current / total * 100) if total else 0, 100)
        self.root.after(0, lambda: self.progress_var.set(pct))
        self.root.after(0, lambda: self.lbl_progress_pct.configure(text=f"{pct:.0f}%"))

    def _on_status(self, message: str):
        self.root.after(0, lambda: self.lbl_progress.configure(text=message))
        self.root.after(0, lambda: self._append_log(message))

    # ── Status bar at the bottom of the window (visible from all tabs) ──
    def _show_status(self, message: str, duration: int = 3000):
        """Show a temporary status message in the status bar (visible from all tabs).
        Auto-clears after `duration` milliseconds."""
        self.lbl_status_bar.configure(text=message, foreground=COLORS["success"])
        # Cancel any previous auto-clear
        if hasattr(self, '_status_clear_id') and self._status_clear_id:
            self.root.after_cancel(self._status_clear_id)
        self._status_clear_id = self.root.after(
            duration, lambda: self.lbl_status_bar.configure(text=""))

    # ── Security: zero passwords in memory ──
    # Called on profile switch and app quit.
    # Uses ctypes.memset via secure_clear() — best-effort on CPython.
    def _clear_sensitive_fields(self):
        """
        Clear all password/credential fields from memory.
        Called when switching profiles and when quitting.
        Uses secure_clear() to zero the underlying string buffers.
        """
        sensitive_vars = [
            'var_enc_password', 'var_enc_password_confirm',
            'var_smtp_password',
            'var_s3_secret_key', 'var_s3_access_key',
            'var_azure_conn',
            'var_sftp_password',
            'var_proton_password',
            'var_restore_password',
        ]
        for var_name in sensitive_vars:
            var = getattr(self, var_name, None)
            if var and isinstance(var, tk.StringVar):
                old_value = var.get()
                if old_value:
                    secure_clear(old_value)
                    var.set("")

    def _append_log(self, text: str):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _clear_log(self):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _refresh_history(self):
        """Refresh the history treeview from log files."""
        for item in self.history_tree.get_children():
            self.history_tree.delete(item)

        log_dir = self.config.LOG_DIR
        if not log_dir.exists():
            return

        log_files = sorted(log_dir.glob("backup_*.log"), reverse=True)
        for lf in log_files[:50]:
            try:
                stat = lf.stat()
                parts = lf.stem.split("_")
                profile_id = parts[1] if len(parts) > 1 else "?"
                date_str = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                size_kb = f"{stat.st_size / 1024:.1f} Ko"
                self.history_tree.insert("", tk.END, values=(date_str, profile_id, size_kb))
            except OSError:
                pass

    def _open_logs_folder(self):
        log_dir = str(self.config.LOG_DIR)
        if sys.platform == "win32":
            os.startfile(log_dir)
        else:
            import subprocess
            subprocess.Popen(["xdg-open", log_dir], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # ──────────────────────────────────────────
    #  About
    # ──────────────────────────────────────────
    def _open_about(self):
        """Open the About dialog."""
        dialog = tk.Toplevel(self.root)
        dialog.title(f"About — Backup Manager v{APP_VERSION}")
        dialog.resizable(False, False)
        dialog.transient(self.root)
        dialog.grab_set()

        # Center on parent
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
        """Open the dependency manager dialog from within the running app."""
        from installer import check_all, is_frozen

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

        # Treeview with status
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

        # Status label
        status_label = ttk.Label(dialog,
                                  text=f"{len(installed)} installed, {len(missing)} missing",
                                  font=("Segoe UI", 9))
        status_label.pack(padx=15, anchor="w")

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=15, pady=10)

        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT)

    # ──────────────────────────────────────────
    #  App Lifecycle
    # ──────────────────────────────────────────
    def _auto_start_first_backup(self):
        """Called after wizard: select first profile, switch to Run tab, start backup."""
        # Refresh profiles
        self._refresh_profile_list()
        if self.profiles:
            # Select first profile
            self.profile_listbox.selection_set(0)
            self._select_profile(self.profiles[0])
            # Switch to Run tab (first tab)
            self.notebook.select(self.tab_run)
            # Start backup
            self.root.after(300, self._run_backup)

    # ── Window close (X button) behaviour ──
    # If tray icon is active: minimize to tray (app keeps running)
    # If no tray: quit normally (stop scheduler, destroy window)
    def _on_close(self):
        """
        Close button (X) behaviour:
          • If the tray icon is active → minimize to tray
          • Otherwise → quit normally
        """
        if self.tray:
            self._minimize_to_tray()
        else:
            self._quit_app()

    def _minimize_to_tray(self):
        """Hide the main window; the tray icon keeps the app alive."""
        self.root.withdraw()
        if self.tray and not self._backup_running:
            self.tray.notify(
                "Backup Manager",
                "The application is still running in the notification area."
            )

    def _show_from_tray(self):
        """Restore the main window from the tray (called from tray thread)."""
        self.root.after(0, self._restore_window)

    def _restore_window(self):
        """Bring the window back on screen."""
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()
        # Restore from minimized state if needed
        self.root.state("normal")

    def _quit_app(self):
        """Fully quit the application (tray + window + scheduler)."""
        if self._backup_running:
            if not messagebox.askyesno(
                "Backup running",
                "A backup is currently running.\n"
                "Are you sure you want to quit?",
                parent=self.root,
            ):
                return
            self.engine.cancel()

        # Stop tray icon
        if self.tray:
            self.tray.stop()
            self.tray = None

        # Stop scheduler
        self.scheduler.stop()

        # Clear sensitive data from memory
        self._clear_sensitive_fields()

        # Destroy the window
        self.root.destroy()

    def run(self):
        """Start the application."""
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self._quit_app()


# ──────────────────────────────────────────────
#  Entry Point
# ──────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
#  ENTRY POINT — Application startup sequence
#  Order: DPI → Tk root → install deps → wizard → integrity → app
#  CRITICAL: A single tk.Tk() persists for the entire lifetime.
#  All dialogs (installer, wizard) run as Toplevel on this root.
#  If anything crashes, crash.log is written next to the .exe.
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import traceback

    # 0. Enable DPI awareness FIRST — before any Tk window is created.
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(2)
        except (AttributeError, OSError):
            try:
                import ctypes
                ctypes.windll.shcore.SetProcessDpiAwareness(1)
            except (AttributeError, OSError):
                try:
                    import ctypes
                    ctypes.windll.user32.SetProcessDPIAware()
                except (AttributeError, OSError):
                    pass

    # Helper: write crash log to a file (critical for --windowed exe debugging)
    def _crash_log(msg: str):
        """Write crash info to a log file next to the exe (or in APPDATA)."""
        import os
        from pathlib import Path
        try:
            # Try next to the exe first
            if getattr(sys, 'frozen', False):
                log_path = Path(sys.executable).parent / "crash.log"
            else:
                log_path = Path(__file__).parent / "crash.log"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(msg)
        except Exception:
            try:
                # Fallback: APPDATA
                appdata = Path(os.environ.get("APPDATA", ".")) / "BackupManager"
                appdata.mkdir(parents=True, exist_ok=True)
                with open(appdata / "crash.log", "w", encoding="utf-8") as f:
                    f.write(msg)
            except Exception:
                pass

    try:
        # 1. Create root window — keep visible throughout startup
        _root = tk.Tk()
        _root.title("Backup Manager — Starting...")
        _root.geometry("400x100")
        _root.resizable(False, False)

        # Set shield icon immediately (before the window appears in taskbar)
        try:
            from PIL import Image, ImageDraw, ImageFont, ImageTk
            _sz = 64
            _ico = Image.new("RGBA", (_sz, _sz), (0, 0, 0, 0))
            _drw = ImageDraw.Draw(_ico)
            _cx, _cy = _sz // 2, _sz // 2
            _drw.polygon([(_cx, 2), (_sz-4, _cy-12), (_sz-6, _cy+12),
                          (_cx, _sz-2), (6, _cy+12), (4, _cy-12)],
                         fill="#3498db", outline="white")
            try:
                _fnt = ImageFont.truetype("arial.ttf", _sz // 3)
            except (OSError, IOError):
                _fnt = ImageFont.load_default()
            _bb = _drw.textbbox((0, 0), "B", font=_fnt)
            _drw.text((_cx - (_bb[2]-_bb[0])//2, _cy - (_bb[3]-_bb[1])//2 - 1),
                      "B", fill="white", font=_fnt)
            _root._startup_icon = ImageTk.PhotoImage(_ico)
            _root.iconphoto(True, _root._startup_icon)
        except Exception:
            pass

        # Center on screen
        _root.update_idletasks()
        x = (_root.winfo_screenwidth() - 400) // 2
        y = (_root.winfo_screenheight() - 100) // 2
        _root.geometry(f"400x100+{x}+{y}")
        _startup_label = tk.Label(_root, text="⏳ Starting Backup Manager...",
                                   font=("Segoe UI", 11))
        _startup_label.pack(expand=True)
        _root.update()

        # 2. Install all missing dependencies automatically
        from installer import auto_install_all
        auto_install_all(parent=_root)

        # 3. Show setup wizard on first launch (no profiles yet)
        from wizard import SetupWizard, should_show_wizard
        config_check = ConfigManager()
        auto_run_backup = False
        if should_show_wizard(config_check):
            wizard = SetupWizard(config_check, parent=_root)
            wizard.run()
            if wizard.result_profile:
                auto_run_backup = True

        # 4. Application integrity check (non-blocking)
        try:
            passed, msg = verify_integrity()
            if not passed:
                reset_checksums()  # Auto-reset silently after update
        except Exception:
            pass

        # 5. Launch main application — clear startup splash first
        _startup_label.destroy()
        app = BackupManagerApp(root=_root)
        if auto_run_backup:
            app.root.after(500, app._auto_start_first_backup)
        app.run()

    except Exception as e:
        # Catch ANY crash during startup and make it visible
        error_text = f"Backup Manager failed to start:\n\n{e}\n\n{traceback.format_exc()}"
        _crash_log(error_text)

        # Try to show a messagebox (may fail if Tk is dead)
        try:
            err_root = tk.Tk()
            err_root.withdraw()
            messagebox.showerror(
                "Backup Manager — Startup Error",
                f"The application failed to start.\n\n"
                f"Error: {e}\n\n"
                f"A crash log has been saved.\n"
                f"Please check crash.log for details.",
                parent=err_root,
            )
            err_root.destroy()
        except Exception:
            pass

        sys.exit(1)
