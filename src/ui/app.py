"""Main application window with sidebar and tabbed interface."""

import logging
import os
import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path

from src import __version__
from src.core.config import BackupProfile, ConfigManager
from src.core.events import EventBus, STATUS
from src.core.backup_engine import BackupEngine, CancelledError
from src.core.scheduler import InAppScheduler, AutoStart
from src.ui.theme import (
    Colors, Fonts, Spacing, APP_TITLE, WINDOW_SIZE, MIN_SIZE, setup_theme,
)
from src.ui.tabs.general_tab import GeneralTab
from src.ui.tabs.storage_tab import StorageTab
from src.ui.tabs.mirror_tab import MirrorTab
from src.ui.tabs.retention_tab import RetentionTab
from src.ui.tabs.encryption_tab import EncryptionTab
from src.ui.tabs.schedule_tab import ScheduleTab
from src.ui.tabs.email_tab import EmailTab
from src.ui.tabs.run_tab import RunTab
from src.ui.tabs.recovery_tab import RecoveryTab
from src.ui.tabs.history_tab import HistoryTab
from src.ui.tray import BackupTray, TrayState

logger = logging.getLogger(__name__)


class BackupManagerApp:
    """Main application with sidebar profile list and tabbed configuration."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(f"{APP_TITLE} v{__version__}")
        self.root.geometry(WINDOW_SIZE)
        self.root.minsize(*MIN_SIZE)

        # Core components
        self.config_manager = ConfigManager()
        self.events = EventBus()
        self.engine: BackupEngine | None = None
        self._current_profile: BackupProfile | None = None

        # Setup theme
        self.style = setup_theme(root)

        # Setup scheduler
        self.scheduler = InAppScheduler(
            self.config_manager.config_dir,
            get_profiles=self.config_manager.get_all_profiles,
            backup_callback=self._scheduled_backup,
        )

        # Setup tray
        self.tray = BackupTray(
            show_callback=self._show_window,
            run_backup_callback=self._run_backup,
            quit_callback=self._quit_app,
        )

        # Build UI
        self._build_ui()
        self._load_profiles()

        # Start services
        self.scheduler.start()
        self.tray.start()

        # Window close handler
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        # Subscribe to status events
        self.events.subscribe(STATUS, self._on_status_change)

        # Listen for single-instance "show me" message from second launch
        self._setup_single_instance_listener()

    def _build_ui(self):
        """Build the main window layout."""
        main = ttk.Frame(self.root)
        main.pack(fill="both", expand=True)

        # Sidebar
        self._build_sidebar(main)

        # Notebook (tabs)
        self._build_tabs(main)

    def _build_sidebar(self, parent):
        """Build the left sidebar with profile list."""
        sidebar = tk.Frame(parent, bg=Colors.SIDEBAR_BG, width=200)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)

        # App title
        tk.Label(
            sidebar, text="Backup\nManager",
            bg=Colors.SIDEBAR_BG, fg=Colors.SIDEBAR_TEXT,
            font=Fonts.title(),
        ).pack(pady=(Spacing.XLARGE, Spacing.SMALL))

        tk.Label(
            sidebar, text=f"v{__version__}",
            bg=Colors.SIDEBAR_BG, fg=Colors.TEXT_DISABLED,
            font=Fonts.small(),
        ).pack()

        ttk.Separator(sidebar, orient="horizontal").pack(
            fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE
        )

        # Profile label
        tk.Label(
            sidebar, text="BACKUP PROFILES",
            bg=Colors.SIDEBAR_BG, fg=Colors.TEXT_DISABLED,
            font=Fonts.small(),
        ).pack(anchor="w", padx=Spacing.LARGE)

        # Profile listbox
        self.profile_listbox = tk.Listbox(
            sidebar,
            bg=Colors.SIDEBAR_BG,
            fg=Colors.SIDEBAR_TEXT,
            selectbackground=Colors.SIDEBAR_ACTIVE,
            selectforeground="white",
            highlightthickness=0,
            borderwidth=0,
            font=Fonts.normal(),
            activestyle="none",
        )
        self.profile_listbox.pack(
            fill="both", expand=True,
            padx=Spacing.MEDIUM, pady=Spacing.MEDIUM,
        )
        self.profile_listbox.bind("<<ListboxSelect>>", self._on_profile_selected)

        # Buttons
        btn_frame = tk.Frame(sidebar, bg=Colors.SIDEBAR_BG)
        btn_frame.pack(fill="x", padx=Spacing.MEDIUM, pady=Spacing.MEDIUM)

        tk.Button(
            btn_frame, text="+ New profile",
            bg=Colors.ACCENT, fg="white",
            activebackground=Colors.ACCENT_HOVER,
            activeforeground="white",
            relief="flat", font=Fonts.normal(),
            command=self._new_profile,
        ).pack(fill="x", pady=2)

        tk.Button(
            btn_frame, text="🗑 Delete",
            bg=Colors.DANGER, fg="white",
            activebackground="#c0392b",
            activeforeground="white",
            relief="flat", font=Fonts.normal(),
            command=self._delete_profile,
        ).pack(fill="x", pady=2)

        # Bottom buttons
        bottom = tk.Frame(sidebar, bg=Colors.SIDEBAR_BG)
        bottom.pack(fill="x", padx=Spacing.MEDIUM, pady=Spacing.MEDIUM)

        tk.Button(
            bottom, text="ℹ About",
            bg=Colors.SIDEBAR_HOVER, fg=Colors.SIDEBAR_TEXT,
            activebackground=Colors.SIDEBAR_BG,
            relief="flat", font=Fonts.small(),
            command=self._show_about,
        ).pack(fill="x", pady=2)

    def _build_tabs(self, parent):
        """Build the right-side tabbed notebook."""
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill="both", expand=True)

        # Create tabs
        self.tab_run = RunTab(self.notebook, events=self.events)
        self.tab_general = GeneralTab(self.notebook)
        self.tab_storage = StorageTab(self.notebook)
        self.tab_mirror1 = MirrorTab(self.notebook, mirror_index=0)
        self.tab_mirror2 = MirrorTab(self.notebook, mirror_index=1)
        self.tab_retention = RetentionTab(self.notebook)
        self.tab_encryption = EncryptionTab(self.notebook)
        self.tab_schedule = ScheduleTab(self.notebook, scheduler=self.scheduler)
        self.tab_email = EmailTab(self.notebook)
        self.tab_recovery = RecoveryTab(self.notebook)
        self.tab_history = HistoryTab(self.notebook)

        # Add tabs to notebook
        tabs = [
            (self.tab_run, "▶ Run"),
            (self.tab_general, "⚙ General"),
            (self.tab_storage, "💾 Storage"),
            (self.tab_mirror1, "🔄 Mirror 1"),
            (self.tab_mirror2, "🔄 Mirror 2"),
            (self.tab_retention, "📊 Retention"),
            (self.tab_encryption, "🔒 Encryption"),
            (self.tab_schedule, "⏰ Schedule"),
            (self.tab_email, "📧 Email"),
            (self.tab_recovery, "🔧 Recovery"),
            (self.tab_history, "📜 History"),
        ]
        for tab, label in tabs:
            self.notebook.add(tab, text=f" {label} ")

        # Connect run tab buttons
        self.tab_run.start_btn.config(command=self._run_backup)
        self.tab_run.cancel_btn.config(command=self._cancel_backup)

        # Save button at bottom
        save_frame = ttk.Frame(parent)
        save_frame.pack(fill="x", side="bottom")
        ttk.Button(
            save_frame, text="💾 Save", style="Accent.TButton",
            command=self._save_profile,
        ).pack(side="right", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

    # --- Profile management ---

    def _load_profiles(self):
        """Load all profiles into the sidebar list."""
        self.profile_listbox.delete(0, "end")
        self._profiles = self.config_manager.get_all_profiles()
        for p in self._profiles:
            self.profile_listbox.insert("end", f"  {p.name}")

        if self._profiles:
            self.profile_listbox.select_set(0)
            self._load_profile(self._profiles[0])

    def _on_profile_selected(self, event=None):
        sel = self.profile_listbox.curselection()
        if sel and sel[0] < len(self._profiles):
            self._load_profile(self._profiles[sel[0]])

    def _load_profile(self, profile: BackupProfile):
        """Load a profile into all tabs."""
        self._current_profile = profile

        self.tab_general.load_profile(profile)
        self.tab_storage.load_profile(profile)
        self.tab_mirror1.load_profile(profile)
        self.tab_mirror2.load_profile(profile)
        self.tab_retention.load_profile(profile)
        self.tab_encryption.load_profile(profile)
        self.tab_schedule.load_profile(profile)
        self.tab_email.load_profile(profile)

        self.tab_run.update_profile_info(
            profile.name,
            profile.backup_type.value,
            profile.last_backup,
        )
        self.tab_run.clear_log()

    def _save_profile(self):
        """Collect config from all tabs and save."""
        if not self._current_profile:
            return

        # Validate encryption
        enc_error = self.tab_encryption.validate()
        if enc_error:
            messagebox.showwarning("Validation", enc_error)
            return

        profile = self._current_profile

        # Collect from all tabs
        general = self.tab_general.collect_config()
        profile.name = general["name"]
        profile.backup_type = general["backup_type"]
        profile.source_paths = general["source_paths"]
        profile.exclude_patterns = general["exclude_patterns"]
        profile.bandwidth_limit_kbps = general["bandwidth_limit_kbps"]

        storage = self.tab_storage.collect_config()
        profile.storage = storage["storage"]

        mirrors = []
        for tab in (self.tab_mirror1, self.tab_mirror2):
            m = tab.collect_config()
            if m is not None:
                mirrors.append(m)
        profile.mirror_destinations = mirrors

        retention = self.tab_retention.collect_config()
        profile.retention = retention["retention"]

        encryption = self.tab_encryption.collect_config()
        profile.encrypt_primary = encryption["encrypt_primary"]
        profile.encrypt_mirror1 = encryption["encrypt_mirror1"]
        profile.encrypt_mirror2 = encryption["encrypt_mirror2"]
        profile.encryption = encryption["encryption"]

        schedule = self.tab_schedule.collect_config()
        sched_cfg = schedule["schedule"]
        # Retry enabled comes from general tab
        sched_cfg.retry_enabled = general["retry_enabled"]
        profile.schedule = sched_cfg

        email = self.tab_email.collect_config()
        profile.email = email["email"]

        self.config_manager.save_profile(profile)
        self._load_profiles()
        messagebox.showinfo("Saved", f"Profile '{profile.name}' saved.")

    def _new_profile(self):
        profile = BackupProfile()
        self.config_manager.save_profile(profile)
        self._load_profiles()
        # Select the new profile
        for i, p in enumerate(self._profiles):
            if p.id == profile.id:
                self.profile_listbox.select_set(i)
                self._load_profile(p)
                break

    def _delete_profile(self):
        if not self._current_profile:
            return
        name = self._current_profile.name
        if messagebox.askyesno("Delete", f"Delete profile '{name}'?"):
            self.config_manager.delete_profile(self._current_profile.id)
            self._current_profile = None
            self._load_profiles()

    # --- Backup execution ---

    def _run_backup(self):
        if not self._current_profile:
            messagebox.showwarning("Backup", "No profile selected.")
            return

        self.tab_run.clear_log()
        self.engine = BackupEngine(self.config_manager, events=self.events)

        profile = self._current_profile

        def _backup_thread():
            try:
                self.tray.set_state(TrayState.BACKUP_RUNNING)
                stats = self.engine.run_backup(profile)
                self.tray.set_state(TrayState.BACKUP_SUCCESS)
                self.tray.notify("Backup complete",
                                  f"{stats.files_processed} files in {stats.duration_seconds:.0f}s")

                # Update last_backup
                from datetime import datetime
                profile.last_backup = datetime.now().isoformat()
                self.config_manager.save_profile(profile)

                # Send email notification
                if profile.email.enabled:
                    from src.notifications.email_notifier import send_backup_report
                    send_backup_report(
                        profile.email, profile.name, True,
                        f"{stats.files_processed} files backed up",
                    )

            except CancelledError:
                self.tray.set_state(TrayState.IDLE)
            except Exception as e:
                self.tray.set_state(TrayState.BACKUP_ERROR)
                self.tray.notify("Backup failed", str(e))
                if profile.email.enabled:
                    from src.notifications.email_notifier import send_backup_report
                    send_backup_report(
                        profile.email, profile.name, False, str(e),
                    )

        threading.Thread(target=_backup_thread, daemon=True, name="Backup").start()

    def _cancel_backup(self):
        if self.engine:
            self.engine.cancel()

    def _scheduled_backup(self, profile: BackupProfile):
        """Callback for scheduler-triggered backups."""
        self.engine = BackupEngine(self.config_manager, events=self.events)
        try:
            self.tray.set_state(TrayState.BACKUP_RUNNING)
            stats = self.engine.run_backup(profile)
            self.tray.set_state(TrayState.BACKUP_SUCCESS)
            self.scheduler.journal.update_last(
                status="success",
                files_count=stats.files_processed,
                duration_seconds=stats.duration_seconds,
            )
        except Exception as e:
            self.tray.set_state(TrayState.BACKUP_ERROR)
            self.scheduler.journal.update_last(
                status="failed", detail=str(e)
            )

    # --- Status ---

    def _on_status_change(self, state="", **kw):
        pass  # RunTab handles status display via events

    # --- Window management ---

    def _setup_single_instance_listener(self):
        """Poll for a signal file that indicates a second launch.

        When a second instance starts, it writes a signal file
        and exits. We poll for that file and show the window.
        """
        import os
        from pathlib import Path

        appdata = os.environ.get("APPDATA", "")
        signal_file = Path(appdata) / "BackupManager" / ".show_signal"

        def _check_signal():
            try:
                if signal_file.exists():
                    signal_file.unlink()
                    self._show_window()
            except Exception:
                pass
            self.root.after(500, _check_signal)

        self.root.after(500, _check_signal)

    def _show_window(self):
        """Bring the main window to the foreground."""
        self.root.deiconify()
        self.root.state("normal")
        self.root.lift()
        self.root.attributes("-topmost", True)
        self.root.after(100, lambda: self.root.attributes("-topmost", False))
        self.root.focus_force()

    def _on_close(self):
        """Minimize to tray instead of closing."""
        self.root.withdraw()

    def _quit_app(self):
        """Actually quit the application."""
        self.scheduler.stop()
        self.tray.stop()
        self.root.destroy()

    def _show_modules(self):
        from src.installer import check_all
        results = check_all()
        msg = "Feature status:\n\n"
        for feat, info in results.items():
            status = "✅ Available" if info["available"] else f"❌ Missing: {', '.join(info['missing'])}"
            msg += f"  {feat}: {status}\n"
        messagebox.showinfo("Modules", msg)

    def _show_about(self):
        messagebox.showinfo(
            "About",
            f"{APP_TITLE} v{__version__}\n\n"
            f"Copyright (c) 2026 Loic Ader\n"
            f"MIT License\n\n"
            f"Backup management system with encryption,\n"
            f"scheduling, and multi-destination support.",
        )
