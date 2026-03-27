"""Main application window with sidebar and tabbed interface."""

import logging
import os
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk

from src import __version__
from src.core.backup_engine import BackupEngine, CancelledError
from src.core.config import BackupProfile, ConfigManager
from src.core.events import STATUS, EventBus
from src.core.scheduler import AutoStart, InAppScheduler
from src.ui.tabs.email_tab import EmailTab
from src.ui.tabs.encryption_tab import EncryptionTab
from src.ui.tabs.general_tab import GeneralTab
from src.ui.tabs.history_tab import HistoryTab
from src.ui.tabs.mirror_tab import MirrorTab
from src.ui.tabs.recovery_tab import RecoveryTab
from src.ui.tabs.retention_tab import RetentionTab
from src.ui.tabs.run_tab import RunTab
from src.ui.tabs.schedule_tab import ScheduleTab
from src.ui.tabs.storage_tab import StorageTab
from src.ui.theme import (
    APP_TITLE,
    MIN_SIZE,
    WINDOW_SIZE,
    Colors,
    Fonts,
    Spacing,
    setup_theme,
)
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
            sidebar,
            text="Backup\nManager",
            bg=Colors.SIDEBAR_BG,
            fg=Colors.SIDEBAR_TEXT,
            font=Fonts.title(),
        ).pack(pady=(Spacing.XLARGE, Spacing.SMALL))

        tk.Label(
            sidebar,
            text=f"v{__version__}",
            bg=Colors.SIDEBAR_BG,
            fg=Colors.TEXT_DISABLED,
            font=Fonts.small(),
        ).pack()

        ttk.Separator(sidebar, orient="horizontal").pack(
            fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE
        )

        # Profile listbox with section headers
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
            fill="both",
            expand=True,
            padx=Spacing.MEDIUM,
            pady=Spacing.MEDIUM,
        )
        self.profile_listbox.bind("<<ListboxSelect>>", self._on_profile_selected)
        # Track which listbox indices are headers (non-selectable)
        self._header_indices: set[int] = set()
        self._listbox_profile_map: list[tuple[int, BackupProfile | None]] = []

        # Buttons
        btn_frame = tk.Frame(sidebar, bg=Colors.SIDEBAR_BG)
        btn_frame.pack(fill="x", padx=Spacing.MEDIUM, pady=Spacing.MEDIUM)

        tk.Button(
            btn_frame,
            text="New profile",
            bg=Colors.ACCENT,
            fg="white",
            activebackground=Colors.ACCENT_HOVER,
            activeforeground="white",
            relief="flat",
            font=Fonts.normal(),
            command=self._new_profile,
        ).pack(fill="x", pady=2)

        # Move buttons row
        move_frame = tk.Frame(btn_frame, bg=Colors.SIDEBAR_BG)
        move_frame.pack(fill="x", pady=2)

        tk.Button(
            move_frame,
            text="▲ Up",
            bg=Colors.SIDEBAR_HOVER,
            fg=Colors.SIDEBAR_TEXT,
            activebackground=Colors.SIDEBAR_BG,
            relief="flat",
            font=Fonts.small(),
            command=self._move_profile_up,
        ).pack(side="left", expand=True, fill="x", padx=(0, 1))

        tk.Button(
            move_frame,
            text="▼ Down",
            bg=Colors.SIDEBAR_HOVER,
            fg=Colors.SIDEBAR_TEXT,
            activebackground=Colors.SIDEBAR_BG,
            relief="flat",
            font=Fonts.small(),
            command=self._move_profile_down,
        ).pack(side="left", expand=True, fill="x", padx=(1, 0))

        tk.Button(
            btn_frame,
            text="Delete profile",
            bg=Colors.DANGER,
            fg="white",
            activebackground="#c0392b",
            activeforeground="white",
            relief="flat",
            font=Fonts.normal(),
            command=self._delete_profile,
        ).pack(fill="x", pady=2)

        # Bottom buttons
        bottom = tk.Frame(sidebar, bg=Colors.SIDEBAR_BG)
        bottom.pack(fill="x", padx=Spacing.MEDIUM, pady=Spacing.MEDIUM)

        tk.Button(
            bottom,
            text="About",
            bg=Colors.SIDEBAR_HOVER,
            fg=Colors.SIDEBAR_TEXT,
            activebackground=Colors.SIDEBAR_BG,
            relief="flat",
            font=Fonts.small(),
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
            (self.tab_run, "Run"),
            (self.tab_general, "General"),
            (self.tab_storage, "Storage"),
            (self.tab_mirror1, "Mirror 1"),
            (self.tab_mirror2, "Mirror 2"),
            (self.tab_encryption, "Encryption"),
            (self.tab_schedule, "Schedule"),
            (self.tab_retention, "Retention"),
            (self.tab_email, "Email"),
            (self.tab_recovery, "Recovery"),
            (self.tab_history, "History"),
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
            save_frame,
            text="Save",
            style="Accent.TButton",
            command=self._save_profile,
        ).pack(side="right", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

    # --- Profile management ---

    def _load_profiles(self):
        """Load all profiles into the sidebar list with active/inactive sections."""
        self.profile_listbox.delete(0, "end")
        self._profiles = self.config_manager.get_all_profiles()
        self._header_indices = set()
        self._listbox_profile_map = []

        active = [p for p in self._profiles if p.active]
        inactive = [p for p in self._profiles if not p.active]

        idx = 0
        # Active header
        self.profile_listbox.insert("end", "ACTIVE PROFILES")
        self.profile_listbox.itemconfig(
            idx,
            fg=Colors.TEXT_DISABLED,
            selectbackground=Colors.SIDEBAR_BG,
            selectforeground=Colors.TEXT_DISABLED,
        )
        self._header_indices.add(idx)
        self._listbox_profile_map.append((idx, None))
        idx += 1

        for p in active:
            self.profile_listbox.insert("end", f"  {p.name}")
            self._listbox_profile_map.append((idx, p))
            idx += 1

        # Spacer + Inactive header
        self.profile_listbox.insert("end", "")
        self.profile_listbox.itemconfig(
            idx, selectbackground=Colors.SIDEBAR_BG, selectforeground=Colors.SIDEBAR_BG
        )
        self._header_indices.add(idx)
        self._listbox_profile_map.append((idx, None))
        idx += 1

        self.profile_listbox.insert("end", "INACTIVE PROFILES")
        self.profile_listbox.itemconfig(
            idx,
            fg=Colors.TEXT_DISABLED,
            selectbackground=Colors.SIDEBAR_BG,
            selectforeground=Colors.TEXT_DISABLED,
        )
        self._header_indices.add(idx)
        self._listbox_profile_map.append((idx, None))
        idx += 1

        for p in inactive:
            self.profile_listbox.insert("end", f"  {p.name}")
            self.profile_listbox.itemconfig(idx, fg="#888888")
            self._listbox_profile_map.append((idx, p))
            idx += 1

        # Select first active profile
        if active:
            first_active_idx = 1  # index 0 is header
            self.profile_listbox.select_set(first_active_idx)
            self._load_profile(active[0])

    def _on_profile_selected(self, event=None):
        sel = self.profile_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        # Skip headers
        if idx in self._header_indices:
            self.profile_listbox.selection_clear(idx)
            return
        # Find the profile for this index
        for map_idx, profile in self._listbox_profile_map:
            if map_idx == idx and profile is not None:
                self._load_profile(profile)
                return

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
        self.tab_recovery.load_profile(profile)

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

        # Validate profile name uniqueness
        new_name = general["name"]
        for p in self._profiles:
            if p.id != profile.id and p.name.lower() == new_name.lower():
                messagebox.showwarning(
                    "Validation",
                    f"A profile named '{p.name}' already exists. "
                    "Please choose a different name.",
                )
                return

        profile.name = new_name
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

        # Apply auto-start setting
        if general["autostart"]:
            show_window = not general["autostart_minimized"]
            AutoStart.ensure_startup(show_window=show_window)
        else:
            AutoStart.disable()

        self._load_profiles()
        messagebox.showinfo("Saved", f"Profile '{profile.name}' saved.")

    def _new_profile(self):
        profile = BackupProfile()
        self.config_manager.save_profile(profile)
        self._load_profiles()
        # Select only the new profile
        self.profile_listbox.selection_clear(0, "end")
        for map_idx, p in self._listbox_profile_map:
            if p is not None and p.id == profile.id:
                self.profile_listbox.select_set(map_idx)
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

    def _get_selected_profile(self):
        """Get the currently selected profile and its listbox index."""
        sel = self.profile_listbox.curselection()
        if not sel:
            return None, None
        idx = sel[0]
        if idx in self._header_indices:
            return None, None
        for map_idx, profile in self._listbox_profile_map:
            if map_idx == idx and profile is not None:
                return profile, idx
        return None, None

    def _move_profile_up(self):
        """Move selected profile up, or from inactive to active."""
        profile, idx = self._get_selected_profile()
        if profile is None:
            return

        active_profiles = [p for p in self._profiles if p.active]
        inactive_profiles = [p for p in self._profiles if not p.active]

        if profile.active:
            # Already active — move up within active list
            pos = active_profiles.index(profile)
            if pos == 0:
                return  # Already at top
            # Swap sort_order with the profile above
            other = active_profiles[pos - 1]
            profile.sort_order, other.sort_order = other.sort_order, profile.sort_order
            self.config_manager.save_profile(profile)
            self.config_manager.save_profile(other)
        else:
            # Inactive — first position: move to active
            pos = inactive_profiles.index(profile)
            if pos == 0:
                # Move to active (bottom of active list)
                profile.active = True
                if active_profiles:
                    profile.sort_order = max(p.sort_order for p in active_profiles) + 1
                else:
                    profile.sort_order = 0
                self.config_manager.save_profile(profile)
            else:
                # Move up within inactive list
                other = inactive_profiles[pos - 1]
                profile.sort_order, other.sort_order = other.sort_order, profile.sort_order
                self.config_manager.save_profile(profile)
                self.config_manager.save_profile(other)

        self._load_profiles()
        self._reselect_profile(profile)

    def _move_profile_down(self):
        """Move selected profile down, or from active to inactive."""
        profile, idx = self._get_selected_profile()
        if profile is None:
            return

        active_profiles = [p for p in self._profiles if p.active]
        inactive_profiles = [p for p in self._profiles if not p.active]

        if profile.active:
            pos = active_profiles.index(profile)
            if pos >= len(active_profiles) - 1:
                # Last active — move to inactive
                profile.active = False
                if inactive_profiles:
                    profile.sort_order = min(p.sort_order for p in inactive_profiles) - 1
                else:
                    profile.sort_order = 0
                self.config_manager.save_profile(profile)
            else:
                # Move down within active list
                other = active_profiles[pos + 1]
                profile.sort_order, other.sort_order = other.sort_order, profile.sort_order
                self.config_manager.save_profile(profile)
                self.config_manager.save_profile(other)
        else:
            # Inactive — move down within inactive list
            pos = inactive_profiles.index(profile)
            if pos >= len(inactive_profiles) - 1:
                return  # Already at bottom
            other = inactive_profiles[pos + 1]
            profile.sort_order, other.sort_order = other.sort_order, profile.sort_order
            self.config_manager.save_profile(profile)
            self.config_manager.save_profile(other)

        self._load_profiles()
        self._reselect_profile(profile)

    def _reselect_profile(self, profile: BackupProfile):
        """Re-select a profile in the listbox after reload."""
        self.profile_listbox.selection_clear(0, "end")
        for map_idx, p in self._listbox_profile_map:
            if p is not None and p.id == profile.id:
                self.profile_listbox.select_set(map_idx)
                self._load_profile(p)
                return

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
                self.tray.notify(
                    "Backup complete",
                    f"{stats.files_processed} files in {stats.duration_seconds:.0f}s",
                )

                # Update last_backup
                from datetime import datetime

                profile.last_backup = datetime.now().isoformat()
                self.config_manager.save_profile(profile)

                # Send email notification
                if profile.email.enabled:
                    from src.notifications.email_notifier import send_backup_report

                    send_backup_report(
                        profile.email,
                        profile.name,
                        True,
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
                        profile.email,
                        profile.name,
                        False,
                        str(e),
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
            self.scheduler.journal.update_last(status="failed", detail=str(e))

    # --- Status ---

    def _on_status_change(self, state="", **kw):
        pass  # RunTab handles status display via events

    # --- Window management ---

    def _setup_single_instance_listener(self):
        """Poll for a signal file that indicates a second launch.

        When a second instance starts, it writes a signal file
        and exits. We poll for that file and show the window.
        """

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
            status = (
                "✅ Available" if info["available"] else f"❌ Missing: {', '.join(info['missing'])}"
            )
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
