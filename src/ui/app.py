"""Main application window with sidebar and tabbed interface."""

import logging
import os
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk

from src import __version__
from src.core.backup_engine import BackupEngine, CancelledError
from src.core.config import BackupProfile, BackupType, ConfigManager
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

    def __init__(self, root: tk.Tk, from_wizard: bool = False):
        self.root = root
        self._from_wizard = from_wizard
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

        # After wizard: switch to Run tab, mark new profiles as
        # already triggered so the scheduler won't auto-run them
        if self._from_wizard:
            self.notebook.select(self.tab_run)
            self.scheduler.skip_startup_check = True
            from datetime import datetime

            for p in self.config_manager.get_all_profiles():
                self.scheduler._state.set_last_trigger(p.id, datetime.now())

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
        self._main_frame = main

        # Sidebar
        self._build_sidebar(main)

        # Notebook (tabs)
        self._build_tabs(main)

        # Alert frame placeholder (shown when targets are unavailable)
        self._alert_frame: tk.Frame | None = None

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

    def _save_profile(self, silent: bool = False) -> bool:
        """Collect config from all tabs and save.

        Args:
            silent: If True, suppress the "Saved" confirmation dialog.

        Returns:
            True if the profile was saved successfully, False otherwise.
        """
        if not self._current_profile:
            return False

        # Validate encryption
        enc_error = self.tab_encryption.validate()
        if enc_error:
            messagebox.showwarning("Validation", enc_error)
            return False

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
                return False

        profile.name = new_name
        profile.backup_type = general["backup_type"]
        profile.full_backup_every = general["full_backup_every"]
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

        # Check for duplicate destinations
        dup_error = self._check_duplicate_destinations(storage["storage"], mirrors)
        if dup_error:
            messagebox.showwarning("Validation", dup_error)
            return False

        retention = self.tab_retention.collect_config()
        profile.retention = retention["retention"]

        # Validate differential full-backup cycle
        if general["backup_type"] == BackupType.DIFFERENTIAL:
            cycle = general["full_backup_every"]
            gfs_d = profile.retention.gfs_daily

            if cycle > gfs_d:
                messagebox.showwarning(
                    "Validation",
                    f"Full backup cycle ({cycle}) must not exceed "
                    f"daily retention ({gfs_d}).\n\n"
                    f"Otherwise the daily rotation could delete the "
                    f"full backup before the next one is created.",
                )
                return False

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
        if not silent:
            messagebox.showinfo("Saved", f"Profile '{profile.name}' saved.")
        return True

    @staticmethod
    def _check_duplicate_destinations(storage, mirrors) -> str:
        """Check that storage and mirrors don't point to the same destination.

        Args:
            storage: Primary StorageConfig.
            mirrors: List of mirror StorageConfig.

        Returns:
            Error message if duplicates found, empty string if OK.
        """
        from src.core.config import StorageType

        def _destination_key(config) -> str:
            """Build a unique key for a destination."""
            st = config.storage_type
            if st in (StorageType.LOCAL, StorageType.NETWORK):
                return f"{st.value}:{config.destination_path.rstrip('/').rstrip(chr(92)).lower()}"
            if st == StorageType.SFTP:
                return (
                    f"sftp:{config.sftp_host}:{config.sftp_port}"
                    f":{config.sftp_remote_path.rstrip('/')}"
                )
            if st == StorageType.S3:
                return f"s3:{config.s3_bucket}:{config.s3_prefix.strip('/')}"
            return ""

        targets = [("Storage", storage)]
        for i, m in enumerate(mirrors):
            targets.append((f"Mirror {i + 1}", m))

        seen: dict[str, str] = {}
        for name, config in targets:
            key = _destination_key(config)
            if not key:
                continue
            if key in seen:
                return (
                    f"{name} and {seen[key]} point to the same destination. "
                    f"Each destination must be unique."
                )
            seen[key] = name

        return ""

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
            if self._current_profile is None:
                self._clear_tabs()

    def _clear_tabs(self):
        """Reset all tabs to empty/default state after profile deletion."""
        blank = BackupProfile()
        self.tab_general.load_profile(blank)
        self.tab_storage.load_profile(blank)
        self.tab_mirror1.load_profile(blank)
        self.tab_mirror2.load_profile(blank)
        self.tab_retention.load_profile(blank)
        self.tab_encryption.load_profile(blank)
        self.tab_schedule.load_profile(blank)
        self.tab_email.load_profile(blank)
        self.tab_history.load_profile(blank)
        self.tab_recovery.load_profile(blank)

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

        # Save current UI state before running (validates config)
        if not self._save_profile(silent=True):
            return

        profile = self._current_profile

        # Validate config before attempting connectivity check
        try:
            profile.storage.validate()
            for mirror in profile.mirror_destinations:
                mirror.validate()
        except ValueError as e:
            messagebox.showwarning("Backup", f"Invalid configuration: {e}")
            return

        self.engine = BackupEngine(self.config_manager, events=self.events)

        # Pre-check targets in background, then start backup if all OK
        self._precheck_and_run(profile)

    def _precheck_and_run(self, profile: BackupProfile) -> None:
        """Run target pre-check in background thread, then start backup.

        Shows a "Checking destinations..." message immediately so the user
        knows something is happening (SFTP timeouts can take 15+ seconds).
        Uses polling pattern (root.after) to stay thread-safe with tkinter.
        """
        self._show_checking_message()

        result: list = [None]  # [None] = pending, [list] = done

        def _do_check() -> None:
            result[0] = self.engine.precheck_targets(profile)

        def _poll() -> None:
            if result[0] is None:
                self.root.after(200, _poll)
                return

            self._hide_target_alert()  # Remove "Checking..." message

            failures = [r for r in result[0] if not r[2]]
            if not failures:
                self._start_backup_thread(profile)
            else:
                self._show_target_alert(
                    failures,
                    on_retry=lambda: self._on_precheck_retry(profile),
                    on_cancel=lambda: self._on_precheck_cancel(),
                )

        threading.Thread(target=_do_check, daemon=True, name="Precheck").start()
        self.root.after(200, _poll)

    def _show_checking_message(self) -> None:
        """Show a 'Checking destinations...' message while precheck runs."""
        self._hide_target_alert()
        self.notebook.pack_forget()

        frame = tk.Frame(self._main_frame, bg=Colors.CARD_BG)
        frame.pack(fill="both", expand=True)
        self._alert_frame = frame

        content = tk.Frame(frame, bg=Colors.CARD_BG)
        content.pack(expand=True)

        tk.Label(
            content,
            text="Checking destinations...",
            font=(Fonts.FAMILY, Fonts.SIZE_HEADER),
            fg=Colors.ACCENT,
            bg=Colors.CARD_BG,
        ).pack(pady=(0, 10))

        tk.Label(
            content,
            text="Verifying that all backup targets are reachable.",
            font=(Fonts.FAMILY, Fonts.SIZE_NORMAL),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.CARD_BG,
        ).pack()

    def _on_precheck_retry(self, profile: BackupProfile) -> None:
        """User clicked Retry — hide alert and re-run precheck."""
        self._hide_target_alert()
        self._precheck_and_run(profile)

    def _on_precheck_cancel(self) -> None:
        """User clicked Cancel — hide alert, set tray to error."""
        self._hide_target_alert()
        self.tray.set_state(TrayState.BACKUP_ERROR)

    def _start_backup_thread(self, profile: BackupProfile) -> None:
        """Start the actual backup in a background thread."""
        self.tab_run.clear_log()

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
                        details="\n".join(stats.log_lines),
                    )

            except CancelledError:
                self.tray.set_state(TrayState.IDLE)
                if profile.email.enabled:
                    from src.notifications.email_notifier import send_backup_report

                    log = ""
                    if self.engine and self.engine._current_result:
                        log = "\n".join(self.engine._current_result.log_lines)
                    send_backup_report(
                        profile.email,
                        profile.name,
                        False,
                        "Backup cancelled by user",
                        details=log,
                        cancelled=True,
                    )
            except Exception as e:
                self.tray.set_state(TrayState.BACKUP_ERROR)
                self.tray.notify("Backup failed", str(e))
                if profile.email.enabled:
                    from src.notifications.email_notifier import send_backup_report

                    log = ""
                    if self.engine and self.engine._current_result:
                        log = "\n".join(self.engine._current_result.log_lines)
                    send_backup_report(
                        profile.email,
                        profile.name,
                        False,
                        str(e),
                        details=log,
                    )

        threading.Thread(target=_backup_thread, daemon=True, name="Backup").start()

    def _cancel_backup(self):
        if self.engine:
            self.engine.cancel()

    def _scheduled_backup(self, profile: BackupProfile):
        """Callback for scheduler-triggered backups.

        Runs in the scheduler daemon thread. Pre-checks targets
        and shows alert on the main thread if any are unavailable.

        Raises:
            RuntimeError: If targets are unavailable and user cancels,
                or if the backup itself fails.
        """
        # Skip unconfigured profiles (default storage has empty destination)
        try:
            profile.storage.validate()
            for mirror in profile.mirror_destinations:
                mirror.validate()
        except ValueError as e:
            logger.warning("Skipping scheduled backup for '%s': %s", profile.name, e)
            return

        self.engine = BackupEngine(self.config_manager, events=self.events)

        # Pre-check targets (blocking — we're in the scheduler thread)
        results = self.engine.precheck_targets(profile)
        failures = [r for r in results if not r[2]]

        if failures:
            # Show alert on main thread and wait for user decision
            user_choice = self._scheduled_precheck_prompt(failures, profile)
            if user_choice == "cancel":
                self.tray.set_state(TrayState.BACKUP_ERROR)
                raise RuntimeError("Backup cancelled: destinations unavailable")

        try:
            self.tray.set_state(TrayState.BACKUP_RUNNING)
            stats = self.engine.run_backup(profile)
            self.tray.set_state(TrayState.BACKUP_SUCCESS)
            self.tray.notify(
                "Scheduled backup complete",
                f"[{profile.name}] {stats.files_processed} files "
                f"in {stats.duration_seconds:.0f}s",
            )
            self.scheduler.journal.update_last(
                status="success",
                files_count=stats.files_processed,
                duration_seconds=stats.duration_seconds,
            )

            if profile.email.enabled:
                from src.notifications.email_notifier import send_backup_report

                send_backup_report(
                    profile.email,
                    profile.name,
                    True,
                    f"{stats.files_processed} files backed up",
                    details="\n".join(stats.log_lines),
                )

        except CancelledError:
            self.tray.set_state(TrayState.IDLE)
            if profile.email.enabled:
                from src.notifications.email_notifier import send_backup_report

                log = ""
                if self.engine and self.engine._current_result:
                    log = "\n".join(self.engine._current_result.log_lines)
                send_backup_report(
                    profile.email,
                    profile.name,
                    False,
                    "Backup cancelled by user",
                    details=log,
                    cancelled=True,
                )

        except Exception as e:
            self.tray.set_state(TrayState.BACKUP_ERROR)
            self.tray.notify(
                "Scheduled backup failed",
                f"[{profile.name}] {e}",
            )

            if profile.email.enabled:
                from src.notifications.email_notifier import send_backup_report

                log = ""
                if self.engine and self.engine._current_result:
                    log = "\n".join(self.engine._current_result.log_lines)
                send_backup_report(
                    profile.email,
                    profile.name,
                    False,
                    str(e),
                    details=log,
                )

            # Re-raise so the scheduler can trigger retry logic
            raise

    def _scheduled_precheck_prompt(
        self,
        failures: list[tuple[str, str, bool, str]],
        profile: BackupProfile,
    ) -> str:
        """Show target alert from scheduler thread, wait for user response.

        Uses root.after() to show UI on the main thread and
        threading.Event to block the scheduler thread until the
        user makes a choice.

        Args:
            failures: Failed targets from precheck_targets().
            profile: Backup profile (for retry precheck).

        Returns:
            "ok" if all targets eventually pass, "cancel" if user cancels.
        """
        decision = {"value": None}  # "retry", "cancel", or None
        event = threading.Event()

        def _show_alert():
            self._show_target_alert(
                failures,
                on_retry=lambda: _on_choice("retry"),
                on_cancel=lambda: _on_choice("cancel"),
            )

        def _on_choice(choice: str):
            decision["value"] = choice
            event.set()

        # Show alert on main thread
        self.root.after(0, _show_alert)
        event.wait()  # Block scheduler thread until user clicks

        if decision["value"] == "cancel":
            self.root.after(0, self._hide_target_alert)
            return "cancel"

        # User clicked retry — hide alert and re-check
        self.root.after(0, self._hide_target_alert)
        results = self.engine.precheck_targets(profile)
        new_failures = [r for r in results if not r[2]]

        if not new_failures:
            return "ok"

        # Still failing — prompt again (recursive)
        return self._scheduled_precheck_prompt(new_failures, profile)

    # --- Target pre-check alert ---

    def _show_target_alert(
        self,
        failures: list[tuple[str, str, bool, str]],
        on_retry: callable,
        on_cancel: callable,
    ) -> None:
        """Replace notebook with an alert frame listing unreachable targets.

        Args:
            failures: List of (role, action, success, detail) with success=False.
            on_retry: Callback when user clicks Retry.
            on_cancel: Callback when user clicks Cancel backup.
        """
        self._hide_target_alert()
        self.notebook.pack_forget()

        # Bring the window to the foreground so the user sees the alert
        self._show_window()

        frame = tk.Frame(self._main_frame, bg=Colors.CARD_BG)
        frame.pack(fill="both", expand=True)
        self._alert_frame = frame

        # Centered content with constrained width
        content = tk.Frame(frame, bg=Colors.CARD_BG)
        content.pack(expand=True, padx=60, pady=40)

        max_width = 700  # Max text width in pixels

        # Warning icon + title
        tk.Label(
            content,
            text="\u26a0  Destinations unavailable",
            font=(Fonts.FAMILY, Fonts.SIZE_HEADER, "bold"),
            fg=Colors.DANGER,
            bg=Colors.CARD_BG,
        ).pack(pady=(0, 20))

        tk.Label(
            content,
            text="The following backup destinations are not reachable:",
            font=(Fonts.FAMILY, Fonts.SIZE_LARGE),
            fg=Colors.TEXT,
            bg=Colors.CARD_BG,
        ).pack(pady=(0, 15))

        # List each failed target
        for role, action, _ok, _detail in failures:
            target_frame = tk.Frame(content, bg=Colors.CARD_BG)
            target_frame.pack(fill="x", pady=8, padx=20)

            tk.Label(
                target_frame,
                text=f"\u25cf  {role}",
                font=(Fonts.FAMILY, Fonts.SIZE_LARGE, "bold"),
                fg=Colors.TEXT,
                bg=Colors.CARD_BG,
                anchor="w",
            ).pack(fill="x")

            tk.Label(
                target_frame,
                text=f"    {action}",
                font=(Fonts.FAMILY, Fonts.SIZE_NORMAL),
                fg=Colors.ACCENT,
                bg=Colors.CARD_BG,
                anchor="w",
                wraplength=max_width,
                justify="left",
            ).pack(fill="x")

        # Footer message
        tk.Label(
            content,
            text="Please connect these destinations and click Retry.",
            font=(Fonts.FAMILY, Fonts.SIZE_NORMAL),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.CARD_BG,
        ).pack(pady=(20, 20))

        # Buttons
        btn_frame = tk.Frame(content, bg=Colors.CARD_BG)
        btn_frame.pack()

        ttk.Button(
            btn_frame,
            text="Retry",
            command=on_retry,
            style="Accent.TButton",
        ).pack(side="left", padx=10)

        ttk.Button(
            btn_frame,
            text="Cancel backup",
            command=on_cancel,
        ).pack(side="left", padx=10)

    def _hide_target_alert(self) -> None:
        """Remove the alert frame and restore the notebook."""
        if self._alert_frame is not None:
            self._alert_frame.destroy()
            self._alert_frame = None
        self.notebook.pack(fill="both", expand=True)

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
        """Auto-save current profile and minimize to tray."""
        self._auto_save()
        self.root.withdraw()

    def _quit_app(self):
        """Auto-save current profile and quit the application."""
        self._auto_save()
        # Hide the window immediately to avoid visual flicker during cleanup
        self.root.withdraw()
        self.root.update_idletasks()
        self.scheduler.stop()
        self.tray.stop()
        self.root.destroy()

    def _auto_save(self):
        """Silently save the current profile if one is loaded."""
        if self._current_profile is not None:
            try:
                self._save_profile(silent=True)
            except Exception as exc:
                logger.warning("Auto-save failed: %s", exc)

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
            f"GNU General Public License v3.0\n\n"
            f"Backup management system with encryption,\n"
            f"scheduling, and multi-destination support.",
        )
