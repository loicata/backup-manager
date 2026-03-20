"""Setup wizard: 12-step guided profile creation.

Shown on first launch when no profiles exist.
Creates a complete BackupProfile through a multi-step flow.
"""

import logging
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import ttk, filedialog, messagebox

from src.core.config import (
    BackupProfile,
    BackupType,
    StorageConfig,
    StorageType,
    ScheduleConfig,
    ScheduleFrequency,
    RetentionConfig,
    RetentionPolicy,
    EncryptionConfig,
    EmailConfig,
)
from src.installer import get_available_features, FEAT_SFTP, FEAT_S3
from src.notifications.email_notifier import send_test_email, SMTP_PRESETS
from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)


class SetupWizard:
    """12-step setup wizard for creating a backup profile."""

    TOTAL_STEPS = 12

    def __init__(self, parent: tk.Tk = None):
        self.result_profile: BackupProfile | None = None
        self._parent = parent
        self._step = 1
        self._features = get_available_features()

        # Profile data collected across steps
        self._data: dict = {
            "name": "My Backup",
            "sources": [],
            "storage": {
                "type": StorageType.LOCAL.value,
                "vars": {},
            },
            "mirror1": {
                "enabled": False,
                "type": StorageType.LOCAL.value,
                "vars": {},
            },
            "mirror2": {
                "enabled": False,
                "type": StorageType.LOCAL.value,
                "vars": {},
            },
            "backup_type": BackupType.FULL,
            "retention_policy": RetentionPolicy.GFS,
            "encrypt_primary": False,
            "encrypt_mirror1": False,
            "encrypt_mirror2": False,
            "encryption_password": "",
            "schedule_enabled": True,
            "schedule_freq": ScheduleFrequency.WEEKLY,
            "schedule_time": "02:00",
            "email_enabled": False,
            "email_config": {},
        }

        self._build_window()

    def _build_window(self) -> None:
        """Build the wizard window, header, progress bar, content area, and footer."""
        self._win = tk.Toplevel(self._parent) if self._parent else tk.Tk()
        self._win.title("Backup Manager \u2014 Setup Wizard")
        win_w, win_h = 800, 600
        screen_w = self._win.winfo_screenwidth()
        screen_h = self._win.winfo_screenheight()
        x = (screen_w - win_w) // 2
        y = (screen_h - win_h) // 2
        self._win.geometry(f"{win_w}x{win_h}+{x}+{y}")
        self._win.resizable(False, False)

        # Set window icon
        if getattr(sys, "frozen", False):
            base = Path(sys._MEIPASS)  # noqa: SLF001
        else:
            base = Path(__file__).resolve().parent.parent.parent
        ico_path = base / "assets" / "backup_manager.ico"
        if ico_path.exists():
            try:
                self._win.iconbitmap(default=str(ico_path))
                self._win.iconbitmap(str(ico_path))
                try:
                    from PIL import Image, ImageTk

                    img = Image.open(str(ico_path))
                    photo = ImageTk.PhotoImage(img)
                    self._win.iconphoto(True, photo)
                    self._win._icon_photo = photo  # noqa: SLF001
                except ImportError:
                    pass
            except Exception:
                pass

        if self._parent:
            self._win.transient(self._parent)
            self._win.grab_set()

        # Header
        self._header = tk.Frame(self._win, bg=Colors.ACCENT, height=100)
        self._header.pack(fill="x")
        self._header.pack_propagate(False)

        self._title_label = tk.Label(
            self._header,
            text="",
            bg=Colors.ACCENT,
            fg="white",
            font=Fonts.header(),
        )
        self._title_label.pack(pady=(Spacing.LARGE, Spacing.SMALL))

        self._step_label = tk.Label(
            self._header,
            text="",
            bg=Colors.ACCENT,
            fg="#bdc3c7",
            font=Fonts.small(),
        )
        self._step_label.pack()

        # Progress bar
        self._progress = ttk.Progressbar(
            self._win,
            maximum=self.TOTAL_STEPS,
            mode="determinate",
        )
        self._progress.pack(fill="x")

        # Content area with scrollbar
        content_outer = ttk.Frame(self._win)
        content_outer.pack(fill="both", expand=True)

        self._canvas = tk.Canvas(content_outer, highlightthickness=0)
        self._scrollbar = ttk.Scrollbar(
            content_outer,
            orient="vertical",
            command=self._canvas.yview,
        )
        self._content = ttk.Frame(self._canvas, padding=Spacing.XLARGE)

        self._content.bind(
            "<Configure>",
            lambda e: self._canvas.configure(scrollregion=self._canvas.bbox("all")),
        )
        self._canvas_window = self._canvas.create_window(
            (0, 0),
            window=self._content,
            anchor="nw",
        )
        self._canvas.configure(yscrollcommand=self._scrollbar.set)

        self._canvas.pack(side="left", fill="both", expand=True)
        self._scrollbar.pack(side="right", fill="y")

        # Resize content width to match canvas
        self._canvas.bind("<Configure>", self._on_canvas_resize)

        # Mousewheel scrolling
        self._canvas.bind_all("<MouseWheel>", self._on_mousewheel)

        # Footer with navigation
        footer = ttk.Frame(self._win)
        footer.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE)

        self._back_btn = ttk.Button(footer, text="\u2190 Back", command=self._go_back)
        self._back_btn.pack(side="left")

        self._next_btn = ttk.Button(
            footer,
            text="Next \u2192",
            style="Accent.TButton",
            command=self._go_next,
        )
        self._next_btn.pack(side="right")

        ttk.Button(
            footer,
            text="Cancel",
            command=self._cancel,
        ).pack(side="right", padx=Spacing.MEDIUM)

        self._show_step()

    def _on_canvas_resize(self, event: tk.Event) -> None:
        """Keep content frame width synced with canvas."""
        self._canvas.itemconfig(self._canvas_window, width=event.width)

    def _on_mousewheel(self, event: tk.Event) -> None:
        """Scroll content with mouse wheel."""
        self._canvas.yview_scroll(-1 * (event.delta // 120), "units")

    def run(self) -> BackupProfile | None:
        """Run the wizard and return the created profile, or None if cancelled."""
        self._win.wait_window()
        return self.result_profile

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def _show_step(self) -> None:
        """Display the current step."""
        for w in self._content.winfo_children():
            w.destroy()

        # Reset scroll to top
        self._canvas.yview_moveto(0)

        self._progress["value"] = self._step
        self._back_btn.state(["!disabled"] if self._step > 1 else ["disabled"])

        if self._step == self.TOTAL_STEPS:
            self._next_btn.config(text="\u2713 Create profile")
        else:
            self._next_btn.config(text="Next \u2192")

        step_builders = {
            1: self._step_welcome,
            2: self._step_name,
            3: self._step_sources,
            4: self._step_storage,
            5: self._step_mirror1,
            6: self._step_mirror2,
            7: self._step_backup_type,
            8: self._step_retention,
            9: self._step_encryption,
            10: self._step_schedule,
            11: self._step_email,
            12: self._step_summary,
        }

        builder = step_builders.get(self._step, lambda: None)
        builder()

    def _set_header(self, title: str) -> None:
        """Update the wizard header with step title and counter."""
        self._title_label.config(text=title)
        self._step_label.config(text=f"Step {self._step} of {self.TOTAL_STEPS}")

    def _go_next(self) -> None:
        """Advance to the next step, or create the profile on the last step."""
        if self._step == self.TOTAL_STEPS:
            self._create_profile()
            return
        self._collect_current_step()
        self._step = min(self._step + 1, self.TOTAL_STEPS)
        self._show_step()

    def _go_back(self) -> None:
        """Go back to the previous step."""
        self._step = max(self._step - 1, 1)
        self._show_step()

    def _cancel(self) -> None:
        """Cancel the wizard without creating a profile."""
        self._canvas.unbind_all("<MouseWheel>")
        self.result_profile = None
        self._win.destroy()

    def _collect_current_step(self) -> None:
        """Collect data from current step before moving."""
        pass  # Each step stores data via its own widgets / traces

    # ------------------------------------------------------------------
    # Step 1: Welcome
    # ------------------------------------------------------------------

    def _step_welcome(self) -> None:
        """Display the welcome screen."""
        self._set_header("Welcome!")
        ttk.Label(
            self._content,
            text="This wizard will guide you through creating\n"
            "your first backup profile.\n\n"
            "You can change any setting later.",
            font=Fonts.large(),
        ).pack(pady=Spacing.XLARGE)

    # ------------------------------------------------------------------
    # Step 2: Profile name
    # ------------------------------------------------------------------

    def _step_name(self) -> None:
        """Display the profile name input."""
        self._set_header("Profile name")
        ttk.Label(
            self._content,
            text="Choose a name for this backup profile:",
        ).pack(anchor="w")
        self._name_var = tk.StringVar(value=self._data["name"])
        ttk.Entry(
            self._content,
            textvariable=self._name_var,
            width=40,
            font=Fonts.large(),
        ).pack(fill="x", pady=Spacing.MEDIUM)
        self._name_var.trace_add(
            "write",
            lambda *a: self._data.update(name=self._name_var.get()),
        )

    # ------------------------------------------------------------------
    # Step 3: Source folders
    # ------------------------------------------------------------------

    def _step_sources(self) -> None:
        """Display the source folder selection."""
        self._set_header("What to back up?")
        ttk.Label(
            self._content,
            text="Select folders and files to include:",
        ).pack(anchor="w")

        self._src_listbox = tk.Listbox(
            self._content,
            height=10,
            font=Fonts.normal(),
        )
        self._src_listbox.pack(fill="both", expand=True, pady=Spacing.MEDIUM)

        for src in self._data["sources"]:
            self._src_listbox.insert("end", src)

        btn = ttk.Frame(self._content)
        btn.pack(fill="x")
        ttk.Button(btn, text="Add", command=self._wizard_add_folder).pack(
            side="left",
            padx=2,
        )
        ttk.Button(btn, text="Remove", command=self._wizard_remove_source).pack(
            side="left",
            padx=2,
        )

    def _wizard_add_folder(self) -> None:
        """Open a directory picker and add the selected folder."""
        path = filedialog.askdirectory(parent=self._win)
        if path:
            self._wizard_add_source(path)

    def _wizard_add_source(self, path: str) -> None:
        """Add a source path to the list if not already present."""
        if path not in self._data["sources"]:
            self._data["sources"].append(path)
            self._src_listbox.insert("end", path)

    def _wizard_remove_source(self) -> None:
        """Remove the selected source path from the list."""
        sel = self._src_listbox.curselection()
        if sel:
            idx = sel[0]
            self._data["sources"].pop(idx)
            self._src_listbox.delete(idx)

    # ------------------------------------------------------------------
    # Factorized storage config builder
    # ------------------------------------------------------------------

    def _build_storage_config_ui(
        self,
        parent: ttk.Frame,
        storage_key: str,
    ) -> dict:
        """Build the full storage type selection + config frames.

        This method is called 3 times (primary, mirror1, mirror2).
        Each call creates its own set of tk vars and widgets.

        Args:
            parent: Parent frame to build into.
            storage_key: Key in self._data ("storage", "mirror1", "mirror2").

        Returns:
            Dict with references to created widgets:
                "type_var", "config_frames", "local_path_var",
                "network_path_var", "sftp_vars", "s3_vars",
                "s3_provider_var", "proton_vars",
                "test_btn", "test_label".
        """
        sd = self._data[storage_key]

        # Storage type selection
        type_frame = ttk.LabelFrame(parent, text="Storage type", padding=Spacing.PAD)
        type_frame.pack(fill="x", pady=(0, Spacing.SMALL))

        type_var = tk.StringVar(value=sd.get("type", StorageType.LOCAL.value))

        options = [
            (StorageType.LOCAL, "External drive / USB stick", True),
            (StorageType.NETWORK, "Network folder (UNC)", True),
            (StorageType.SFTP, "Remote server SFTP (SSH)", FEAT_SFTP in self._features),
            (StorageType.S3, "S3 Cloud Storage (beta)", FEAT_S3 in self._features),
            (StorageType.PROTON, "Proton Drive (beta)", True),
        ]

        for stype, label, available in options:
            ttk.Radiobutton(
                type_frame,
                text=label,
                value=stype.value,
                variable=type_var,
                state="normal" if available else "disabled",
            ).pack(anchor="w", pady=2)

        # Config container
        config_container = ttk.LabelFrame(
            parent,
            text="Configuration",
            padding=Spacing.PAD,
        )
        config_container.pack(fill="both", expand=True, pady=Spacing.SMALL)

        config_frames: dict[str, ttk.Frame] = {}
        saved = sd.get("vars", {})

        # --- Local config ---
        f = ttk.Frame(config_container)
        config_frames["local"] = f
        ttk.Label(f, text="Destination path:").pack(anchor="w")
        local_path_var = tk.StringVar(value=saved.get("destination_path", ""))
        row = ttk.Frame(f)
        row.pack(fill="x")
        ttk.Entry(row, textvariable=local_path_var).pack(
            side="left",
            fill="x",
            expand=True,
        )
        ttk.Button(
            row,
            text="Browse...",
            command=lambda v=local_path_var: v.set(
                filedialog.askdirectory(parent=self._win) or v.get()
            ),
        ).pack(side="right", padx=(Spacing.SMALL, 0))

        # --- Network config ---
        f = ttk.Frame(config_container)
        config_frames["network"] = f
        ttk.Label(f, text="Network path (UNC):").pack(anchor="w")
        network_path_var = tk.StringVar(value=saved.get("destination_path", ""))
        ttk.Entry(f, textvariable=network_path_var).pack(fill="x")
        ttk.Label(
            f,
            text=r"e.g. \\server\share\backups",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w")

        # --- SFTP config ---
        f = ttk.Frame(config_container)
        config_frames["sftp"] = f
        sftp_fields = [
            ("Host SFTP", "sftp_host", ""),
            ("Port", "sftp_port", "22"),
            ("Username", "sftp_username", ""),
            ("Password (leave empty if using SSH key)", "sftp_password", ""),
            ("SSH private key (optional \u2014 replaces password)", "sftp_key_path", ""),
            ("Key passphrase (if key is protected)", "sftp_key_passphrase", ""),
            ("Remote path", "sftp_remote_path", "/home/username/backups"),
        ]
        sftp_vars: dict[str, tk.StringVar] = {}
        for label, key, default in sftp_fields:
            ttk.Label(f, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=saved.get(key, default))
            sftp_vars[key] = var

            if key == "sftp_key_path":
                row = ttk.Frame(f)
                row.pack(fill="x")
                ttk.Entry(row, textvariable=var).pack(
                    side="left",
                    fill="x",
                    expand=True,
                )
                ttk.Button(
                    row,
                    text="Browse...",
                    command=lambda v=var: v.set(
                        filedialog.askopenfilename(
                            parent=self._win,
                            title="Select SSH key",
                            filetypes=[
                                ("SSH keys", "*.pem *.key *.ppk id_*"),
                                ("All files", "*.*"),
                            ],
                        )
                        or v.get()
                    ),
                ).pack(side="right", padx=(4, 0))
            elif "password" in key or "passphrase" in key:
                ttk.Entry(f, textvariable=var, show="\u25cf").pack(fill="x")
            elif key == "sftp_port":
                ttk.Spinbox(
                    f,
                    textvariable=var,
                    from_=1,
                    to=65535,
                    width=8,
                ).pack(anchor="w")
            else:
                ttk.Entry(f, textvariable=var).pack(fill="x")

        ttk.Label(
            f,
            text="Supports RSA, Ed25519, ECDSA keys (.pem, .key, .ppk, id_rsa).",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w")
        ttk.Label(
            f,
            text="Absolute path on the remote server, e.g. /home/username/backups",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w")

        # --- S3 config ---
        f = ttk.Frame(config_container)
        config_frames["s3"] = f

        ttk.Label(f, text="Provider:").pack(anchor="w")
        s3_provider_var = tk.StringVar(value=saved.get("s3_provider", "aws"))
        providers = [
            "aws",
            "minio",
            "wasabi",
            "ovh",
            "scaleway",
            "digitalocean",
            "cloudflare",
            "backblaze_s3",
            "other",
        ]
        ttk.Combobox(
            f,
            textvariable=s3_provider_var,
            values=providers,
            state="readonly",
        ).pack(fill="x")

        s3_fields = [
            ("Bucket", "s3_bucket", ""),
            ("Prefix (optional)", "s3_prefix", ""),
            ("Region", "s3_region", "eu-west-1"),
            ("Access Key", "s3_access_key", ""),
            ("Secret Key", "s3_secret_key", ""),
            ("Endpoint URL (for S3-compatible)", "s3_endpoint_url", ""),
        ]
        s3_vars: dict[str, tk.StringVar] = {}
        for label, key, default in s3_fields:
            ttk.Label(f, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=saved.get(key, default))
            s3_vars[key] = var
            if "secret" in key or ("access" in key and "key" in key):
                ttk.Entry(f, textvariable=var, show="\u25cf").pack(fill="x")
            else:
                ttk.Entry(f, textvariable=var).pack(fill="x")

        # --- Proton config ---
        f = ttk.Frame(config_container)
        config_frames["proton"] = f

        proton_fields = [
            ("Proton username", "proton_username", ""),
            ("Proton password", "proton_password", ""),
            ("2FA seed (optional)", "proton_2fa", ""),
            ("Remote path", "proton_remote_path", "/Backups"),
            ("rclone path (optional)", "proton_rclone_path", ""),
        ]
        proton_vars: dict[str, tk.StringVar] = {}
        for label, key, default in proton_fields:
            ttk.Label(f, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=saved.get(key, default))
            proton_vars[key] = var
            if "password" in key or "2fa" in key:
                ttk.Entry(f, textvariable=var, show="\u25cf").pack(fill="x")
            else:
                ttk.Entry(f, textvariable=var).pack(fill="x")

        self._add_proton_guide(f)

        # --- Test connection button ---
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill="x", pady=(Spacing.SMALL, 0))

        test_label = ttk.Label(btn_frame, text="", foreground=Colors.TEXT_SECONDARY)

        test_btn = ttk.Button(
            btn_frame,
            text="Test connection",
            command=lambda: self._test_storage_connection(
                storage_key,
                test_btn,
                test_label,
            ),
        )
        test_btn.pack(side="left")
        test_label.pack(side="left", padx=Spacing.MEDIUM)

        # --- Show/hide config frames on type change ---
        def _show_hide(*_args: object) -> None:
            for frm in config_frames.values():
                frm.pack_forget()
            stype = type_var.get()
            frm = config_frames.get(stype)
            if frm:
                frm.pack(fill="both", expand=True)

        type_var.trace_add("write", _show_hide)
        _show_hide()

        # --- Save state on every var change ---
        def _save(*_args: object) -> None:
            sd["type"] = type_var.get()
            vd: dict[str, str] = {}
            stype = type_var.get()
            if stype in ("local",):
                vd["destination_path"] = local_path_var.get()
            elif stype in ("network",):
                vd["destination_path"] = network_path_var.get()
            elif stype == "sftp":
                for k, v in sftp_vars.items():
                    vd[k] = v.get()
            elif stype == "s3":
                vd["s3_provider"] = s3_provider_var.get()
                for k, v in s3_vars.items():
                    vd[k] = v.get()
            elif stype == "proton":
                for k, v in proton_vars.items():
                    vd[k] = v.get()
            sd["vars"] = vd

        type_var.trace_add("write", _save)
        local_path_var.trace_add("write", _save)
        network_path_var.trace_add("write", _save)
        for v in sftp_vars.values():
            v.trace_add("write", _save)
        s3_provider_var.trace_add("write", _save)
        for v in s3_vars.values():
            v.trace_add("write", _save)
        for v in proton_vars.values():
            v.trace_add("write", _save)

        # Initial save
        _save()

        return {
            "type_var": type_var,
            "config_frames": config_frames,
            "local_path_var": local_path_var,
            "network_path_var": network_path_var,
            "sftp_vars": sftp_vars,
            "s3_vars": s3_vars,
            "s3_provider_var": s3_provider_var,
            "proton_vars": proton_vars,
            "test_btn": test_btn,
            "test_label": test_label,
        }

    # ------------------------------------------------------------------
    # Storage config builder (from vars dict)
    # ------------------------------------------------------------------

    def _build_storage_config_from_key(self, storage_key: str) -> StorageConfig:
        """Build a StorageConfig from self._data[storage_key].

        Args:
            storage_key: Key in self._data ("storage", "mirror1", "mirror2").

        Returns:
            A StorageConfig instance populated from the saved vars.
        """
        sd = self._data[storage_key]
        stype = StorageType(sd["type"])
        vd = sd.get("vars", {})

        # Build with default type first (avoids __post_init__ validation
        # before fields are populated), then set the real type.
        config = StorageConfig()

        if stype in (StorageType.LOCAL, StorageType.NETWORK):
            config.destination_path = vd.get("destination_path", "")
        elif stype == StorageType.SFTP:
            for key, val in vd.items():
                if key == "sftp_port":
                    setattr(config, key, int(val) if val else 22)
                else:
                    setattr(config, key, val)
        elif stype == StorageType.S3:
            config.s3_provider = vd.get("s3_provider", "aws")
            for key, val in vd.items():
                if key != "s3_provider":
                    setattr(config, key, val)
        elif stype == StorageType.PROTON:
            for key, val in vd.items():
                setattr(config, key, val)

        config.storage_type = stype
        return config

    # ------------------------------------------------------------------
    # Test connection (uses BackupEngine._get_backend exactly like tabs)
    # ------------------------------------------------------------------

    def _test_storage_connection(
        self,
        storage_key: str,
        btn: ttk.Button,
        lbl: ttk.Label,
    ) -> None:
        """Test storage connection in a background daemon thread.

        Copies the working pattern from storage_tab.py:
        build StorageConfig, use BackupEngine._get_backend, call
        backend.test_connection(), show result in label.

        Args:
            storage_key: Key in self._data for the storage to test.
            btn: The test button to disable during test.
            lbl: The label to show result in.
        """
        lbl.config(text="Testing...", foreground=Colors.WARNING)
        btn.state(["disabled"])

        # Build config in the main thread (reads tk vars)
        try:
            config = self._build_storage_config_from_key(storage_key)
        except Exception as e:
            self._show_test_result(btn, lbl, False, str(e))
            return

        result: list = [None]  # [None] = pending, [(bool, str)] = done

        def _do_test() -> None:
            try:
                from src.core.backup_engine import BackupEngine

                engine = BackupEngine.__new__(BackupEngine)
                backend = engine._get_backend(config)
                ok, msg = backend.test_connection()
            except Exception as e:
                logger.error("Wizard connection test failed: %s", e, exc_info=True)
                ok, msg = False, str(e)
            result[0] = (ok, msg)

        def _poll() -> None:
            if result[0] is not None:
                ok, msg = result[0]
                self._show_test_result(btn, lbl, ok, msg)
            else:
                self._win.after(200, _poll)

        threading.Thread(target=_do_test, daemon=True).start()
        self._win.after(200, _poll)

    def _show_test_result(
        self,
        btn: ttk.Button,
        lbl: ttk.Label,
        ok: bool,
        msg: str,
    ) -> None:
        """Display connection test result.

        Args:
            btn: Test button to re-enable.
            lbl: Label to show result message.
            ok: Whether the test succeeded.
            msg: Result message to display.
        """
        btn.state(["!disabled"])
        color = Colors.SUCCESS if ok else Colors.DANGER
        lbl.config(text=msg, foreground=color)

    # ------------------------------------------------------------------
    # Proton Drive setup guide
    # ------------------------------------------------------------------

    @staticmethod
    def _add_proton_guide(parent: ttk.Frame) -> None:
        """Add a step-by-step setup guide for Proton Drive.

        Args:
            parent: Parent frame to add the guide to.
        """
        guide_frame = ttk.LabelFrame(parent, text="Setup guide")
        guide_frame.pack(fill="x", pady=(8, 4))

        steps = [
            (
                "1.",
                "Install rclone",
                "Download from https://rclone.org/downloads/\n"
                "Extract the .zip and place rclone.exe in\n"
                "C:\\Program Files\\rclone\\ or add it to your PATH.",
            ),
            (
                "2.",
                "Find your Proton credentials",
                "Use your Proton Mail / Proton account email\n"
                "as username, and your account password.",
            ),
            (
                "3.",
                "2FA seed (optional)",
                "If you have 2FA enabled on your Proton account:\n"
                "Open your authenticator app settings, find the\n"
                "secret key (base32 string) and paste it here.\n"
                "Backup Manager will generate TOTP codes\n"
                "automatically.",
            ),
            (
                "4.",
                "Remote path",
                "The folder on Proton Drive where backups will\n"
                "be stored. Default: /Backups\n"
                "The folder is created automatically if needed.",
            ),
            (
                "5.",
                "Test your connection",
                "Click 'Test connection' below to verify that\n"
                "rclone can reach your Proton Drive account.",
            ),
        ]

        for num, title, detail in steps:
            step_frame = ttk.Frame(guide_frame)
            step_frame.pack(fill="x", padx=6, pady=2)

            header = ttk.Frame(step_frame)
            header.pack(fill="x")
            ttk.Label(
                header,
                text=num,
                foreground=Colors.ACCENT,
                font=("Segoe UI", 9, "bold"),
            ).pack(side="left")
            ttk.Label(
                header,
                text=title,
                font=("Segoe UI", 9, "bold"),
            ).pack(side="left", padx=(4, 0))

            ttk.Label(
                step_frame,
                text=detail,
                foreground=Colors.TEXT_SECONDARY,
                font=("Segoe UI", 8),
                justify="left",
            ).pack(anchor="w", padx=(18, 0))

    # ------------------------------------------------------------------
    # Step 4: Primary storage
    # ------------------------------------------------------------------

    def _step_storage(self) -> None:
        """Display the primary storage destination step."""
        self._set_header("Where to store?")
        ttk.Label(
            self._content,
            text="Choose primary backup destination:",
        ).pack(anchor="w")

        self._build_storage_config_ui(self._content, "storage")

    # ------------------------------------------------------------------
    # Step 5: Mirror 1
    # ------------------------------------------------------------------

    def _step_mirror1(self) -> None:
        """Display the Mirror 1 configuration step."""
        self._build_mirror_step("mirror1", 1)

    # ------------------------------------------------------------------
    # Step 6: Mirror 2
    # ------------------------------------------------------------------

    def _step_mirror2(self) -> None:
        """Display the Mirror 2 configuration step."""
        self._build_mirror_step("mirror2", 2)

    def _build_mirror_step(self, storage_key: str, label_num: int) -> None:
        """Build a mirror configuration step.

        Args:
            storage_key: Key in self._data ("mirror1" or "mirror2").
            label_num: Mirror number for display (1 or 2).
        """
        self._set_header(f"Mirror {label_num} (optional)")

        md = self._data[storage_key]

        # Enable checkbox
        enable_var = tk.BooleanVar(value=md.get("enabled", False))
        ttk.Checkbutton(
            self._content,
            text=f"Enable Mirror {label_num} (beta)",
            variable=enable_var,
        ).pack(anchor="w", pady=(0, Spacing.SMALL))

        # Content frame (enable/disable based on checkbox)
        content = ttk.Frame(self._content)
        content.pack(fill="both", expand=True)

        refs = self._build_storage_config_ui(content, storage_key)

        # Toggle content enable/disable
        def _toggle(*_args: object) -> None:
            md["enabled"] = enable_var.get()
            state = "normal" if enable_var.get() else "disabled"
            _set_children_state(content, state)

        def _set_children_state(widget: tk.Widget, state: str) -> None:
            try:
                widget.configure(state=state)
            except tk.TclError:
                pass
            for child in widget.winfo_children():
                _set_children_state(child, state)

        enable_var.trace_add("write", _toggle)
        _toggle()

    # ------------------------------------------------------------------
    # Step 7: Backup type
    # ------------------------------------------------------------------

    def _step_backup_type(self) -> None:
        """Display the backup type selection step."""
        self._set_header("Backup type")
        self._btype_var = tk.StringVar(value=self._data["backup_type"].value)
        for bt, desc in [
            (BackupType.FULL, "Full \u2014 backs up everything every time"),
            (
                BackupType.INCREMENTAL,
                "Incremental \u2014 only changed files since last backup (beta)",
            ),
            (
                BackupType.DIFFERENTIAL,
                "Differential \u2014 only changed since last full backup (beta)",
            ),
        ]:
            ttk.Radiobutton(
                self._content,
                text=desc,
                value=bt.value,
                variable=self._btype_var,
            ).pack(anchor="w", pady=4)

        self._btype_var.trace_add(
            "write",
            lambda *a: self._data.update(
                backup_type=BackupType(self._btype_var.get()),
            ),
        )

    # ------------------------------------------------------------------
    # Step 8: Retention policy
    # ------------------------------------------------------------------

    def _step_retention(self) -> None:
        """Display the retention policy step."""
        self._set_header("Retention policy")

        gfs_frame = ttk.LabelFrame(
            self._content,
            text="GFS Retention (Grandfather-Father-Son)",
            padding=4,
        )
        gfs_frame.pack(fill="x", pady=(8, 0))

        self._ret_gfs_vars: dict[str, tk.IntVar] = {}
        for label, key, default in [
            ("Daily backups to keep (days):", "gfs_daily", 7),
            ("Weekly backups to keep (weeks):", "gfs_weekly", 4),
            ("Monthly backups to keep (months):", "gfs_monthly", 12),
        ]:
            row = ttk.Frame(gfs_frame)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=label).pack(side="left")
            var = tk.IntVar(value=self._data.get(key, default))
            self._ret_gfs_vars[key] = var
            ttk.Spinbox(
                row,
                textvariable=var,
                from_=1,
                to=999,
                width=8,
            ).pack(side="right")

        for key, var in self._ret_gfs_vars.items():
            var.trace_add(
                "write",
                lambda *a, k=key, v=var: self._data.update({k: v.get()}),
            )

    # ------------------------------------------------------------------
    # Step 9: Encryption
    # ------------------------------------------------------------------

    def _step_encryption(self) -> None:
        """Display the encryption configuration step."""
        self._set_header("Encryption mode (beta)")
        self._enc_updating = False

        # No encryption checkbox
        any_enc = (
            self._data["encrypt_primary"]
            or self._data["encrypt_mirror1"]
            or self._data["encrypt_mirror2"]
        )
        self._enc_no_var = tk.BooleanVar(value=not any_enc)
        self._enc_no_cb = ttk.Checkbutton(
            self._content,
            text="No encryption",
            variable=self._enc_no_var,
            command=self._on_enc_no_toggled,
        )
        self._enc_no_cb.pack(anchor="w", pady=2)

        ttk.Separator(self._content, orient="horizontal").pack(fill="x", pady=4)

        # Encrypt Primary
        self._enc_primary_var = tk.BooleanVar(value=self._data["encrypt_primary"])
        self._enc_primary_cb = ttk.Checkbutton(
            self._content,
            text="Encrypt Primary",
            variable=self._enc_primary_var,
            command=self._on_enc_option_toggled,
        )
        self._enc_primary_cb.pack(anchor="w", pady=2)

        # Encrypt Mirror 1
        self._enc_mirror1_var = tk.BooleanVar(value=self._data["encrypt_mirror1"])
        self._enc_mirror1_cb = ttk.Checkbutton(
            self._content,
            text="Encrypt Mirror 1",
            variable=self._enc_mirror1_var,
            command=self._on_enc_option_toggled,
        )
        self._enc_mirror1_cb.pack(anchor="w", pady=2)

        # Encrypt Mirror 2
        self._enc_mirror2_var = tk.BooleanVar(value=self._data["encrypt_mirror2"])
        self._enc_mirror2_cb = ttk.Checkbutton(
            self._content,
            text="Encrypt Mirror 2",
            variable=self._enc_mirror2_var,
            command=self._on_enc_option_toggled,
        )
        self._enc_mirror2_cb.pack(anchor="w", pady=2)

        # Password frame
        self._enc_pw_frame = ttk.Frame(self._content)
        ttk.Label(
            self._enc_pw_frame,
            text="Password:",
        ).pack(anchor="w", pady=(8, 0))
        self._enc_pw_var = tk.StringVar(value=self._data["encryption_password"])
        ttk.Entry(
            self._enc_pw_frame,
            textvariable=self._enc_pw_var,
            show="\u25cf",
            width=30,
        ).pack(anchor="w")
        ttk.Label(
            self._enc_pw_frame,
            text="Confirm password:",
        ).pack(anchor="w", pady=(8, 0))
        self._enc_pw_confirm_var = tk.StringVar()
        ttk.Entry(
            self._enc_pw_frame,
            textvariable=self._enc_pw_confirm_var,
            show="\u25cf",
            width=30,
        ).pack(anchor="w")
        self._enc_pw_var.trace_add(
            "write",
            lambda *a: self._data.update(
                encryption_password=self._enc_pw_var.get(),
            ),
        )

        self._update_enc_ui_state()

    def _on_enc_no_toggled(self) -> None:
        """Handle 'No encryption' toggled in wizard."""
        if self._enc_updating:
            return
        self._enc_updating = True
        if self._enc_no_var.get():
            self._enc_primary_var.set(False)
            self._enc_mirror1_var.set(False)
            self._enc_mirror2_var.set(False)
        self._sync_enc_data()
        self._update_enc_ui_state()
        self._enc_updating = False

    def _on_enc_option_toggled(self) -> None:
        """Handle any encrypt checkbox toggled in wizard."""
        if self._enc_updating:
            return
        self._enc_updating = True
        any_enc = (
            self._enc_primary_var.get()
            or self._enc_mirror1_var.get()
            or self._enc_mirror2_var.get()
        )
        self._enc_no_var.set(not any_enc)
        self._sync_enc_data()
        self._update_enc_ui_state()
        self._enc_updating = False

    def _sync_enc_data(self) -> None:
        """Sync wizard _data dict from checkbox variables."""
        self._data["encrypt_primary"] = self._enc_primary_var.get()
        self._data["encrypt_mirror1"] = self._enc_mirror1_var.get()
        self._data["encrypt_mirror2"] = self._enc_mirror2_var.get()

    def _update_enc_ui_state(self) -> None:
        """Update password frame visibility."""
        any_enc = (
            self._enc_primary_var.get()
            or self._enc_mirror1_var.get()
            or self._enc_mirror2_var.get()
        )
        if any_enc:
            self._enc_pw_frame.pack(anchor="w", fill="x")
        else:
            self._enc_pw_frame.pack_forget()

    # ------------------------------------------------------------------
    # Step 10: Schedule
    # ------------------------------------------------------------------

    def _step_schedule(self) -> None:
        """Display the schedule configuration step."""
        self._set_header("Schedule")
        self._sched_var = tk.BooleanVar(value=self._data["schedule_enabled"])
        ttk.Checkbutton(
            self._content,
            text="Enable automatic scheduling",
            variable=self._sched_var,
        ).pack(anchor="w")

        ttk.Label(self._content, text="\nFrequency:").pack(anchor="w")
        self._freq_var = tk.StringVar(value=self._data["schedule_freq"].value)
        for f in [
            ScheduleFrequency.DAILY,
            ScheduleFrequency.WEEKLY,
            ScheduleFrequency.MONTHLY,
        ]:
            ttk.Radiobutton(
                self._content,
                text=f.value.capitalize(),
                value=f.value,
                variable=self._freq_var,
            ).pack(anchor="w")

        ttk.Label(self._content, text="\nTime (HH:MM):").pack(anchor="w")
        self._time_var = tk.StringVar(value=self._data["schedule_time"])
        ttk.Entry(
            self._content,
            textvariable=self._time_var,
            width=8,
        ).pack(anchor="w")

        self._sched_var.trace_add(
            "write",
            lambda *a: self._data.update(
                schedule_enabled=self._sched_var.get(),
            ),
        )
        self._freq_var.trace_add(
            "write",
            lambda *a: self._data.update(
                schedule_freq=ScheduleFrequency(self._freq_var.get()),
            ),
        )
        self._time_var.trace_add(
            "write",
            lambda *a: self._data.update(schedule_time=self._time_var.get()),
        )

    # ------------------------------------------------------------------
    # Step 11: Email notifications
    # ------------------------------------------------------------------

    def _step_email(self) -> None:
        """Display the email notification step."""
        self._set_header("Send notification (beta)")

        # Trigger mode
        ttk.Label(self._content, text="Send notifications:").pack(anchor="w")
        self._email_trigger_var = tk.StringVar(
            value=self._data["email_config"].get("trigger", "disabled"),
        )
        for val, label in [
            ("disabled", "Disabled"),
            ("failure", "On failure only"),
            ("success", "On success only"),
            ("always", "Always (success + failure)"),
        ]:
            ttk.Radiobutton(
                self._content,
                text=label,
                value=val,
                variable=self._email_trigger_var,
            ).pack(anchor="w", pady=2)

        self._email_trigger_var.trace_add("write", self._on_email_trigger_changed)

        # SMTP config frame
        self._email_smtp_frame = ttk.LabelFrame(
            self._content,
            text="SMTP server",
            padding=4,
        )

        # Presets
        preset_row = ttk.Frame(self._email_smtp_frame)
        preset_row.pack(fill="x", pady=(0, 4))
        ttk.Label(preset_row, text="Presets:").pack(side="left")
        for name in ["Gmail", "Outlook", "ProtonMail"]:
            ttk.Button(
                preset_row,
                text=name,
                command=lambda n=name.lower(): self._apply_email_preset(n),
            ).pack(side="left", padx=2)

        # Fields
        fields = [
            ("SMTP Host:", "host", ""),
            ("Port:", "port", "587"),
            ("Username:", "username", ""),
            ("Password:", "password", ""),
            ("From address:", "from_addr", ""),
            ("To address:", "to_addr", ""),
        ]

        sc = self._data["email_config"]
        self._email_vars: dict[str, tk.StringVar] = {}
        for label, key, default in fields:
            row = ttk.Frame(self._email_smtp_frame)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=label, width=15).pack(side="left")
            var = tk.StringVar(value=sc.get(key, default))
            self._email_vars[key] = var
            if key == "password":
                ttk.Entry(
                    row,
                    textvariable=var,
                    show="\u25cf",
                ).pack(side="left", fill="x", expand=True)
            elif key == "port":
                ttk.Spinbox(
                    row,
                    textvariable=var,
                    from_=1,
                    to=65535,
                    width=8,
                ).pack(side="left")
            else:
                ttk.Entry(row, textvariable=var).pack(
                    side="left",
                    fill="x",
                    expand=True,
                )

        # TLS
        self._email_tls_var = tk.BooleanVar(value=sc.get("tls", True))
        ttk.Checkbutton(
            self._email_smtp_frame,
            text="Use TLS (STARTTLS)",
            variable=self._email_tls_var,
        ).pack(anchor="w", pady=(2, 0))

        ttk.Label(
            self._email_smtp_frame,
            text="For multiple recipients, separate with commas",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w", pady=(2, 0))

        # Test button
        test_row = ttk.Frame(self._email_smtp_frame)
        test_row.pack(fill="x", pady=(4, 0))
        self._email_test_btn = ttk.Button(
            test_row,
            text="Send test email",
            command=self._test_wizard_email,
        )
        self._email_test_btn.pack(side="left")
        self._email_test_lbl = ttk.Label(
            test_row,
            text="",
            foreground=Colors.TEXT_SECONDARY,
        )
        self._email_test_lbl.pack(side="left", padx=8)

        # Show/hide based on initial trigger
        self._on_email_trigger_changed()

    def _on_email_trigger_changed(self, *args: object) -> None:
        """Show or hide SMTP config based on trigger mode."""
        trigger = self._email_trigger_var.get()
        enabled = trigger != "disabled"
        self._data["email_enabled"] = enabled
        self._data["email_config"]["trigger"] = trigger
        if enabled:
            self._email_smtp_frame.pack(fill="x", pady=(8, 0))
        else:
            self._email_smtp_frame.pack_forget()

    def _apply_email_preset(self, name: str) -> None:
        """Apply an SMTP preset.

        Args:
            name: Preset name (e.g. "gmail", "outlook", "protonmail").
        """
        preset = SMTP_PRESETS.get(name, {})
        self._email_vars["host"].set(preset.get("host", ""))
        self._email_vars["port"].set(str(preset.get("port", 587)))
        self._email_tls_var.set(preset.get("tls", True))

    def _test_wizard_email(self) -> None:
        """Send a test email from the wizard."""
        self._email_test_lbl.config(text="Sending...", foreground=Colors.WARNING)
        self._email_test_btn.state(["disabled"])

        config = self._build_wizard_email_config()
        result: list = [None]

        def _send() -> None:
            try:
                ok, msg = send_test_email(config)
            except Exception as e:
                ok, msg = False, str(e)
            result[0] = (ok, msg)

        def _poll() -> None:
            if result[0] is not None:
                ok, msg = result[0]
                self._email_test_btn.state(["!disabled"])
                color = Colors.SUCCESS if ok else Colors.DANGER
                self._email_test_lbl.config(text=msg, foreground=color)
            else:
                self._win.after(200, _poll)

        threading.Thread(target=_send, daemon=True).start()
        self._win.after(200, _poll)

    def _build_wizard_email_config(self) -> EmailConfig:
        """Build EmailConfig from wizard state.

        Returns:
            Configured EmailConfig instance.
        """
        trigger = self._email_trigger_var.get()
        return EmailConfig(
            enabled=trigger != "disabled",
            smtp_host=self._email_vars["host"].get(),
            smtp_port=int(self._email_vars["port"].get() or 587),
            use_tls=self._email_tls_var.get(),
            username=self._email_vars["username"].get(),
            password=self._email_vars["password"].get(),
            from_address=self._email_vars["from_addr"].get(),
            to_address=self._email_vars["to_addr"].get(),
            send_on_success=trigger in ("success", "always"),
            send_on_failure=trigger in ("failure", "always"),
        )

    # ------------------------------------------------------------------
    # Step 12: Summary
    # ------------------------------------------------------------------

    def _step_summary(self) -> None:
        """Display the summary of all collected settings."""
        self._set_header("Summary")
        d = self._data

        storage_type = d["storage"].get("type", "local")
        enc_parts = []
        if d["encrypt_primary"]:
            enc_parts.append("Primary")
        if d["encrypt_mirror1"]:
            enc_parts.append("Mirror1")
        if d["encrypt_mirror2"]:
            enc_parts.append("Mirror2")
        enc_text = " ".join(enc_parts) if enc_parts else "None"

        summary = (
            f"Profile: {d['name']}\n"
            f"Sources: {len(d['sources'])} items\n"
            f"Destination: {storage_type}\n"
            f"Backup type: {d['backup_type'].value}\n"
            f"Retention: GFS (daily {d.get('gfs_daily', 7)}, "
            f"weekly {d.get('gfs_weekly', 4)}, "
            f"monthly {d.get('gfs_monthly', 12)})\n"
            f"Encryption: {enc_text}\n"
            f"Schedule: {'Enabled' if d['schedule_enabled'] else 'Manual'}\n"
            f"Email: {d['email_config'].get('trigger', 'disabled').capitalize()}\n"
        )
        ttk.Label(
            self._content,
            text=summary,
            font=Fonts.normal(),
            justify="left",
        ).pack(anchor="w")

    # ------------------------------------------------------------------
    # Profile creation
    # ------------------------------------------------------------------

    def _create_profile(self) -> None:
        """Build the BackupProfile from all collected data and close the wizard."""
        d = self._data

        # Primary storage
        storage = self._build_storage_config_from_key("storage")

        # Mirror destinations
        mirrors: list[StorageConfig] = []
        for mk in ("mirror1", "mirror2"):
            md = d[mk]
            if md.get("enabled", False):
                mirrors.append(self._build_storage_config_from_key(mk))

        # Email config
        if hasattr(self, "_email_vars"):
            email = self._build_wizard_email_config()
        else:
            email = EmailConfig(enabled=d["email_enabled"])

        profile = BackupProfile(
            name=d["name"],
            source_paths=d["sources"],
            backup_type=d["backup_type"],
            storage=storage,
            mirror_destinations=mirrors,
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
                gfs_daily=d.get("gfs_daily", 7),
                gfs_weekly=d.get("gfs_weekly", 4),
                gfs_monthly=d.get("gfs_monthly", 12),
            ),
            encrypt_primary=d["encrypt_primary"],
            encrypt_mirror1=d["encrypt_mirror1"],
            encrypt_mirror2=d["encrypt_mirror2"],
            encryption=EncryptionConfig(
                enabled=(d["encrypt_primary"] or d["encrypt_mirror1"] or d["encrypt_mirror2"]),
                stored_password=d["encryption_password"],
            ),
            schedule=ScheduleConfig(
                enabled=d["schedule_enabled"],
                frequency=d["schedule_freq"],
                time=d["schedule_time"],
            ),
            email=email,
        )

        self.result_profile = profile
        self._canvas.unbind_all("<MouseWheel>")
        self._win.destroy()
