"""Setup wizard: 3-step guided profile creation.

Shown on first launch when no profiles exist.
Creates a complete BackupProfile through a multi-step flow.
"""

import logging
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk

from src.core.config import (
    BackupProfile,
    ScheduleConfig,
    ScheduleFrequency,
    StorageConfig,
    StorageType,
)
from src.installer import FEAT_S3, FEAT_SFTP, get_available_features
from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)


class SetupWizard:
    """3-step setup wizard for creating a backup profile."""

    TOTAL_STEPS = 3

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

        self._next_btn.config(text="Next \u2192")

        step_builders = {
            1: self._step_name,
            2: self._step_sources,
            3: self._step_storage,
        }

        builder = step_builders.get(self._step, lambda: None)
        builder()

    def _set_header(self, title: str) -> None:
        """Update the wizard header with step title and counter."""
        self._title_label.config(text=title)
        self._step_label.config(text=f"Step {self._step} of {self.TOTAL_STEPS}")

    def _go_next(self) -> None:
        """Advance to the next step, or create the profile on the last step."""
        # Validate current step before advancing or creating
        error = self._validate_current_step()
        if error:
            from tkinter import messagebox

            messagebox.showwarning("Validation", error, parent=self._win)
            return

        if self._step == self.TOTAL_STEPS:
            self._create_profile()
            return

        self._collect_current_step()
        self._step = min(self._step + 1, self.TOTAL_STEPS)
        self._show_step()

    def _validate_current_step(self) -> str | None:
        """Validate the current step. Returns error message or None."""
        if self._step == 1:
            name = self._data.get("name", "").strip()
            if not name:
                return "Please enter a profile name."
        elif self._step == 2:
            if not self._data.get("sources"):
                return "Please add at least one source folder."
        elif self._step == 3:
            storage = self._data.get("storage", {})
            svars = storage.get("vars", {})
            stype = storage.get("type", "")
            if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
                path = svars.get("destination_path", "")
                if not path:
                    return "Please select a destination folder."
            elif stype == StorageType.SFTP.value:
                host = svars.get("sftp_host", "")
                if not host:
                    return "Please enter a SFTP host."
            elif stype == StorageType.S3.value:
                bucket = svars.get("s3_bucket", "")
                if not bucket:
                    return "Please enter an S3 bucket name."
        return None

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
                "s3_provider_var",
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
            (StorageType.S3, "S3 Cloud Storage", FEAT_S3 in self._features),
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
            "scaleway",
            "wasabi",
            "ovh",
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
            sd["vars"] = vd

        type_var.trace_add("write", _save)
        local_path_var.trace_add("write", _save)
        network_path_var.trace_add("write", _save)
        for v in sftp_vars.values():
            v.trace_add("write", _save)
        s3_provider_var.trace_add("write", _save)
        for v in s3_vars.values():
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
    # Profile creation
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Profile creation
    # ------------------------------------------------------------------

    def _create_profile(self) -> None:
        """Build the BackupProfile from collected data and close the wizard.

        Uses defaults for all settings not configured in the wizard
        (backup type, retention, encryption, mirrors, email).
        """
        d = self._data

        storage = self._build_storage_config_from_key("storage")

        profile = BackupProfile(
            name=d["name"],
            source_paths=d["sources"],
            storage=storage,
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.DAILY,
                time="10:00",
            ),
        )

        self.result_profile = profile
        self._canvas.unbind_all("<MouseWheel>")
        self._win.destroy()
