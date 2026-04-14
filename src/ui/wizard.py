"""Setup wizard: guided profile creation with personal/professional modes.

Shown on first launch when no profiles exist.

Two paths:
  - **Personal**: 3-step local/network/SFTP/S3 backup (unchanged).
  - **Professional**: 9-step S3 Object Lock anti-ransomware setup with
    guided AWS account creation, cost simulation, disclaimers, and
    automatic bucket provisioning.
"""

import logging
import sys
import threading
import tkinter as tk
import uuid
import webbrowser
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from src.core.config import (
    BackupProfile,
    BackupType,
    EncryptionConfig,
    RetentionConfig,
    RetentionPolicy,
    ScheduleConfig,
    ScheduleFrequency,
    StorageConfig,
    StorageType,
)
from src.installer import FEAT_S3, FEAT_SFTP, get_available_features
from src.storage.s3_setup import (
    GLACIER_IR_PRICE_PER_GB,
    REQUIRED_IAM_POLICY,
    RETENTION_OPTIONS,
    S3ObjectLockSetup,
    detect_local_currency,
    estimate_total_cost,
    format_cost,
)
from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)

# Wizard mode constants
MODE_CHOICE = "choice"
MODE_PERSONAL = "personal"
MODE_PROFESSIONAL = "professional"


def _resolve_retention(data: dict) -> tuple[str, int, int]:
    """Resolve the selected retention option from wizard data.

    Handles both predefined options and custom year input.

    Args:
        data: Wizard data dict with pro_retention_idx and pro_custom_years.

    Returns:
        (label, months, days) tuple.
    """
    idx = data.get("pro_retention_idx", 2)
    if idx < len(RETENTION_OPTIONS):
        return RETENTION_OPTIONS[idx]
    # Custom duration
    years = data.get("pro_custom_years", 2)
    months = years * 12
    days = years * 365
    return f"{years} years (custom)", months, days


class SetupWizard:
    """Setup wizard with personal/professional paths."""

    def __init__(self, parent: tk.Tk = None, standalone: bool = False):
        self.result_profile: BackupProfile | None = None
        self._parent = parent
        self._standalone = standalone
        self._mode = MODE_CHOICE
        self._step = 0  # 0 = mode choice screen
        self._total_steps = 3  # Updated when mode is chosen
        self._features = get_available_features()

        # Profile data collected across steps
        self._data: dict = {
            "name": "My Backup",
            "sources": [],
            "storage": {
                "type": StorageType.LOCAL.value,
                "vars": {},
            },
            # Professional mode extras
            "pro_aws_key": "",
            "pro_aws_secret": "",
            "pro_region": "eu-west-1",
            "pro_bucket": "",
            "pro_retention_idx": 2,  # Default: 13 months
            "pro_encrypt": False,
            "pro_encrypt_password": "",
            "pro_mirror_local": False,
            "pro_mirror_path": "",
        }

        self._build_window()

    def _build_window(self) -> None:
        """Build the wizard window, header, progress bar, content area, and footer."""
        self._win = tk.Toplevel(self._parent) if self._parent else tk.Tk()
        self._win.title("Backup Manager \u2014 Setup Wizard")
        win_w, win_h = 1000, 780
        screen_w = self._win.winfo_screenwidth()
        screen_h = self._win.winfo_screenheight()
        x = (screen_w - win_w) // 2
        y = (screen_h - win_h) // 2
        self._win.geometry(f"{win_w}x{win_h}+{x}+{y}")
        self._win.resizable(False, False)

        # Set window icon
        if getattr(sys, "frozen", False):
            if hasattr(sys, "_MEIPASS"):
                base = Path(sys._MEIPASS)  # noqa: SLF001
            else:
                base = Path(__file__).resolve().parent.parent.parent
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

        if self._parent and not self._standalone:
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
            maximum=self._total_steps,
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
        """Display the current step based on the active mode."""
        for w in self._content.winfo_children():
            w.destroy()

        # Reset scroll to top
        self._canvas.yview_moveto(0)

        if self._step == 0:
            # Mode choice screen — hide progress and back
            self._progress["value"] = 0
            self._back_btn.state(["disabled"])
            self._next_btn.pack_forget()
            self._step_mode_choice()
            return

        self._next_btn.pack(side="right")
        self._progress["maximum"] = self._total_steps
        self._progress["value"] = self._step
        self._back_btn.state(["!disabled"])
        self._next_btn.config(text="Next \u2192")

        if self._mode == MODE_PERSONAL:
            builders = {
                1: self._step_name,
                2: self._step_sources,
                3: self._step_storage,
            }
        else:
            builders = {
                1: self._step_pro_protection_info,
                2: self._step_pro_retention_choice,
                3: self._step_pro_backup_strategy,
                4: self._step_pro_cost_simulation,
                5: self._step_pro_disclaimers,
                6: self._step_pro_aws_guide,
                7: self._step_name,
                8: self._step_sources,
                9: self._step_pro_encryption,
                10: self._step_pro_local_mirror,
                11: self._step_pro_auto_setup,
            }

        builder = builders.get(self._step, lambda: None)
        builder()

    def _set_header(self, title: str) -> None:
        """Update the wizard header with step title and counter."""
        self._title_label.config(text=title)
        self._step_label.config(text=f"Step {self._step} of {self._total_steps}")

    def _go_next(self) -> None:
        """Advance to the next step, or create the profile on the last step."""
        error = self._validate_current_step()
        if error:
            messagebox.showwarning("Validation", error, parent=self._win)
            return

        if self._step == self._total_steps:
            if self._mode == MODE_PERSONAL:
                self._create_profile()
            else:
                self._create_pro_profile()
            return

        self._collect_current_step()
        self._step = min(self._step + 1, self._total_steps)
        self._show_step()

    def _validate_current_step(self) -> str | None:
        """Validate the current step. Returns error message or None."""
        if self._mode == MODE_PERSONAL:
            return self._validate_personal_step()
        return self._validate_pro_step()

    def _validate_personal_step(self) -> str | None:
        """Validate personal (classic) mode steps."""
        if self._step == 1:
            name = self._data.get("name", "").strip()
            if not name:
                return "Please enter a profile name."
        elif self._step == 2:
            if not self._data.get("sources"):
                return "Please add at least one source folder."
        elif self._step == 3:
            return self._validate_personal_storage()
        return None

    def _validate_personal_storage(self) -> str | None:
        """Validate the personal storage step."""
        storage = self._data.get("storage", {})
        svars = storage.get("vars", {})
        stype = storage.get("type", "")
        if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            if not svars.get("destination_path", ""):
                return "Please select a destination folder."
        elif stype == StorageType.SFTP.value:
            if not svars.get("sftp_host", ""):
                return "Please enter a SFTP host."
        elif stype == StorageType.S3.value and not svars.get("s3_bucket", ""):
            return "Please enter an S3 bucket name."
        return None

    def _validate_pro_step(self) -> str | None:
        """Validate professional mode steps.

        Step order: 1=info, 2=strategy, 3=retention, 4=cost,
        5=disclaimers, 6=AWS keys, 7=name, 8=sources,
        9=encryption, 10=local mirror, 11=setup.
        """
        if self._step == 6:
            if not self._data.get("pro_aws_key", "").strip():
                return "Please enter your Amazon AWS Access Key."
            if not self._data.get("pro_aws_secret", "").strip():
                return "Please enter your Amazon AWS Secret Key."
        elif self._step == 7:
            name = self._data.get("name", "").strip()
            if not name:
                return "Please enter a profile name."
        elif self._step == 8:
            if not self._data.get("sources"):
                return "Please add at least one source folder."
        elif self._step == 9:
            if self._data.get("pro_encrypt"):
                pw = self._data.get("pro_encrypt_password", "")
                if not pw or len(pw) < 8:
                    return "Encryption password must be at least 8 characters."
        return None

    def _go_back(self) -> None:
        """Go back to the previous step."""
        if self._step <= 1:
            self._mode = MODE_CHOICE
            self._step = 0
        else:
            self._step -= 1
            # Skip encryption step (9) going back if retention > 13 months
            if self._mode == MODE_PROFESSIONAL and self._step == 9:
                _label, months, _days = _resolve_retention(self._data)
                if months > 13:
                    self._step = 8
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
    # Step 0: Mode choice (Personal / Professional)
    # ------------------------------------------------------------------

    def _step_mode_choice(self) -> None:
        """Display the mode selection screen with two cards."""
        self._set_header("Choose your backup mode")
        self._step_label.config(text="")

        ttk.Label(
            self._content,
            text="How would you like to protect your data?",
            font=Fonts.large(),
        ).pack(pady=(Spacing.XLARGE, Spacing.LARGE))

        cards = ttk.Frame(self._content)
        cards.pack(fill="both", expand=True, pady=Spacing.MEDIUM)
        cards.columnconfigure(0, weight=1)
        cards.columnconfigure(1, weight=1)

        # Classic card
        personal = ttk.LabelFrame(cards, text="", padding=Spacing.XLARGE)
        personal.grid(row=0, column=0, padx=Spacing.MEDIUM, sticky="nsew")

        tk.Label(
            personal,
            text="\U0001f3e0",
            font=("Segoe UI", 40),
        ).pack(pady=(0, Spacing.MEDIUM))
        ttk.Label(
            personal,
            text="Classic",
            font=Fonts.title(),
        ).pack()
        ttk.Label(
            personal,
            text="Backup to external drive,\nnetwork share, SSH server\nor S3 cloud storage.\n\n"
            "Simple and fast setup.",
            justify="center",
        ).pack(pady=Spacing.MEDIUM)
        ttk.Button(
            personal,
            text="Choose Classic",
            style="Accent.TButton",
            command=lambda: self._select_mode(MODE_PERSONAL),
        ).pack(pady=Spacing.MEDIUM)

        # Anti-Ransomware card
        pro = ttk.LabelFrame(cards, text="", padding=Spacing.XLARGE)
        pro.grid(row=0, column=1, padx=Spacing.MEDIUM, sticky="nsew")

        tk.Label(
            pro,
            text="\U0001f6e1",
            font=("Segoe UI", 40),
            anchor="center",
        ).pack(pady=(0, Spacing.MEDIUM), fill="x")
        ttk.Label(
            pro,
            text="Anti-Ransomware",
            font=Fonts.title(),
        ).pack()
        ttk.Label(
            pro,
            text="High Security",
            font=Fonts.title(),
        ).pack()
        ttk.Label(
            pro,
            text="Backup to Amazon AWS S3\nserver with Object Lock.\n\n"
            "Your data is IMMUTABLE\nand impossible to delete.",
            justify="center",
        ).pack(pady=Spacing.MEDIUM)

        s3_available = FEAT_S3 in self._features
        pro_btn = ttk.Button(
            pro,
            text="Choose Anti-Ransomware",
            style="Accent.TButton",
            command=lambda: self._select_mode(MODE_PROFESSIONAL),
            state="normal" if s3_available else "disabled",
        )
        pro_btn.pack(pady=Spacing.MEDIUM)

        if not s3_available:
            ttk.Label(
                pro,
                text="Requires boto3 (pip install boto3)",
                foreground=Colors.DANGER,
                font=Fonts.small(),
            ).pack()

    def _select_mode(self, mode: str) -> None:
        """Set the wizard mode and start the flow."""
        self._mode = mode
        if mode == MODE_PERSONAL:
            self._total_steps = 3
        else:
            self._total_steps = 11
            # Auto-detect nearest AWS region in background (no UI freeze)
            self._data["pro_region"] = "eu-west-1"  # Default until detected

            # Detect region and currency in background
            self._data["pro_currency"] = ("$", 1.0)

            def _detect_region_and_currency() -> None:
                from src.storage.s3_setup import detect_nearest_region

                self._data["pro_region"] = detect_nearest_region()
                self._data["pro_currency"] = detect_local_currency()

            threading.Thread(
                target=_detect_region_and_currency,
                daemon=True,
            ).start()
        self._step = 1
        self._show_step()

    # ------------------------------------------------------------------
    # Step 1: Profile name (shared)
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
        s3_provider_var = tk.StringVar(value=saved.get("s3_provider", "Amazon AWS"))
        providers = [
            "Amazon AWS",
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
            config.s3_provider = vd.get("s3_provider", "Amazon AWS")
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
    # Professional steps (3-9)
    # ------------------------------------------------------------------

    def _step_pro_aws_guide(self) -> None:
        """Step 3 (pro): Guide for creating an AWS account + enter keys."""
        self._set_header("Amazon AWS Account Setup")

        ttk.Label(
            self._content,
            text="Follow these steps to create your Amazon AWS account:",
            font=Fonts.large(),
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        steps = [
            (
                "1. Create an Amazon AWS account (skip if you already have one)",
                "https://aws.amazon.com/free/",
                "Go to the link below and click 'Create an AWS Account'.\n"
                "You will need an email address, a password, and a credit card.\n"
                "The Free Tier is sufficient \u2014 you only pay for the storage you use.",
            ),
            (
                "2. Create the security policy",
                "https://console.aws.amazon.com/iam/home#/policies/create",
                "Go to the link below. You will see 'Specify permissions' page.\n"
                "In the 'Policy editor' section, click on the 'JSON' tab.\n"
                "Select all the default text in the editor (Ctrl+A) and delete it.\n"
                "Copy the policy displayed below and paste it (Ctrl+V).\n"
                "Click the 'Next' button at the bottom right.\n"
                "In the 'Policy name' field, type: BackupManagerPolicy\n"
                "Leave the 'Description' field empty.\n"
                "Click 'Create policy'.",
            ),
            (
                "3. Create the user and attach the policy",
                "https://console.aws.amazon.com/iam/home#/users/create",
                "Go to the link below. You will see 'Specify user details' page.\n"
                "In the 'User name' field, type: BackupManager\n"
                "Do NOT check 'Provide user access to the AWS Management Console'.\n"
                "Click 'Next'.\n"
                "In 'Permissions options', select 'Attach policies directly'.\n"
                "In the 'Search' box under 'Permissions policies', "
                "type: BackupManagerPolicy\n"
                "Check the box next to 'BackupManagerPolicy' (1 match).\n"
                "Click 'Next', then click 'Create user'.",
            ),
            (
                "4. Generate Access Keys",
                "https://console.aws.amazon.com/iam/home#/users",
                "In the 'Users' list, click on 'BackupManager'.\n"
                "Click on the 'Security credentials' tab.\n"
                "In the 'Access keys' section, click 'Create access key'.\n"
                "Select 'Application running outside AWS'.\n"
                "Click 'Next', then click 'Create access key'.\n"
                "You will see the 'Retrieve access keys' page.\n"
                "Copy the 'Access key' and click 'Show' next to the "
                "'Secret access key' to reveal it, then copy it too.\n"
                "Paste both keys in the fields below.\n"
                "WARNING: The Secret access key will only be shown once.",
            ),
        ]

        for i, (title, url, desc) in enumerate(steps):
            f = ttk.LabelFrame(self._content, text="", padding=Spacing.PAD)
            f.pack(fill="x", pady=Spacing.SMALL)
            ttk.Label(f, text=title, font=Fonts.bold()).pack(anchor="w")
            ttk.Label(
                f,
                text=desc,
                foreground=Colors.TEXT_SECONDARY,
                wraplength=900,
                justify="left",
            ).pack(anchor="w", pady=(Spacing.SMALL, 0))
            if url:
                link = ttk.Label(
                    f,
                    text=url,
                    foreground=Colors.ACCENT,
                    cursor="hand2",
                )
                link.pack(anchor="w", pady=(Spacing.SMALL, 0))
                link.bind("<Button-1>", lambda e, u=url: webbrowser.open(u))

            # Insert the IAM policy right after step 2 (index 1)
            if i == 1:
                policy_frame = ttk.LabelFrame(
                    f,
                    text="Policy to copy and paste in the JSON editor",
                    padding=Spacing.PAD,
                )
                policy_frame.pack(fill="x", pady=(Spacing.MEDIUM, 0))

                policy_text = tk.Text(
                    policy_frame,
                    height=10,
                    font=Fonts.mono(),
                    wrap="word",
                )
                policy_text.insert("1.0", REQUIRED_IAM_POLICY)
                policy_text.config(state="disabled")
                policy_text.pack(fill="x")

                def _copy_policy() -> None:
                    self._win.clipboard_clear()
                    self._win.clipboard_append(REQUIRED_IAM_POLICY)

                ttk.Button(
                    policy_frame,
                    text="Copy to clipboard",
                    command=_copy_policy,
                ).pack(anchor="w", pady=(Spacing.SMALL, 0))
        # Credentials input
        cred_frame = ttk.LabelFrame(
            self._content,
            text="Your Amazon AWS Credentials",
            padding=Spacing.PAD,
        )
        cred_frame.pack(fill="x", pady=Spacing.MEDIUM)

        ttk.Label(cred_frame, text="Access Key:").pack(anchor="w")
        key_var = tk.StringVar(value=self._data.get("pro_aws_key", ""))
        ttk.Entry(cred_frame, textvariable=key_var, show="\u25cf").pack(fill="x")
        key_var.trace_add(
            "write",
            lambda *a: self._data.update(pro_aws_key=key_var.get()),
        )

        ttk.Label(cred_frame, text="Secret Key:").pack(
            anchor="w",
            pady=(Spacing.SMALL, 0),
        )
        secret_var = tk.StringVar(value=self._data.get("pro_aws_secret", ""))
        ttk.Entry(cred_frame, textvariable=secret_var, show="\u25cf").pack(fill="x")
        secret_var.trace_add(
            "write",
            lambda *a: self._data.update(pro_aws_secret=secret_var.get()),
        )

        # Test credentials button
        test_frame = ttk.Frame(self._content)
        test_frame.pack(fill="x", pady=Spacing.SMALL)
        test_lbl = ttk.Label(test_frame, text="")
        test_btn = ttk.Button(
            test_frame,
            text="Test credentials",
            command=lambda: self._test_aws_credentials(test_btn, test_lbl),
        )
        test_btn.pack(side="left")
        test_lbl.pack(side="left", padx=Spacing.MEDIUM)

    def _test_aws_credentials(self, btn: ttk.Button, lbl: ttk.Label) -> None:
        """Test AWS credentials in background thread."""
        btn.state(["disabled"])
        lbl.config(text="Testing...", foreground=Colors.WARNING)

        ak = self._data.get("pro_aws_key", "")
        sk = self._data.get("pro_aws_secret", "")
        region = self._data.get("pro_region", "eu-west-1")
        result: list = [None]

        def _do() -> None:
            try:
                setup = S3ObjectLockSetup(ak, sk, region)
                result[0] = setup.validate_credentials()
            except Exception as e:
                result[0] = (False, str(e))

        def _poll() -> None:
            if result[0] is not None:
                ok, msg = result[0]
                btn.state(["!disabled"])
                lbl.config(
                    text=msg,
                    foreground=Colors.SUCCESS if ok else Colors.DANGER,
                )
            else:
                self._win.after(200, _poll)

        threading.Thread(target=_do, daemon=True).start()
        self._win.after(200, _poll)

    def _step_pro_protection_info(self) -> None:
        """Step 1 (pro): Explain what we will do and why."""
        self._set_header("Anti-Ransomware Protection")

        # Introduction
        ttk.Label(
            self._content,
            text="What we are going to set up together:",
            font=Fonts.large(),
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        info_texts = [
            (
                "The problem",
                "Ransomware, hackers, and human errors can destroy your data "
                "at any time. A simple backup on an external drive is not enough: "
                "if the drive is connected when the attack happens, your backups "
                "are destroyed too. Moreover, the most sophisticated ransomware "
                "does not activate immediately \u2014 it waits for you to run a "
                "backup, thereby contaminating your backups as well.",
            ),
            (
                "The solution: immutable cloud backups",
                "We will store your backups on Amazon AWS S3 with Object Lock "
                "technology. Once uploaded, your data becomes INDESTRUCTIBLE "
                "for the duration you choose (from 1 month to 13 years).\n\n"
                "No one can delete or modify your data during this period \u2014 "
                "not you, not a hacker, not even Amazon.",
            ),
            (
                "What this wizard will do",
                "1. Offer you different protection durations and the estimated "
                "storage cost based on the duration you select and the "
                "amount of data you want to secure\n"
                "2. Help you create an Amazon AWS account\n"
                "3. Set up your backup profile (name, folders to protect)\n"
                "4. Automatically create and lock your backup vault\n\n"
                "Everything is guided step by step. No technical knowledge required.",
            ),
            (
                "Why this is the best method",
                "This is the same technology used by banks, hospitals, and "
                "governments to protect critical data. Object Lock Compliance "
                "mode is the highest level of data protection available "
                "in the cloud. Your data is guaranteed to survive any attack "
                "during the entire retention period.",
            ),
        ]

        for title, text in info_texts:
            f = ttk.LabelFrame(self._content, text=title, padding=Spacing.PAD)
            f.pack(fill="x", pady=Spacing.SMALL)
            ttk.Label(f, text=text, wraplength=900, justify="left").pack(anchor="w")

    def _step_pro_backup_strategy(self) -> None:
        """Step 2 (pro): Explain the backup strategy."""
        self._set_header("Backup Strategy")

        ttk.Label(
            self._content,
            text="How Backup Manager protects your data:",
            font=Fonts.large(),
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        strategy_items = [
            (
                "Monthly full backup",
                "Once a month, Backup Manager performs a full backup of all "
                "your data. This backup is self-contained and is sufficient "
                "on its own to restore all your files.",
            ),
            (
                "Daily differential backup",
                "Every day, Backup Manager backs up only the files that have "
                "been modified since the last full backup. This significantly "
                "reduces the volume of data sent and storage costs.",
            ),
            (
                "Retention and cleanup",
                "Monthly full backups are kept for the entire retention "
                "period that you will choose in the next step. "
                "Daily differential backups are kept for 1 month, then "
                "automatically deleted by Amazon AWS. "
                "You have nothing to manage.",
            ),
            (
                "Fully automatic",
                "Once configured, Backup Manager runs in the background "
                "without any intervention on your part. You can continue "
                "working normally.",
            ),
        ]

        for title, text in strategy_items:
            f = ttk.LabelFrame(self._content, text=title, padding=Spacing.PAD)
            f.pack(fill="x", pady=Spacing.SMALL)
            ttk.Label(
                f,
                text=text,
                wraplength=900,
                foreground=Colors.TEXT_SECONDARY,
                justify="left",
            ).pack(anchor="w")

    def _step_pro_retention_choice(self) -> None:
        """Step 3 (pro): Choose retention duration."""
        self._set_header("Retention Duration")

        ttk.Label(
            self._content,
            text="How long should your backups be protected?",
            font=Fonts.large(),
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        ttk.Label(
            self._content,
            text=(
                "Choose the duration during which your data will be "
                "impossible to delete. This is the core of your "
                "anti-ransomware protection."
            ),
            wraplength=900,
            foreground=Colors.TEXT_SECONDARY,
        ).pack(anchor="w", pady=(0, Spacing.SMALL))

        # Recommendation based on ransomware dwell time
        tip = ttk.LabelFrame(
            self._content,
            text="Good to know",
            padding=Spacing.PAD,
        )
        tip.pack(fill="x", pady=(0, Spacing.LARGE))
        ttk.Label(
            tip,
            text=(
                "Since the standard recommended data retention period is "
                "3 months, sophisticated ransomware waits 3 months and one "
                "day before activating. Keeping your data for at least "
                "4 months can be useful to restore a version that predates "
                "the infection. A retention period of 13 months protects "
                "even better."
            ),
            wraplength=880,
            foreground=Colors.TEXT_SECONDARY,
            justify="left",
        ).pack(anchor="w")

        # Warning — always visible, in red, before choices
        ttk.Label(
            self._content,
            text=(
                "\u26a0 Because data is impossible to delete during the "
                "retention period, you will be billed by Amazon AWS for "
                "the entire duration. Neither you, nor Backup Manager, "
                "nor Amazon can cancel this commitment. "
                "This decision will be irreversible after you configure "
                "your Amazon AWS account."
            ),
            wraplength=900,
            foreground=Colors.DANGER,
        ).pack(fill="x", pady=(0, Spacing.MEDIUM))

        ret_var = tk.IntVar(value=self._data.get("pro_retention_idx", 2))
        custom_idx = len(RETENTION_OPTIONS)  # Index for "Custom" option

        for i, (label, _months, _days) in enumerate(RETENTION_OPTIONS):
            ttk.Radiobutton(
                self._content,
                text=label,
                value=i,
                variable=ret_var,
            ).pack(anchor="w", pady=2)

        # Custom duration option
        custom_frame = ttk.Frame(self._content)
        custom_frame.pack(anchor="w", pady=2)
        ttk.Radiobutton(
            custom_frame,
            text="Custom:",
            value=custom_idx,
            variable=ret_var,
        ).pack(side="left")
        custom_years_var = tk.IntVar(
            value=self._data.get("pro_custom_years", 2),
        )
        custom_spin = ttk.Spinbox(
            custom_frame,
            textvariable=custom_years_var,
            from_=2,
            to=20,
            width=4,
        )
        custom_spin.pack(side="left", padx=Spacing.SMALL)
        ttk.Label(custom_frame, text="years").pack(side="left")

        def _on_retention_change(*_a: object) -> None:
            idx = ret_var.get()
            self._data["pro_retention_idx"] = idx
            if idx == custom_idx:
                years = custom_years_var.get()
                self._data["pro_custom_years"] = years

        ret_var.trace_add("write", _on_retention_change)
        custom_years_var.trace_add("write", _on_retention_change)

    def _step_pro_s3_config(self) -> None:
        """Step 9 (pro): S3 region and bucket name."""
        self._set_header("S3 Configuration")

        from src.storage.s3 import PROVIDER_REGIONS

        # Region
        ttk.Label(self._content, text="Amazon AWS Region:", font=Fonts.bold()).pack(
            anchor="w",
        )
        region_var = tk.StringVar(value=self._data.get("pro_region", "eu-west-1"))
        regions = PROVIDER_REGIONS.get("Amazon AWS", ["eu-west-1"])
        ttk.Combobox(
            self._content,
            textvariable=region_var,
            values=regions,
            state="readonly",
        ).pack(fill="x", pady=(0, Spacing.MEDIUM))
        region_var.trace_add(
            "write",
            lambda *a: self._data.update(pro_region=region_var.get()),
        )

        # Bucket name — auto-generated from profile name
        import re

        profile_name = self._data.get("name", "backup")
        sanitized = re.sub(r"[^a-z0-9-]", "-", profile_name.lower()).strip("-")[:30]
        short_id = uuid.uuid4().hex[:6]
        auto_bucket = f"bm-{sanitized}-{short_id}" if sanitized else f"bm-{short_id}"
        if not self._data.get("pro_bucket") or self._data["pro_bucket"].startswith("bm-"):
            self._data["pro_bucket"] = auto_bucket

        ttk.Label(
            self._content,
            text="Bucket name (auto-generated, you can modify if needed):",
            font=Fonts.bold(),
        ).pack(anchor="w", pady=(Spacing.MEDIUM, 0))
        bucket_var = tk.StringVar(value=self._data.get("pro_bucket", ""))
        ttk.Entry(self._content, textvariable=bucket_var).pack(fill="x")
        ttk.Label(
            self._content,
            text="Must be globally unique across all Amazon AWS accounts.",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w")
        bucket_var.trace_add(
            "write",
            lambda *a: self._data.update(pro_bucket=bucket_var.get()),
        )

    def _step_pro_cost_simulation(self) -> None:
        """Step 3 (pro): Total cost simulation for all durations."""
        self._set_header("Cost Simulation")

        region = self._data.get("pro_region", "eu-west-1")
        cur_symbol, cur_rate = self._data.get("pro_currency", ("$", 1.0))

        ttk.Label(
            self._content,
            text="Estimated total cost by retention duration and data size:",
            font=Fonts.large(),
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        ttk.Label(
            self._content,
            text=(
                "This simulation includes monthly full backups and daily "
                "differential backups (only changed files). The cost increases "
                "progressively as backups accumulate, then stabilizes."
            ),
            foreground=Colors.TEXT_SECONDARY,
            wraplength=900,
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        # Full table: all durations × all sizes (total cost only)
        table = ttk.Frame(self._content)
        table.pack(fill="x", pady=Spacing.MEDIUM)

        sizes = [10, 50, 100, 200]

        # Header row
        ttk.Label(table, text="Retention", font=Fonts.bold()).grid(
            row=0,
            column=0,
            padx=Spacing.MEDIUM,
            pady=Spacing.SMALL,
            sticky="w",
        )
        for col, size_gb in enumerate(sizes, start=1):
            ttk.Label(table, text=f"{size_gb} GB", font=Fonts.bold()).grid(
                row=0,
                column=col,
                padx=Spacing.MEDIUM,
                pady=Spacing.SMALL,
            )

        # Data rows — predefined options + custom if selected
        all_options = list(RETENTION_OPTIONS)
        selected = _resolve_retention(self._data)
        # Add custom row if not a predefined option
        if self._data.get("pro_retention_idx", 0) >= len(RETENTION_OPTIONS):
            all_options.append(selected)

        for row, (label, months, _days) in enumerate(all_options, start=1):
            ttk.Label(table, text=label).grid(
                row=row,
                column=0,
                padx=Spacing.MEDIUM,
                pady=2,
                sticky="w",
            )
            for col, size_gb in enumerate(sizes, start=1):
                total = estimate_total_cost(size_gb, region, months)
                cost_text = format_cost(total, cur_symbol, cur_rate)
                ttk.Label(table, text=cost_text).grid(
                    row=row,
                    column=col,
                    padx=Spacing.MEDIUM,
                    pady=2,
                )

        # Pricing source
        price = GLACIER_IR_PRICE_PER_GB.get(region, 0.004)
        ttk.Label(
            self._content,
            text=(
                f"Pricing based on Amazon AWS S3 Glacier Instant Retrieval "
                f"(~${price}/GB/month) as of April 2026."
            ),
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w", pady=(Spacing.MEDIUM, 0))

        # Disclaimer
        ttk.Label(
            self._content,
            text=(
                "This is an indicative and informative simulation. "
                "Amazon AWS pricing may vary. This is not a contractual commitment. "
                "Actual costs depend on your usage and Amazon AWS pricing conditions."
            ),
            wraplength=900,
            foreground=Colors.DANGER,
            font=Fonts.small(),
        ).pack(fill="x")

    def _step_pro_encryption(self) -> None:
        """Step 8 (pro): Optional client-side encryption.

        Skipped automatically if retention > 13 months.
        AWS already encrypts server-side; disabling BM encryption
        allows direct file access via the AWS Console.
        """
        # Auto-skip for long retention periods
        _label, months, _days = _resolve_retention(self._data)
        if months > 13:
            self._data["pro_encrypt"] = False
            self._step += 1
            self._show_step()
            return

        self._set_header("Encryption (Optional)")

        # Explanation
        info = [
            (
                "Amazon AWS already encrypts your data",
                "Amazon AWS automatically encrypts all data stored on S3 "
                "(server-side encryption). Your backups are protected "
                "by default without any action on your part.",
                Colors.TEXT_SECONDARY,
            ),
            (
                "Without Backup Manager encryption",
                "You can access and restore your files directly from "
                "the Amazon S3 web console, without needing Backup "
                "Manager. This can be useful if you need to recover "
                "data independently.",
                Colors.TEXT_SECONDARY,
            ),
            (
                "With Backup Manager encryption (AES-256)",
                "Your files are double-encrypted for maximum "
                "confidentiality. Nobody \u2014 including Amazon \u2014 can "
                "read the content of your files. However, you will "
                "ALWAYS need Backup Manager AND your password to "
                "restore your data.",
                Colors.TEXT_SECONDARY,
            ),
        ]

        for title, text, color in info:
            f = ttk.LabelFrame(self._content, text=title, padding=Spacing.PAD)
            f.pack(fill="x", pady=Spacing.SMALL)
            ttk.Label(
                f,
                text=text,
                wraplength=900,
                foreground=color,
                justify="left",
            ).pack(anchor="w")

        # Toggle
        encrypt_var = tk.BooleanVar(value=self._data.get("pro_encrypt", False))

        pw_frame = ttk.LabelFrame(
            self._content,
            text="Encryption password",
            padding=Spacing.PAD,
        )

        def _toggle(*_a: object) -> None:
            self._data["pro_encrypt"] = encrypt_var.get()
            if encrypt_var.get():
                pw_frame.pack(fill="x", pady=Spacing.SMALL)
            else:
                pw_frame.pack_forget()

        ttk.Checkbutton(
            self._content,
            text="Enable Backup Manager encryption (AES-256)",
            variable=encrypt_var,
            command=_toggle,
        ).pack(anchor="w", pady=Spacing.MEDIUM)

        # Password fields
        ttk.Label(pw_frame, text="Password:").pack(anchor="w")
        pw_var = tk.StringVar(value=self._data.get("pro_encrypt_password", ""))
        ttk.Entry(pw_frame, textvariable=pw_var, show="\u25cf").pack(fill="x")
        pw_var.trace_add(
            "write",
            lambda *a: self._data.update(pro_encrypt_password=pw_var.get()),
        )

        ttk.Label(pw_frame, text="Confirm password:").pack(
            anchor="w",
            pady=(Spacing.SMALL, 0),
        )
        pw2_var = tk.StringVar()
        ttk.Entry(pw_frame, textvariable=pw2_var, show="\u25cf").pack(fill="x")

        _toggle()

    def _step_pro_disclaimers(self) -> None:
        """Step 4 (pro): Informational disclaimers (no acceptance required)."""
        self._set_header("Important Information")

        label, _months, _days = _resolve_retention(self._data)

        ttk.Label(
            self._content,
            text="Please read the following information before continuing:",
            font=Fonts.large(),
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        notices = [
            (
                "Data immutability",
                f"Data backed up with Object Lock will be IMPOSSIBLE to delete "
                f"before the expiration of the chosen retention period ({label}).",
            ),
            (
                "Amazon AWS billing commitment",
                f"Amazon Web Services will bill you for the storage during "
                f"the entire lock duration ({label}). Even if you stop using "
                f"Backup Manager, already locked data will remain billed "
                f"by Amazon AWS until expiration.",
            ),
            (
                "No early unlock possible",
                "No one \u2014 not you, not Backup Manager, not even Amazon \u2014 "
                "can unlock your data before expiration. In case of "
                "configuration error, data remains locked and billed "
                "for the entire duration.",
            ),
            (
                "Cost estimate",
                "The cost simulation presented earlier is indicative. "
                "Actual costs depend on your usage and current Amazon AWS "
                "pricing conditions.",
            ),
            (
                "Backup Manager liability",
                "Backup Manager provides this information for informational "
                "purposes only. The use of Amazon AWS S3 is subject to "
                "Amazon Web Services terms and conditions. Backup Manager "
                "provides no guarantee regarding data protection, recovery, "
                "or the effectiveness of the chosen retention duration "
                "against any specific threat. Backup Manager cannot be held "
                "responsible for data loss, costs related to your Amazon AWS "
                "account, or any consequence resulting from the use of "
                "this service.",
            ),
        ]

        for title, text in notices:
            f = ttk.LabelFrame(self._content, text=title, padding=Spacing.PAD)
            f.pack(fill="x", pady=Spacing.SMALL)
            ttk.Label(
                f,
                text=text,
                wraplength=900,
                foreground=Colors.TEXT_SECONDARY,
                justify="left",
            ).pack(anchor="w")

    def _step_pro_local_mirror(self) -> None:
        """Step 9 (pro): Optional secondary backup destination."""
        self._set_header("Additional Backup (Optional)")

        ttk.Label(
            self._content,
            text="Would you like to also save your backups to another location?",
            font=Fonts.large(),
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        ttk.Label(
            self._content,
            text=(
                "Your data is already protected in the cloud with Object Lock. "
                "Adding a secondary copy gives you faster access to your files "
                "for restoration. You can choose an external drive, a network "
                "share, or an SSH server."
            ),
            wraplength=900,
            foreground=Colors.TEXT_SECONDARY,
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        # Toggle
        mirror_var = tk.BooleanVar(value=self._data.get("pro_mirror_local", False))
        ttk.Checkbutton(
            self._content,
            text="Enable secondary backup destination",
            variable=mirror_var,
        ).pack(anchor="w", pady=Spacing.MEDIUM)

        # Storage config (reuse the factorized builder — without S3)
        mirror_frame = ttk.Frame(self._content)

        # Initialize mirror data if not present
        if "mirror1" not in self._data:
            self._data["mirror1"] = {
                "type": StorageType.LOCAL.value,
                "vars": {},
            }

        def _toggle(*_a: object) -> None:
            self._data["pro_mirror_local"] = mirror_var.get()
            if mirror_var.get():
                mirror_frame.pack(fill="x", pady=Spacing.SMALL)
            else:
                mirror_frame.pack_forget()

        mirror_var.trace_add("write", _toggle)

        self._build_storage_config_ui(mirror_frame, "mirror1")

        _toggle()

    def _step_pro_auto_setup(self) -> None:
        """Step 10 (pro): Automatic S3 bucket provisioning."""
        self._set_header("Amazon AWS Automatic Configuration")

        # Auto-generate bucket name from profile name
        import re

        profile_name = self._data.get("name", "backup")
        sanitized = re.sub(r"[^a-z0-9-]", "-", profile_name.lower()).strip("-")[:30]
        short_id = uuid.uuid4().hex[:6]
        auto_bucket = f"bm-{sanitized}-{short_id}" if sanitized else f"bm-{short_id}"
        if not self._data.get("pro_bucket") or self._data["pro_bucket"].startswith("bm-"):
            self._data["pro_bucket"] = auto_bucket

        ttk.Label(
            self._content,
            text="Automatic configuration of your Amazon AWS S3 bucket...",
            font=Fonts.large(),
        ).pack(anchor="w", pady=(0, Spacing.MEDIUM))

        # Summary
        label, _months, days = _resolve_retention(self._data)
        region = self._data.get("pro_region", "eu-west-1")
        bucket = self._data.get("pro_bucket", "")

        summary = ttk.LabelFrame(
            self._content,
            text="Configuration summary",
            padding=Spacing.PAD,
        )
        summary.pack(fill="x", pady=Spacing.MEDIUM)

        summary_items = [
            ("Bucket", bucket),
            ("Speedtest bucket", f"{bucket}-speedtest"),
            ("Region", region),
            ("Retention", f"{label} ({days} days)"),
            ("Mode", "Compliance (immutable)"),
            ("Cleanup", "Automatic (S3 Lifecycle)"),
        ]
        if self._data.get("pro_encrypt"):
            summary_items.append(("Encryption", "AES-256 (Backup Manager)"))
        if self._data.get("pro_mirror_local") and self._data.get("pro_mirror_path"):
            summary_items.append(("Local mirror", self._data["pro_mirror_path"]))

        for k, v in summary_items:
            row = ttk.Frame(summary)
            row.pack(fill="x", pady=1)
            ttk.Label(row, text=f"{k}:", font=Fonts.bold(), width=15).pack(
                side="left",
            )
            ttk.Label(row, text=v).pack(side="left")

        # Setup log area
        self._setup_log = tk.Text(
            self._content,
            height=8,
            font=Fonts.mono(),
            state="disabled",
        )

        self._setup_log.pack(fill="x", pady=Spacing.SMALL)

        # Auto-launch setup when page is displayed
        self._win.after(500, self._run_pro_setup)

    def _run_pro_setup(self) -> None:
        """Execute S3 bucket provisioning in a background thread."""
        self._next_btn.state(["disabled"])

        ak = self._data["pro_aws_key"]
        sk = self._data["pro_aws_secret"]
        region = self._data["pro_region"]
        bucket = self._data["pro_bucket"]
        _label, _months, days = _resolve_retention(self._data)

        result: list = [None]

        speedtest_bucket = f"{bucket}-speedtest"

        def _do() -> None:
            try:
                setup = S3ObjectLockSetup(ak, sk, region)
                result[0] = setup.full_setup(
                    bucket,
                    days,
                    full_extra_days=30,
                    speedtest_bucket_name=speedtest_bucket,
                )
            except Exception as e:
                result[0] = [("Setup", False, str(e))]

        def _append_log(text: str) -> None:
            self._setup_log.config(state="normal")
            self._setup_log.insert("end", text + "\n")
            self._setup_log.config(state="disabled")
            self._setup_log.see("end")

        def _poll() -> None:
            if result[0] is None:
                self._win.after(300, _poll)
                return

            steps = result[0]
            critical_fail = False
            for step_name, ok, msg in steps:
                icon = "\u2713" if ok else "\u2717"
                _append_log(f"  {icon} {step_name}: {msg}")
                if not ok and step_name != "Create speedtest bucket":
                    critical_fail = True
                if not ok and step_name == "Create speedtest bucket":
                    self._data["pro_speedtest_failed"] = True

            if not critical_fail:
                _append_log("\n  Configuration complete!")
                self._data["pro_setup_done"] = True
                self._next_btn.state(["!disabled"])
                self._next_btn.config(text="Finish")
            else:
                _append_log("\n  Setup failed. Please go back and check your settings.")

        _append_log("Starting S3 configuration...")
        threading.Thread(target=_do, daemon=True).start()
        self._win.after(300, _poll)

    # ------------------------------------------------------------------
    # Profile creation (personal — unchanged)
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
            backup_type=BackupType.FULL,
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.WEEKLY,
                time="10:00",
            ),
            retention=RetentionConfig(
                gfs_daily=1,
                gfs_weekly=4,
                gfs_monthly=7,
            ),
        )

        self.result_profile = profile
        self._canvas.unbind_all("<MouseWheel>")
        self._win.destroy()

    def _create_pro_profile(self) -> None:
        """Build a professional BackupProfile with S3 Object Lock settings.

        Configures: S3 storage with Object Lock, daily schedule,
        differential with monthly full, GFS disabled, optional
        encryption, optional local mirror.
        """
        d = self._data
        _label, _months, days = _resolve_retention(d)

        speedtest_bucket = "" if d.get("pro_speedtest_failed") else f"{d['pro_bucket']}-speedtest"

        storage = StorageConfig(
            storage_type=StorageType.S3,
            s3_bucket=d["pro_bucket"],
            s3_region=d["pro_region"],
            s3_access_key=d["pro_aws_key"],
            s3_secret_key=d["pro_aws_secret"],
            s3_provider="Amazon AWS",
            s3_object_lock=True,
            s3_object_lock_mode="COMPLIANCE",
            s3_object_lock_days=days,
            s3_object_lock_full_extra_days=30,
            s3_speedtest_bucket=speedtest_bucket,
        )

        mirrors: list[StorageConfig] = []
        if d.get("pro_mirror_local") and "mirror1" in d:
            try:
                mirror_config = self._build_storage_config_from_key("mirror1")
                mirrors.append(mirror_config)
            except Exception:
                pass  # Mirror config incomplete, skip

        encryption = EncryptionConfig()
        if d.get("pro_encrypt") and d.get("pro_encrypt_password"):
            encryption = EncryptionConfig(
                enabled=True,
                stored_password=d["pro_encrypt_password"],
            )

        profile = BackupProfile(
            name=d["name"],
            source_paths=d["sources"],
            backup_type=BackupType.DIFFERENTIAL,
            storage=storage,
            mirror_destinations=mirrors,
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.DAILY,
                time="10:00",
            ),
            encryption=encryption,
            encrypt_primary=encryption.enabled,
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
                gfs_enabled=False,
            ),
            full_backup_every=30,
            object_lock_enabled=True,
        )

        self.result_profile = profile
        self._canvas.unbind_all("<MouseWheel>")
        self._win.destroy()
