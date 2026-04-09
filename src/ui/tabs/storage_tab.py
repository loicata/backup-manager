"""Storage tab: primary backup destination configuration."""

import tkinter as tk
from tkinter import filedialog, ttk

from src.core.config import BackupProfile, StorageConfig, StorageType
from src.installer import FEAT_S3, FEAT_SFTP, get_available_features
from src.storage.s3 import PROVIDER_REGIONS
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing


class StorageTab(ScrollableTab):
    """Primary storage destination configuration."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._features = get_available_features()
        self._config_frames: dict[str, ttk.Frame] = {}
        self._saved_device_serial: str = ""
        self._build_ui()

    def _build_ui(self):
        # Storage type selection
        type_frame = ttk.LabelFrame(self.inner, text="Storage type", padding=Spacing.PAD)
        type_frame.pack(fill="x", padx=Spacing.LARGE, pady=(Spacing.LARGE, Spacing.MEDIUM))

        self.type_var = tk.StringVar(value=StorageType.LOCAL.value)
        self.type_var.trace_add("write", self._on_type_changed)

        options = [
            (StorageType.LOCAL, "External drive / USB stick", True),
            (StorageType.NETWORK, "Network folder (UNC)", True),
            (StorageType.SFTP, "Remote server SFTP (SSH)", FEAT_SFTP in self._features),
            (StorageType.S3, "S3 Cloud Storage", FEAT_S3 in self._features),
        ]

        for stype, label, available in options:
            rb = ttk.Radiobutton(
                type_frame,
                text=label,
                value=stype.value,
                variable=self.type_var,
                state="normal" if available else "disabled",
            )
            rb.pack(anchor="w", pady=2)

        # Configuration frame (dynamic content)
        self._config_container = ttk.LabelFrame(
            self.inner, text="Configuration", padding=Spacing.PAD
        )
        self._config_container.pack(
            fill="both",
            expand=True,
            padx=Spacing.LARGE,
            pady=Spacing.MEDIUM,
        )

        self._build_local_config()
        self._build_network_config()
        self._build_sftp_config()
        self._build_s3_config()

        # Test connection button
        btn_frame = ttk.Frame(self.inner)
        btn_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.test_btn = ttk.Button(
            btn_frame,
            text="🔌 Test connection",
            command=self._test_connection,
        )
        self.test_btn.pack(side="left")

        self.test_label = ttk.Label(btn_frame, text="", foreground=Colors.TEXT_SECONDARY)
        self.test_label.pack(side="left", padx=Spacing.LARGE)

        # Show initial config
        self._on_type_changed()

    def _build_local_config(self):
        frame = ttk.Frame(self._config_container)
        self._config_frames["local"] = frame

        ttk.Label(frame, text="Destination path:").pack(anchor="w")
        path_frame = ttk.Frame(frame)
        path_frame.pack(fill="x", pady=Spacing.SMALL)

        self.local_path_var = tk.StringVar()
        ttk.Entry(path_frame, textvariable=self.local_path_var).pack(
            side="left", fill="x", expand=True
        )
        ttk.Button(path_frame, text="Browse...", command=self._browse_local).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )

    def _build_network_config(self):
        frame = ttk.Frame(self._config_container)
        self._config_frames["network"] = frame

        ttk.Label(frame, text="Network path (UNC):").pack(anchor="w")
        self.network_path_var = tk.StringVar(value=r"\\server\backups")
        ttk.Entry(frame, textvariable=self.network_path_var).pack(fill="x")

        ttk.Label(frame, text="Username:").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self.network_user_var = tk.StringVar(value=r"username")
        ttk.Entry(frame, textvariable=self.network_user_var).pack(fill="x")

        ttk.Label(frame, text="Password:").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self.network_pass_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.network_pass_var, show="\u25cf").pack(fill="x")

    def _build_sftp_config(self):
        frame = ttk.Frame(self._config_container)
        self._config_frames["sftp"] = frame

        fields = [
            ("Host SFTP", "sftp_host", ""),
            ("Port", "sftp_port", "22"),
            ("Username", "sftp_username", ""),
            ("Password (leave empty if using SSH key)", "sftp_password", ""),
            ("SSH private key (optional — replaces password)", "sftp_key_path", ""),
            ("Key passphrase (if key is protected)", "sftp_key_passphrase", ""),
            ("Remote path", "sftp_remote_path", "/home/username/backups"),
        ]

        self._sftp_vars = {}
        for label, key, default in fields:
            ttk.Label(frame, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=default)
            self._sftp_vars[key] = var

            if key == "sftp_key_path":
                f = ttk.Frame(frame)
                f.pack(fill="x")
                ttk.Entry(f, textvariable=var).pack(side="left", fill="x", expand=True)
                ttk.Button(f, text="Browse...", command=self._browse_key).pack(
                    side="right", padx=(Spacing.SMALL, 0)
                )
            elif "password" in key or "passphrase" in key:
                ttk.Entry(frame, textvariable=var, show="●").pack(fill="x")
            elif key == "sftp_port":
                ttk.Spinbox(frame, textvariable=var, from_=1, to=65535, width=8).pack(anchor="w")
            else:
                ttk.Entry(frame, textvariable=var).pack(fill="x")

        ttk.Label(
            frame,
            text="Supports RSA, Ed25519, ECDSA keys (.pem, .key, .ppk, id_rsa).",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w")
        ttk.Label(
            frame,
            text="Absolute path on the remote server, e.g. /home/username/backups",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w")

    def _build_s3_config(self):
        frame = ttk.Frame(self._config_container)
        self._config_frames["s3"] = frame

        # Provider selector
        ttk.Label(frame, text="Provider:").pack(anchor="w")
        self.s3_provider_var = tk.StringVar(value="aws")
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
        provider_cb = ttk.Combobox(
            frame, textvariable=self.s3_provider_var, values=providers, state="readonly"
        )
        provider_cb.pack(fill="x")

        fields = [
            ("Bucket", "s3_bucket", ""),
            ("Prefix (optional)", "s3_prefix", ""),
            ("Access Key", "s3_access_key", ""),
            ("Secret Key", "s3_secret_key", ""),
            ("Endpoint URL (optional — auto-detected from provider)", "s3_endpoint_url", ""),
        ]

        self._s3_vars = {}

        # Region — Combobox with provider-specific values
        ttk.Label(frame, text="Region:").pack(anchor="w", pady=(Spacing.SMALL, 0))
        default_regions = PROVIDER_REGIONS.get("aws", [])
        region_var = tk.StringVar(value=default_regions[0] if default_regions else "")
        self._s3_vars["s3_region"] = region_var
        self._s3_region_cb = ttk.Combobox(frame, textvariable=region_var, values=default_regions)
        self._s3_region_cb.pack(fill="x")

        self.s3_provider_var.trace_add("write", self._on_s3_provider_changed)

        for label, key, default in fields:
            ttk.Label(frame, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=default)
            self._s3_vars[key] = var
            if "secret" in key or "key" in key.lower() and "access" in key.lower():
                ttk.Entry(frame, textvariable=var, show="●").pack(fill="x")
            else:
                ttk.Entry(frame, textvariable=var).pack(fill="x")

    def _on_s3_provider_changed(self, *args):
        """Update region combobox when the S3 provider changes."""
        provider = self.s3_provider_var.get()
        regions = PROVIDER_REGIONS.get(provider, [])
        self._s3_region_cb["values"] = regions
        current = self._s3_vars["s3_region"].get()
        if current not in regions:
            self._s3_vars["s3_region"].set(regions[0] if regions else "")

    def _on_type_changed(self, *args):
        """Show config fields for selected storage type."""
        for frame in self._config_frames.values():
            frame.pack_forget()

        stype = self.type_var.get()
        frame = self._config_frames.get(stype)
        if frame:
            frame.pack(fill="both", expand=True)

        if stype == StorageType.NETWORK.value:
            current = self.network_path_var.get()
            if not current.startswith("\\\\"):
                self.network_path_var.set(r"\\server\backups")
                self.network_user_var.set("username")
                self.network_pass_var.set("")
        elif stype == StorageType.SFTP.value and hasattr(self, "_sftp_vars"):
            if not self._sftp_vars["sftp_remote_path"].get():
                self._sftp_vars["sftp_remote_path"].set("/home/username/backups")

    def _browse_local(self):
        path = filedialog.askdirectory(title="Select backup destination")
        if path:
            self.local_path_var.set(path)

    @staticmethod
    def _detect_device_serial(destination_path: str) -> str:
        """Detect hardware serial for a local drive path.

        Args:
            destination_path: Local path like "G:\\Backups".

        Returns:
            Hardware serial string, or empty string if unavailable.
        """
        if not destination_path or len(destination_path) < 2:
            return ""
        if destination_path[1] != ":":
            return ""

        from src.storage.drive_serial import get_hardware_serial

        return get_hardware_serial(destination_path[0]) or ""

    def _browse_key(self):
        path = filedialog.askopenfilename(
            title="Select SSH private key",
            filetypes=[
                ("SSH keys", "*.pem *.key *.ppk id_*"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self._sftp_vars["sftp_key_path"].set(path)

    def _test_connection(self):
        """Test storage connection in background."""
        import threading

        self.test_label.config(text="Testing...", foreground=Colors.WARNING)
        self.test_btn.state(["disabled"])

        def _do_test():
            try:
                config = self._build_storage_config()
                from src.core.backup_engine import BackupEngine

                engine = BackupEngine.__new__(BackupEngine)
                backend = engine._get_backend(config)
                ok, msg = backend.test_connection()
                self.after(0, lambda: self._show_test_result(ok, msg))
            except Exception as e:
                self.after(0, lambda _e=e: self._show_test_result(False, str(_e)))

        threading.Thread(target=_do_test, daemon=True).start()

    def _show_test_result(self, ok: bool, msg: str):
        self.test_btn.state(["!disabled"])
        color = Colors.SUCCESS if ok else Colors.DANGER
        self.test_label.config(text=msg, foreground=color)

    def _build_storage_config(self) -> StorageConfig:
        """Build StorageConfig from current UI state."""
        stype = StorageType(self.type_var.get())
        config = StorageConfig()  # Default first, set type after populating

        if stype == StorageType.LOCAL:
            config.destination_path = self.local_path_var.get()
            detected = self._detect_device_serial(config.destination_path)
            config.device_serial = detected or self._saved_device_serial
        elif stype == StorageType.NETWORK:
            config.destination_path = self.network_path_var.get()
            config.network_username = self.network_user_var.get()
            config.network_password = self.network_pass_var.get()
        elif stype == StorageType.SFTP:
            for key, var in self._sftp_vars.items():
                val = var.get()
                if key == "sftp_port":
                    setattr(config, key, int(val) if val else 22)
                else:
                    setattr(config, key, val)
        elif stype == StorageType.S3:
            config.s3_provider = self.s3_provider_var.get()
            for key, var in self._s3_vars.items():
                setattr(config, key, var.get())
        config.storage_type = stype
        return config

    def load_profile(self, profile: BackupProfile):
        s = profile.storage
        self.type_var.set(s.storage_type.value)
        self._saved_device_serial = getattr(s, "device_serial", "")

        # Reset all fields before loading to avoid stale values
        self.local_path_var.set("")
        self.network_path_var.set("")
        self.network_user_var.set("")
        self.network_pass_var.set("")
        if hasattr(self, "_sftp_vars"):
            for key, var in self._sftp_vars.items():
                var.set("22" if key == "sftp_port" else "")
        if hasattr(self, "_s3_vars"):
            self.s3_provider_var.set("aws")
            for var in self._s3_vars.values():
                var.set("")

        # Load values for the active storage type
        if s.storage_type == StorageType.LOCAL:
            self.local_path_var.set(s.destination_path)
        elif s.storage_type == StorageType.NETWORK:
            self.network_path_var.set(s.destination_path)
            self.network_user_var.set(getattr(s, "network_username", ""))
            self.network_pass_var.set(getattr(s, "network_password", ""))
        elif s.storage_type == StorageType.SFTP and hasattr(self, "_sftp_vars"):
            for key, var in self._sftp_vars.items():
                var.set(str(getattr(s, key, "")))
        elif s.storage_type == StorageType.S3 and hasattr(self, "_s3_vars"):
            self.s3_provider_var.set(s.s3_provider)
            for key, var in self._s3_vars.items():
                var.set(str(getattr(s, key, "")))
            saved_region = str(getattr(s, "s3_region", ""))
            if saved_region:
                self._s3_vars["s3_region"].set(saved_region)

    def collect_config(self) -> dict:
        return {
            "storage": self._build_storage_config(),
        }
