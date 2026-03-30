"""Mirror tab: optional mirror destination."""

import contextlib
import threading
import tkinter as tk
from tkinter import filedialog, ttk

from src.core.config import BackupProfile, StorageConfig, StorageType
from src.installer import FEAT_S3, FEAT_SFTP, get_available_features
from src.storage.s3 import PROVIDER_REGIONS
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Spacing


class MirrorTab(ScrollableTab):
    """Mirror destination configuration (reused for Mirror 1 and Mirror 2)."""

    def __init__(self, parent, mirror_index: int = 0, **kwargs):
        super().__init__(parent, **kwargs)
        self._mirror_index = mirror_index
        self._features = get_available_features()
        self._config_frames: dict[str, ttk.Frame] = {}
        self._build_ui()

    def _build_ui(self):
        # Enable checkbox
        self.enabled_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            self.inner,
            text=f"Enable Mirror {self._mirror_index + 1}",
            variable=self.enabled_var,
            command=self._toggle_enabled,
        ).pack(anchor="w", padx=Spacing.LARGE, pady=Spacing.LARGE)

        # Content frame (disabled when mirror is off)
        self._content = ttk.Frame(self.inner)
        self._content.pack(fill="both", expand=True)

        # Storage type
        type_frame = ttk.LabelFrame(self._content, text="Storage type", padding=Spacing.PAD)
        type_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self.type_var = tk.StringVar(value=StorageType.LOCAL.value)
        self.type_var.trace_add("write", self._on_type_changed)

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
                variable=self.type_var,
                state="normal" if available else "disabled",
            ).pack(anchor="w", pady=2)

        # Config container
        self._config_container = ttk.LabelFrame(
            self._content, text="Configuration", padding=Spacing.PAD
        )
        self._config_container.pack(
            fill="both", expand=True, padx=Spacing.LARGE, pady=Spacing.MEDIUM
        )

        # Build config forms for each storage type
        self._build_configs()

        # Test connection button
        btn_frame = ttk.Frame(self._content)
        btn_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.test_btn = ttk.Button(
            btn_frame,
            text="Test connection",
            command=self._test_connection,
        )
        self.test_btn.pack(side="left")

        self.test_label = ttk.Label(btn_frame, text="", foreground=Colors.TEXT_SECONDARY)
        self.test_label.pack(side="left", padx=Spacing.LARGE)

        self._toggle_enabled()

    def _build_configs(self):
        """Build configuration forms for each storage type."""
        # Local
        f = ttk.Frame(self._config_container)
        self._config_frames["local"] = f
        ttk.Label(f, text="Destination path:").pack(anchor="w")
        self.local_path_var = tk.StringVar()
        row = ttk.Frame(f)
        row.pack(fill="x")
        ttk.Entry(row, textvariable=self.local_path_var).pack(side="left", fill="x", expand=True)
        ttk.Button(
            row,
            text="Browse...",
            command=lambda: self.local_path_var.set(
                filedialog.askdirectory() or self.local_path_var.get()
            ),
        ).pack(side="right")

        # Network
        f = ttk.Frame(self._config_container)
        self._config_frames["network"] = f
        ttk.Label(f, text="Network path (UNC):").pack(anchor="w")
        self.network_path_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.network_path_var).pack(fill="x")

        # SFTP
        f = ttk.Frame(self._config_container)
        self._config_frames["sftp"] = f
        self._sftp_vars = {}
        for label, key, default in [
            ("Host SFTP", "sftp_host", ""),
            ("Port", "sftp_port", "22"),
            ("Username", "sftp_username", ""),
            ("Password (leave empty if using SSH key)", "sftp_password", ""),
            ("SSH private key (optional — replaces password)", "sftp_key_path", ""),
            ("Key passphrase (if key is protected)", "sftp_key_passphrase", ""),
            ("Remote path", "sftp_remote_path", "/home/username/backups"),
        ]:
            ttk.Label(f, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=default)
            self._sftp_vars[key] = var
            if key == "sftp_key_path":
                row = ttk.Frame(f)
                row.pack(fill="x")
                ttk.Entry(row, textvariable=var).pack(side="left", fill="x", expand=True)
                ttk.Button(row, text="Browse...", command=self._browse_ssh_key).pack(
                    side="right", padx=(Spacing.SMALL, 0)
                )
            elif "password" in key or "passphrase" in key:
                ttk.Entry(f, textvariable=var, show="●").pack(fill="x")
            elif key == "sftp_port":
                ttk.Spinbox(f, textvariable=var, from_=1, to=65535, width=8).pack(anchor="w")
            else:
                ttk.Entry(f, textvariable=var).pack(fill="x")

        # S3
        f = ttk.Frame(self._config_container)
        self._config_frames["s3"] = f
        self._s3_vars = {}
        self.s3_provider_var = tk.StringVar(value="aws")
        ttk.Label(f, text="Provider:").pack(anchor="w")
        ttk.Combobox(
            f,
            textvariable=self.s3_provider_var,
            values=[
                "aws",
                "scaleway",
                "wasabi",
                "ovh",
                "digitalocean",
                "cloudflare",
                "backblaze_s3",
                "other",
            ],
            state="readonly",
        ).pack(fill="x")

        # Region — Combobox with provider-specific values
        default_regions = PROVIDER_REGIONS.get("aws", [])
        ttk.Label(f, text="Region:").pack(anchor="w")
        region_var = tk.StringVar(value=default_regions[0] if default_regions else "")
        self._s3_vars["s3_region"] = region_var
        self._s3_region_cb = ttk.Combobox(f, textvariable=region_var, values=default_regions)
        self._s3_region_cb.pack(fill="x")

        self.s3_provider_var.trace_add("write", self._on_s3_provider_changed)

        for label, key, default in [
            ("Bucket", "s3_bucket", ""),
            ("Prefix (optional)", "s3_prefix", ""),
            ("Access Key", "s3_access_key", ""),
            ("Secret Key", "s3_secret_key", ""),
            ("Endpoint URL (optional — auto-detected from provider)", "s3_endpoint_url", ""),
        ]:
            ttk.Label(f, text=f"{label}:").pack(anchor="w")
            var = tk.StringVar(value=default)
            self._s3_vars[key] = var
            if "secret" in key:
                ttk.Entry(f, textvariable=var, show="●").pack(fill="x")
            else:
                ttk.Entry(f, textvariable=var).pack(fill="x")

        self._on_type_changed()

    def _browse_ssh_key(self):
        """Browse for SSH private key file."""
        path = filedialog.askopenfilename(
            title="Select SSH private key",
            filetypes=[
                ("SSH keys", "*.pem *.key *.ppk id_*"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self._sftp_vars["sftp_key_path"].set(path)

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
        frame = self._config_frames.get(self.type_var.get())
        if frame:
            frame.pack(fill="both", expand=True)

    def _toggle_enabled(self):
        """Enable or disable all content widgets."""
        state = "normal" if self.enabled_var.get() else "disabled"
        for child in self._content.winfo_children():
            self._set_state_recursive(child, state)

    def _set_state_recursive(self, widget, state):
        if widget is self.test_label:
            return
        with contextlib.suppress(tk.TclError):
            widget.configure(state=state)
        for child in widget.winfo_children():
            self._set_state_recursive(child, state)

    def _test_connection(self):
        """Test mirror storage connection in background."""
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
        """Display test connection result."""
        self.test_btn.state(["!disabled"])
        color = Colors.SUCCESS if ok else Colors.DANGER
        self.test_label.config(text=msg, foreground=color)

    def _build_storage_config(self) -> StorageConfig:
        """Build StorageConfig from current UI state."""
        stype = StorageType(self.type_var.get())
        config = StorageConfig()  # Default first, set type after populating

        if stype == StorageType.LOCAL:
            config.destination_path = self.local_path_var.get()
        elif stype == StorageType.NETWORK:
            config.destination_path = self.network_path_var.get()
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
        """Load mirror config from profile."""
        mirrors = profile.mirror_destinations
        if self._mirror_index < len(mirrors):
            m = mirrors[self._mirror_index]
            self.enabled_var.set(True)
            self.type_var.set(m.storage_type.value)
            self.local_path_var.set(m.destination_path)
            self.network_path_var.set(m.destination_path)
            if hasattr(self, "_sftp_vars"):
                for key, var in self._sftp_vars.items():
                    var.set(str(getattr(m, key, "")))
            if hasattr(self, "_s3_vars"):
                self.s3_provider_var.set(getattr(m, "s3_provider", "aws"))
                # Set region AFTER provider to override the provider callback reset.
                for key, var in self._s3_vars.items():
                    var.set(str(getattr(m, key, "")))
                # Re-apply region explicitly: the provider trace may reset it.
                saved_region = str(getattr(m, "s3_region", ""))
                if saved_region:
                    self._s3_vars["s3_region"].set(saved_region)
        else:
            self.enabled_var.set(False)
        self._toggle_enabled()

    def collect_config(self) -> StorageConfig | None:
        """Collect mirror config or None if disabled."""
        if not self.enabled_var.get():
            return None
        return self._build_storage_config()
