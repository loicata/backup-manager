"""Mirror tab: optional mirror destination."""

import threading
import tkinter as tk
from tkinter import ttk, filedialog

from src.core.config import BackupProfile, StorageConfig, StorageType
from src.installer import get_available_features, FEAT_SFTP, FEAT_S3
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing


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
            (StorageType.S3, "S3 Cloud Storage (beta)", FEAT_S3 in self._features),
            (StorageType.PROTON, "Proton Drive (beta)", True),
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
        for label, key, default, show in [
            ("Host", "sftp_host", "", ""),
            ("Port", "sftp_port", "22", ""),
            ("Username", "sftp_username", "", ""),
            ("Password", "sftp_password", "", "●"),
            ("SSH key path", "sftp_key_path", "", ""),
            ("Key passphrase", "sftp_key_passphrase", "", "●"),
            ("Remote path", "sftp_remote_path", "/home/username/backups", ""),
        ]:
            ttk.Label(f, text=f"{label}:").pack(anchor="w")
            var = tk.StringVar(value=default)
            self._sftp_vars[key] = var
            if show:
                ttk.Entry(f, textvariable=var, show=show).pack(fill="x")
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
            values=["aws", "minio", "wasabi", "ovh", "other"],
            state="readonly",
        ).pack(fill="x")
        for label, key, default in [
            ("Bucket", "s3_bucket", ""),
            ("Region", "s3_region", "eu-west-1"),
            ("Access Key", "s3_access_key", ""),
            ("Secret Key", "s3_secret_key", ""),
        ]:
            ttk.Label(f, text=f"{label}:").pack(anchor="w")
            var = tk.StringVar(value=default)
            self._s3_vars[key] = var
            ttk.Entry(f, textvariable=var).pack(fill="x")

        # Proton
        f = ttk.Frame(self._config_container)
        self._config_frames["proton"] = f
        self._proton_vars = {}
        for label, key, default in [
            ("Username", "proton_username", ""),
            ("Password", "proton_password", ""),
            ("Remote path", "proton_remote_path", "/Backups"),
        ]:
            ttk.Label(f, text=f"{label}:").pack(anchor="w")
            var = tk.StringVar(value=default)
            self._proton_vars[key] = var
            ttk.Entry(f, textvariable=var).pack(fill="x")

        self._on_type_changed()

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
        try:
            widget.configure(state=state)
        except tk.TclError:
            pass
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
                self.after(0, lambda: self._show_test_result(False, str(e)))

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
        elif stype == StorageType.PROTON:
            for key, var in self._proton_vars.items():
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
                for key, var in self._s3_vars.items():
                    var.set(str(getattr(m, key, "")))
            if hasattr(self, "_proton_vars"):
                for key, var in self._proton_vars.items():
                    var.set(str(getattr(m, key, "")))
        else:
            self.enabled_var.set(False)
        self._toggle_enabled()

    def collect_config(self) -> StorageConfig | None:
        """Collect mirror config or None if disabled."""
        if not self.enabled_var.get():
            return None
        return self._build_storage_config()
