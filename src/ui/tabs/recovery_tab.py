"""Recovery tab: restore local backups and retrieve remote ones."""

import logging
import os
import shutil
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from src.core.config import BackupProfile, StorageConfig, StorageType
from src.installer import FEAT_S3, FEAT_SFTP, get_available_features
from src.security.encryption import DecryptingReader
from src.storage.base import long_path_mkdir, long_path_str
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)

# Placeholder value when profile has a stored password.
_PASSWORD_PLACEHOLDER = "****************"

# Local directory for downloaded backups (user's Desktop).
_DOWNLOAD_DIR = Path(os.path.expanduser("~/Desktop"))


class RecoveryTab(ScrollableTab):
    """Restore local backups and retrieve remote ones."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._stored_password: str = ""
        self._user_modified_pw = False
        self._profile: BackupProfile | None = None
        self._features = get_available_features()
        self._retrieve_config_frames: dict[str, ttk.Frame] = {}
        self._filling_retrieve = False
        self._build_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        """Build all UI sections."""
        self._build_select_backup()
        self._build_restore_destination()
        self._build_encryption_password()
        self._build_restore_button()
        self._build_retrieve_section()

    def _build_select_backup(self) -> None:
        """Build the 'Select backup' section."""
        frame = ttk.LabelFrame(self.inner, text="Select backup", padding=Spacing.PAD)
        frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE)

        row = ttk.Frame(frame)
        row.pack(fill="x")
        self.backup_path_var = tk.StringVar()
        self.backup_path_var.trace_add("write", self._on_backup_path_changed)
        ttk.Entry(row, textvariable=self.backup_path_var).pack(side="left", fill="x", expand=True)
        ttk.Button(row, text="Browse...", command=self._browse_backup).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )

    def _build_restore_destination(self) -> None:
        """Build the 'Restore destination' section."""
        frame = ttk.LabelFrame(self.inner, text="Restore destination", padding=Spacing.PAD)
        frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        row = ttk.Frame(frame)
        row.pack(fill="x")
        self.dest_path_var = tk.StringVar()
        ttk.Entry(row, textvariable=self.dest_path_var).pack(side="left", fill="x", expand=True)
        ttk.Button(row, text="Browse...", command=self._browse_dest).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )

    def _build_encryption_password(self) -> None:
        """Build the 'Encryption password' section."""
        pw_frame = ttk.LabelFrame(
            self.inner, text="Encryption password (optional)", padding=Spacing.PAD
        )
        pw_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        self.password_var = tk.StringVar()
        self.password_var.trace_add("write", self._on_password_changed)
        self._pw_entry = ttk.Entry(pw_frame, textvariable=self.password_var, show="●")
        self._pw_entry.pack(fill="x")

        ttk.Label(
            pw_frame,
            text=("We recommend always typing your password manually " "to verify it is correct."),
            foreground=Colors.ACCENT,
            font=Fonts.small(),
            wraplength=1200,
            justify="left",
        ).pack(anchor="w", pady=(Spacing.SMALL, 0))

    def _build_restore_button(self) -> None:
        """Build the Restore button and status label."""
        btn_frame = ttk.Frame(self.inner)
        btn_frame.pack(
            fill="x",
            padx=Spacing.LARGE,
            pady=(Spacing.MEDIUM, Spacing.LARGE),
        )

        self.restore_btn = ttk.Button(
            btn_frame,
            text="Restore",
            style="Accent.TButton",
            command=self._restore,
        )
        self.restore_btn.pack(side="left")

        self.status_label = ttk.Label(btn_frame, text="", foreground=Colors.TEXT_SECONDARY)
        self.status_label.pack(side="left", padx=Spacing.LARGE)

    def _build_retrieve_section(self) -> None:
        """Build the 'Retrieve backup' section at the bottom."""
        retrieve_frame = ttk.LabelFrame(self.inner, text="Retrieve backup", padding=Spacing.PAD)
        retrieve_frame.pack(
            fill="x",
            padx=Spacing.LARGE,
            pady=(Spacing.MEDIUM, Spacing.LARGE),
        )

        # --- Backup source ---
        src_row = ttk.Frame(retrieve_frame)
        src_row.pack(fill="x")
        ttk.Label(src_row, text="Backup source:").pack(side="left")
        self.source_var = tk.StringVar(value="Storage")
        self._source_combo = ttk.Combobox(
            src_row,
            textvariable=self.source_var,
            values=["Storage", "Mirror 1", "Mirror 2"],
            state="readonly",
            width=20,
        )
        self._source_combo.pack(side="left", padx=(Spacing.SMALL, 0))
        self._source_combo.bind("<<ComboboxSelected>>", self._on_source_changed)

        # --- Storage type (radio buttons) ---
        self._type_frame = ttk.LabelFrame(retrieve_frame, text="Storage type", padding=Spacing.PAD)
        self._type_frame.pack(fill="x", pady=(Spacing.MEDIUM, 0))

        self.retrieve_type_var = tk.StringVar(value=StorageType.SFTP.value)
        self.retrieve_type_var.trace_add("write", self._on_retrieve_type_changed)

        type_options = [
            (StorageType.SFTP, "Remote server", FEAT_SFTP in self._features),
            (StorageType.S3, "S3 cloud", FEAT_S3 in self._features),
        ]

        for stype, label, available in type_options:
            rb = ttk.Radiobutton(
                self._type_frame,
                text=label,
                value=stype.value,
                variable=self.retrieve_type_var,
                state="normal" if available else "disabled",
            )
            rb.pack(anchor="w", pady=2)

        # --- Configuration container (dynamic) ---
        self._retrieve_config_container = ttk.Frame(retrieve_frame)
        self._retrieve_config_container.pack(fill="x", pady=(Spacing.SMALL, 0))

        self._build_retrieve_local_config()
        self._build_retrieve_network_config()
        self._build_retrieve_sftp_config()
        self._build_retrieve_s3_config()

        # --- Retrieve destination ---
        dest_frame = ttk.LabelFrame(
            retrieve_frame, text="Retrieve destination", padding=Spacing.PAD
        )
        dest_frame.pack(fill="x", pady=(Spacing.MEDIUM, 0))

        dest_row = ttk.Frame(dest_frame)
        dest_row.pack(fill="x")
        self.retrieve_dest_var = tk.StringVar(value=str(_DOWNLOAD_DIR))
        ttk.Entry(dest_row, textvariable=self.retrieve_dest_var).pack(
            side="left", fill="x", expand=True
        )
        ttk.Button(dest_row, text="Browse...", command=self._browse_retrieve_dest).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )

        # --- Retrieve button ---
        dl_row = ttk.Frame(retrieve_frame)
        dl_row.pack(fill="x", pady=(Spacing.MEDIUM, 0))

        self.retrieve_btn = ttk.Button(
            dl_row,
            text="Retrieve",
            style="Accent.TButton",
            command=self._retrieve_all,
        )
        self.retrieve_btn.pack(side="left")

        self.retrieve_label = ttk.Label(dl_row, text="", foreground=Colors.TEXT_SECONDARY)
        self.retrieve_label.pack(side="left", padx=Spacing.LARGE)

        # Show initial config
        self._on_retrieve_type_changed()

    # --- Retrieve storage config frames ---

    def _build_retrieve_local_config(self) -> None:
        """Build config fields for local/external drive."""
        frame = ttk.Frame(self._retrieve_config_container)
        self._retrieve_config_frames["local"] = frame

        ttk.Label(frame, text="Backup path:").pack(anchor="w")
        path_row = ttk.Frame(frame)
        path_row.pack(fill="x", pady=Spacing.SMALL)

        self._ret_local_path_var = tk.StringVar()
        ttk.Entry(path_row, textvariable=self._ret_local_path_var).pack(
            side="left", fill="x", expand=True
        )
        ttk.Button(
            path_row,
            text="Browse...",
            command=lambda: self._browse_to_var(self._ret_local_path_var),
        ).pack(side="right", padx=(Spacing.SMALL, 0))

    def _build_retrieve_network_config(self) -> None:
        """Build config fields for network folder."""
        frame = ttk.Frame(self._retrieve_config_container)
        self._retrieve_config_frames["network"] = frame

        ttk.Label(frame, text="Network path (UNC):").pack(anchor="w")
        self._ret_network_path_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self._ret_network_path_var).pack(fill="x")
        ttk.Label(
            frame,
            text=r"e.g. \\server\share\backups",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w")

    def _build_retrieve_sftp_config(self) -> None:
        """Build config fields for SFTP."""
        frame = ttk.Frame(self._retrieve_config_container)
        self._retrieve_config_frames["sftp"] = frame

        fields = [
            ("Host SFTP", "sftp_host", ""),
            ("Port", "sftp_port", "22"),
            ("Username", "sftp_username", ""),
            ("Password", "sftp_password", ""),
            ("SSH private key (optional)", "sftp_key_path", ""),
            ("Key passphrase (if key is protected)", "sftp_key_passphrase", ""),
            ("Remote path", "sftp_remote_path", "/home/username/backups"),
        ]

        self._ret_sftp_vars: dict[str, tk.StringVar] = {}
        for label, key, default in fields:
            ttk.Label(frame, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=default)
            self._ret_sftp_vars[key] = var

            if key == "sftp_key_path":
                f = ttk.Frame(frame)
                f.pack(fill="x")
                ttk.Entry(f, textvariable=var).pack(side="left", fill="x", expand=True)
                ttk.Button(
                    f,
                    text="Browse...",
                    command=lambda v=var: self._browse_key_to_var(v),
                ).pack(side="right", padx=(Spacing.SMALL, 0))
            elif "password" in key or "passphrase" in key:
                ttk.Entry(frame, textvariable=var, show="●").pack(fill="x")
            elif key == "sftp_port":
                ttk.Spinbox(frame, textvariable=var, from_=1, to=65535, width=8).pack(anchor="w")
            else:
                ttk.Entry(frame, textvariable=var).pack(fill="x")

    def _build_retrieve_s3_config(self) -> None:
        """Build config fields for S3."""
        frame = ttk.Frame(self._retrieve_config_container)
        self._retrieve_config_frames["s3"] = frame

        ttk.Label(frame, text="Provider:").pack(anchor="w")
        self._ret_s3_provider_var = tk.StringVar(value="aws")
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
        self._ret_s3_provider_cb = ttk.Combobox(
            frame,
            textvariable=self._ret_s3_provider_var,
            values=providers,
            state="readonly",
        )
        self._ret_s3_provider_cb.pack(fill="x")

        fields = [
            ("Bucket", "s3_bucket", ""),
            ("Prefix (optional)", "s3_prefix", ""),
            ("Region", "s3_region", "eu-west-1"),
            ("Access Key", "s3_access_key", ""),
            ("Secret Key", "s3_secret_key", ""),
            ("Endpoint URL (optional — auto-detected from provider)", "s3_endpoint_url", ""),
        ]

        self._ret_s3_vars: dict[str, tk.StringVar] = {}
        for label, key, default in fields:
            ttk.Label(frame, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=default)
            self._ret_s3_vars[key] = var
            if "secret" in key:
                ttk.Entry(frame, textvariable=var, show="●").pack(fill="x")
            else:
                ttk.Entry(frame, textvariable=var).pack(fill="x")

    # ------------------------------------------------------------------
    # Retrieve callbacks
    # ------------------------------------------------------------------

    def _on_source_changed(self, _event=None) -> None:
        """Pre-fill storage type and config from profile when source changes."""
        config = self._get_source_storage_config()
        if not config:
            return
        self._fill_retrieve_from_config(config)

    def _on_retrieve_type_changed(self, *_args) -> None:
        """Show config fields for the selected storage type and auto-fill."""
        # Always switch visible frame, even during programmatic fill.
        for frame in self._retrieve_config_frames.values():
            frame.pack_forget()

        stype = self.retrieve_type_var.get()
        frame = self._retrieve_config_frames.get(stype)
        if frame:
            frame.pack(fill="x")

        # Skip auto-fill when called from _fill_retrieve_from_config
        # to avoid overwriting the values it is about to set.
        if self._filling_retrieve:
            return

        # Auto-fill from profile: find a config matching the selected type.
        self._filling_retrieve = True
        try:
            config = self._find_profile_config_by_type(StorageType(stype))
            if config:
                self._fill_retrieve_fields(config)
        finally:
            self._filling_retrieve = False

    def _find_profile_config_by_type(self, stype: StorageType) -> StorageConfig | None:
        """Search all profile storage configs for one matching the given type.

        Checks main storage first, then mirrors in order.

        Args:
            stype: The storage type to look for.

        Returns:
            First matching StorageConfig, or None.
        """
        if not self._profile:
            return None
        configs = [self._profile.storage] + list(self._profile.mirror_destinations)
        for cfg in configs:
            if cfg.storage_type == stype:
                return cfg
        return None

    def _get_source_storage_config(self) -> StorageConfig | None:
        """Get storage config from profile based on selected source.

        Returns:
            StorageConfig or None if not available.
        """
        if not self._profile:
            return None
        source = self.source_var.get()
        if source == "Storage":
            return self._profile.storage
        elif source == "Mirror 1":
            mirrors = self._profile.mirror_destinations
            return mirrors[0] if len(mirrors) > 0 else None
        elif source == "Mirror 2":
            mirrors = self._profile.mirror_destinations
            return mirrors[1] if len(mirrors) > 1 else None
        return None

    def _fill_retrieve_fields(self, config: StorageConfig) -> None:
        """Fill retrieve field values from a StorageConfig without changing type.

        Args:
            config: Storage configuration to read from.
        """
        stype = config.storage_type

        if stype in (StorageType.LOCAL, StorageType.NETWORK):
            self._ret_local_path_var.set(config.destination_path or "")
            self._ret_network_path_var.set(config.destination_path or "")

        elif stype == StorageType.SFTP:
            for key, var in self._ret_sftp_vars.items():
                val = getattr(config, key, "")
                var.set(str(val) if val else "")

        elif stype == StorageType.S3:
            for key, var in self._ret_s3_vars.items():
                val = getattr(config, key, "")
                var.set(str(val) if val else "")
            # Set provider last and force Combobox sync.
            provider = config.s3_provider or "aws"
            self._ret_s3_provider_var.set(provider)
            self._ret_s3_provider_cb.set(provider)

    def _fill_retrieve_from_config(self, config: StorageConfig) -> None:
        """Pre-fill retrieve fields from a StorageConfig (sets type + fields).

        Args:
            config: Storage configuration to read from.
        """
        self._filling_retrieve = True
        try:
            self.retrieve_type_var.set(config.storage_type.value)
            self._fill_retrieve_fields(config)
        finally:
            self._filling_retrieve = False

    def _build_retrieve_storage_config(self) -> StorageConfig:
        """Build a StorageConfig from the retrieve UI fields.

        Returns:
            Configured StorageConfig.
        """
        stype = StorageType(self.retrieve_type_var.get())
        config = StorageConfig()

        if stype == StorageType.LOCAL:
            config.destination_path = self._ret_local_path_var.get()
        elif stype == StorageType.NETWORK:
            config.destination_path = self._ret_network_path_var.get()
        elif stype == StorageType.SFTP:
            for key, var in self._ret_sftp_vars.items():
                val = var.get()
                if key == "sftp_port":
                    setattr(config, key, int(val) if val else 22)
                else:
                    setattr(config, key, val)
        elif stype == StorageType.S3:
            config.s3_provider = self._ret_s3_provider_var.get()
            for key, var in self._ret_s3_vars.items():
                setattr(config, key, var.get())
        config.storage_type = stype
        return config

    def _retrieve_all(self) -> None:
        """Download everything from the configured remote source."""
        retrieve_dest = self.retrieve_dest_var.get().strip()
        if not retrieve_dest:
            messagebox.showwarning("Retrieve", "Please select a retrieve destination.")
            return

        config = self._build_retrieve_storage_config()
        dest = Path(retrieve_dest)

        self.retrieve_label.config(
            text="Retrieving... This may take a long time depending on the backup size.",
            foreground=Colors.WARNING,
        )
        self.retrieve_btn.state(["disabled"])

        def _do_retrieve():
            try:
                from src.core.backup_engine import BackupEngine

                engine = BackupEngine.__new__(BackupEngine)
                backend = engine._get_backend(config)
                logger.info("Retrieve: listing backups on %s", config.storage_type.value)
                backups = backend.list_backups()
                logger.info("Retrieve: found %d backup(s)", len(backups))
                if not backups:
                    self.after(
                        0,
                        lambda: self._on_retrieve_error("No files found on remote."),
                    )
                    return
                for b in backups:
                    logger.info("Retrieve: downloading %s", b["name"])
                    backend.download_backup(b["name"], dest)
                self.after(0, lambda: self._on_retrieve_done(str(dest)))
            except Exception as e:
                logger.error("Failed to retrieve: %s", e)
                self.after(0, lambda _e=e: self._on_retrieve_error(str(_e)))

        threading.Thread(target=_do_retrieve, daemon=True).start()

    def _on_retrieve_done(self, local_path: str) -> None:
        """Handle successful retrieve.

        Args:
            local_path: Path to the downloaded backup folder.
        """
        self.retrieve_btn.state(["!disabled"])
        self.retrieve_label.config(text="Retrieve complete", foreground=Colors.SUCCESS)
        self.backup_path_var.set(local_path)

    def _on_retrieve_error(self, error: str) -> None:
        """Handle retrieve error.

        Args:
            error: Error message.
        """
        self.retrieve_btn.state(["!disabled"])
        self.retrieve_label.config(text=f"Error: {error}", foreground=Colors.DANGER)

    # ------------------------------------------------------------------
    # Browse helpers
    # ------------------------------------------------------------------

    def _browse_backup(self) -> None:
        """Browse for a backup folder or encrypted .tar.wbenc file.

        Opens a file dialog first (to allow selecting .tar.wbenc files).
        If the user cancels, falls back to a folder dialog.
        """
        path = filedialog.askopenfilename(
            title="Select backup (.tar.wbenc) or cancel for folder",
            filetypes=[
                ("Encrypted backups", "*.wbenc"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            path = filedialog.askdirectory(title="Select backup folder")
        if path:
            self.backup_path_var.set(path)

    def _browse_dest(self) -> None:
        """Browse for a restore destination folder."""
        path = filedialog.askdirectory(title="Select restore destination")
        if path:
            self.dest_path_var.set(path)

    def _browse_retrieve_dest(self) -> None:
        """Browse for a retrieve destination folder."""
        path = filedialog.askdirectory(title="Select retrieve destination")
        if path:
            self.retrieve_dest_var.set(path)

    @staticmethod
    def _browse_to_var(var: tk.StringVar) -> None:
        """Browse for a directory and set result to a StringVar.

        Args:
            var: Target StringVar to update.
        """
        path = filedialog.askdirectory(title="Select folder")
        if path:
            var.set(path)

    @staticmethod
    def _browse_key_to_var(var: tk.StringVar) -> None:
        """Browse for an SSH key file and set result to a StringVar.

        Args:
            var: Target StringVar to update.
        """
        path = filedialog.askopenfilename(
            title="Select SSH private key",
            filetypes=[
                ("SSH keys", "*.pem *.key *.ppk id_*"),
                ("All files", "*.*"),
            ],
        )
        if path:
            var.set(path)

    # ------------------------------------------------------------------
    # Password handling
    # ------------------------------------------------------------------

    def _on_backup_path_changed(self, *_args) -> None:
        """Check for encrypted files when backup path changes."""
        path = self.backup_path_var.get().strip()
        if not path:
            self.password_var.set("")
            self._user_modified_pw = False
            return
        src = Path(path)
        if not src.exists():
            return
        has_encrypted = src.suffix == ".wbenc" or (src.is_dir() and any(src.rglob("*.wbenc")))
        if has_encrypted and self._stored_password:
            self._user_modified_pw = False
            self.password_var.set(_PASSWORD_PLACEHOLDER)
            self._user_modified_pw = False
        elif not has_encrypted:
            self.password_var.set("")
            self._user_modified_pw = False

    def _on_password_changed(self, *_args) -> None:
        """Track manual password edits."""
        current = self.password_var.get()
        if current != _PASSWORD_PLACEHOLDER:
            self._user_modified_pw = True

    def _get_effective_password(self) -> str:
        """Return the password to use for decryption.

        Returns:
            User-typed password if modified, else stored profile password.
        """
        if self._user_modified_pw:
            return self.password_var.get()
        return self._stored_password

    # ------------------------------------------------------------------
    # Restore
    # ------------------------------------------------------------------

    def _restore(self) -> None:
        """Validate inputs and launch restore in background."""
        backup_path = self.backup_path_var.get().strip()
        dest_path = self.dest_path_var.get().strip()

        if not backup_path:
            messagebox.showwarning("Restore", "Please select a backup folder.")
            return
        src = Path(backup_path)
        if not src.exists():
            messagebox.showwarning("Restore", f"Backup does not exist:\n{backup_path}")
            return
        if not dest_path:
            messagebox.showwarning("Restore", "Please select a restore destination.")
            return

        password = self._get_effective_password()

        has_encrypted = src.suffix == ".wbenc" or (
            src.is_dir() and any(f.suffix == ".wbenc" for f in src.rglob("*"))
        )
        if has_encrypted and not password:
            messagebox.showwarning(
                "Restore",
                "This backup contains encrypted files but no password "
                "was provided.\nPlease enter your encryption password.",
            )
            return

        self.status_label.config(
            text="Restoring... This may take a long time depending on the backup size.",
            foreground=Colors.WARNING,
        )
        self.restore_btn.state(["disabled"])

        threading.Thread(
            target=self._do_restore,
            args=(src, Path(dest_path), password),
            daemon=True,
        ).start()

    def _do_restore(self, src: Path, dst: Path, password: str) -> None:
        """Restore files from backup to destination.

        Supports two backup formats:
        - .tar.wbenc file: encrypted tar archive (streamed decryption)
        - Plain directory: copy files as-is

        Args:
            src: Source backup (directory or .tar.wbenc file).
            dst: Destination directory.
            password: Decryption password (empty string if not needed).
        """
        try:
            # Detect .tar.wbenc file (either src itself or alongside a dir)
            tar_wbenc = None
            if src.is_file() and src.name.endswith(".tar.wbenc"):
                tar_wbenc = src
            elif src.is_dir():
                candidate = src.with_suffix(".tar.wbenc")
                if candidate.is_file():
                    tar_wbenc = candidate

            if tar_wbenc is not None:
                self._restore_encrypted_tar(tar_wbenc, dst, password)
                return

            # Plain directory: copy files (skip internal .wbverify manifests)
            copied = 0
            files = [
                f for f in src.rglob("*")
                if f.is_file() and not f.name.endswith(".wbverify")
            ]
            if not files:
                self.after(
                    0,
                    lambda: self._restore_done(False, "No files found in backup"),
                )
                return

            for f in files:
                rel = f.relative_to(src)
                target = dst / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(f, target)
                copied += 1

            msg = f"Restore complete — {copied} files copied"
            self.after(0, lambda: self._restore_done(True, msg))

        except Exception as e:
            logger.exception("Restore failed")
            self.after(0, lambda _e=e: self._restore_done(False, str(_e)))

    def _restore_encrypted_tar(
        self,
        tar_path: Path,
        dst: Path,
        password: str,
    ) -> None:
        """Restore from a .tar.wbenc encrypted archive.

        Args:
            tar_path: Path to the .tar.wbenc file.
            dst: Destination directory.
            password: Decryption password.
        """
        import tarfile

        if not password:
            self.after(
                0,
                lambda: self._restore_done(False, "Password required for encrypted backup"),
            )
            return

        try:
            # Create a subfolder named after the backup file
            # e.g. "loicata_FULL_2026-04-01_215315.tar.wbenc" → "loicata_FULL_2026-04-01_215315"
            backup_name = tar_path.name
            if backup_name.endswith(".tar.wbenc"):
                backup_name = backup_name[: -len(".tar.wbenc")]
            restore_dir = dst / backup_name
            long_path_mkdir(restore_dir)

            count = 0
            strip_prefix = ""
            with open(tar_path, "rb") as f:
                reader = DecryptingReader(f, password)
                with tarfile.open(fileobj=reader, mode="r|") as tar:
                    for member in tar:
                        if member.name.endswith(".wbverify"):
                            continue
                        # Strip the first directory level from tar paths
                        # e.g. "loicata/Documents/file.txt" → "Documents/file.txt"
                        if not strip_prefix and "/" in member.name:
                            strip_prefix = member.name.split("/")[0] + "/"
                        name = member.name
                        if strip_prefix and name.startswith(strip_prefix):
                            name = name[len(strip_prefix):]
                        if not name:
                            continue
                        if member.isdir():
                            long_path_mkdir(restore_dir / name)
                            continue
                        # Extract file with long path support
                        target = restore_dir / name
                        long_path_mkdir(target.parent)
                        fileobj = tar.extractfile(member)
                        if fileobj is not None:
                            with open(long_path_str(target), "wb") as out:
                                shutil.copyfileobj(fileobj, out)
                            count += 1

            msg = f"Restore complete — {count} files decrypted"
            self.after(0, lambda: self._restore_done(True, msg))

        except Exception as e:
            err_msg = str(e)
            if "tag" in err_msg.lower() or "authentication" in err_msg.lower():
                err_msg = "Decryption failed — wrong password?"
            logger.exception("Encrypted restore failed")
            self.after(0, lambda _e=err_msg: self._restore_done(False, _e))

    def _restore_done(self, ok: bool, msg: str) -> None:
        """Display restore result.

        Args:
            ok: True if restore succeeded.
            msg: Status message.
        """
        self.restore_btn.state(["!disabled"])
        color = Colors.SUCCESS if ok else Colors.DANGER
        self.status_label.config(text=msg, foreground=color)

    # ------------------------------------------------------------------
    # Profile loading
    # ------------------------------------------------------------------

    def load_profile(self, profile: BackupProfile) -> None:
        """Load recovery settings from profile.

        Args:
            profile: The active backup profile.
        """
        self._profile = profile
        self._stored_password = profile.encryption.stored_password or ""
        self._user_modified_pw = False
        self.password_var.set("")
        self.backup_path_var.set("")
        self.dest_path_var.set("")
        self.status_label.config(text="")
        self.retrieve_label.config(text="")

        # Pre-fill retrieve from profile storage
        self.source_var.set("Storage")
        config = self._get_source_storage_config()
        if config:
            self._fill_retrieve_from_config(config)

    def collect_config(self) -> dict:
        """Recovery tab has no persistent config.

        Returns:
            Empty dict.
        """
        return {}
