"""Recovery tab: restore local backups and retrieve remote ones."""

import logging
import os
import shutil
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from src.core.config import BackupProfile, StorageConfig, StorageType
from src.installer import get_available_features, FEAT_SFTP, FEAT_S3
from src.security.encryption import decrypt_file
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
            foreground=Colors.WARNING,
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
            (StorageType.PROTON, "Proton Drive", True),
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
        self._build_retrieve_proton_config()

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
            frame,
            textvariable=self._ret_s3_provider_var,
            values=providers,
            state="readonly",
        ).pack(fill="x")

        fields = [
            ("Bucket", "s3_bucket", ""),
            ("Prefix (optional)", "s3_prefix", ""),
            ("Region", "s3_region", "eu-west-1"),
            ("Access Key", "s3_access_key", ""),
            ("Secret Key", "s3_secret_key", ""),
            ("Endpoint URL (for S3-compatible)", "s3_endpoint_url", ""),
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

    def _build_retrieve_proton_config(self) -> None:
        """Build config fields for Proton Drive."""
        frame = ttk.Frame(self._retrieve_config_container)
        self._retrieve_config_frames["proton"] = frame

        fields = [
            ("Proton username", "proton_username", ""),
            ("Proton password", "proton_password", ""),
            ("2FA seed (optional)", "proton_2fa", ""),
            ("Remote path", "proton_remote_path", "/Backups"),
            ("rclone path (optional)", "proton_rclone_path", ""),
        ]

        self._ret_proton_vars: dict[str, tk.StringVar] = {}
        for label, key, default in fields:
            ttk.Label(frame, text=f"{label}:").pack(anchor="w", pady=(Spacing.SMALL, 0))
            var = tk.StringVar(value=default)
            self._ret_proton_vars[key] = var
            if "password" in key or "2fa" in key:
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
        """Show config fields for the selected storage type."""
        for frame in self._retrieve_config_frames.values():
            frame.pack_forget()

        stype = self.retrieve_type_var.get()
        frame = self._retrieve_config_frames.get(stype)
        if frame:
            frame.pack(fill="x")

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

    def _fill_retrieve_from_config(self, config: StorageConfig) -> None:
        """Pre-fill retrieve fields from a StorageConfig.

        Args:
            config: Storage configuration to read from.
        """
        self.retrieve_type_var.set(config.storage_type.value)

        # Local
        self._ret_local_path_var.set(config.destination_path or "")

        # Network
        self._ret_network_path_var.set(config.destination_path or "")

        # SFTP
        for key, var in self._ret_sftp_vars.items():
            val = getattr(config, key, "")
            var.set(str(val) if val else "")

        # S3
        self._ret_s3_provider_var.set(config.s3_provider or "aws")
        for key, var in self._ret_s3_vars.items():
            val = getattr(config, key, "")
            var.set(str(val) if val else "")

        # Proton
        for key, var in self._ret_proton_vars.items():
            val = getattr(config, key, "")
            var.set(str(val) if val else "")

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
        elif stype == StorageType.PROTON:
            for key, var in self._ret_proton_vars.items():
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
                local_path = backend.download_all(dest)
                self.after(0, lambda: self._on_retrieve_done(str(local_path)))
            except AttributeError:
                # Fallback: download each backup individually
                try:
                    backups = backend.list_backups()
                    if not backups:
                        self.after(
                            0,
                            lambda: self._on_retrieve_error("No files found on remote."),
                        )
                        return
                    last_path = None
                    for b in backups:
                        last_path = backend.download_backup(b["name"], dest)
                    self.after(
                        0,
                        lambda: self._on_retrieve_done(str(dest)),
                    )
                except Exception as e2:
                    logger.error("Failed to retrieve backups: %s", e2)
                    self.after(0, lambda: self._on_retrieve_error(str(e2)))
            except Exception as e:
                logger.error("Failed to retrieve: %s", e)
                self.after(0, lambda: self._on_retrieve_error(str(e)))

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
        """Browse for a local backup folder."""
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
        if not src.is_dir():
            return
        has_encrypted = any(src.rglob("*.wbenc"))
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
        if not src.is_dir():
            messagebox.showwarning("Restore", f"Backup folder does not exist:\n{backup_path}")
            return
        if not dest_path:
            messagebox.showwarning("Restore", "Please select a restore destination.")
            return

        password = self._get_effective_password()

        src_files = [f for f in src.rglob("*") if f.is_file()]
        has_encrypted = any(f.suffix == ".wbenc" for f in src_files)
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
        """Restore files from backup folder to destination.

        Args:
            src: Source backup directory.
            dst: Destination directory.
            password: Decryption password (empty string if not needed).
        """
        try:
            copied = 0
            decrypted = 0
            skipped = 0

            files = [f for f in src.rglob("*") if f.is_file()]
            if not files:
                self.after(
                    0,
                    lambda: self._restore_done(False, "No files found in backup folder"),
                )
                return

            for f in files:
                rel = f.relative_to(src)

                if f.suffix == ".wbenc":
                    if not password:
                        skipped += 1
                        logger.warning("Skipped encrypted file (no password): %s", rel)
                        continue
                    target = dst / rel.with_suffix("")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    ok = decrypt_file(f, target, password)
                    if ok:
                        decrypted += 1
                    else:
                        skipped += 1
                        logger.warning("Failed to decrypt: %s", rel)
                else:
                    target = dst / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, target)
                    copied += 1

            parts = []
            if copied:
                parts.append(f"{copied} copied")
            if decrypted:
                parts.append(f"{decrypted} decrypted")
            if skipped:
                parts.append(f"{skipped} skipped")

            success = (copied + decrypted) > 0 and skipped == 0
            if copied + decrypted == 0:
                msg = f"Restore failed — {skipped} files skipped " "(wrong password?)"
            elif skipped > 0:
                msg = f"Restore partial — {', '.join(parts)}"
            else:
                msg = f"Restore complete — {', '.join(parts)}"

            self.after(0, lambda: self._restore_done(success, msg))
        except Exception as e:
            logger.exception("Restore failed")
            self.after(0, lambda: self._restore_done(False, str(e)))

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
