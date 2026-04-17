"""Recovery tab: unified retrieve + restore workflow.

Single linear flow:
  1. Source  (External drive / Network / SFTP / S3 with scan)
  2. Select backups  (treeview — remote only, grouped by bucket for S3)
  3. Encryption password  (only when encrypted backups are selected)
  4. Destination
"""

import logging
import shutil
import threading
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from src.core.config import BackupProfile, StorageConfig, StorageType
from src.installer import FEAT_S3, FEAT_SFTP, get_available_features
from src.security.encryption import DecryptingReader
from src.storage.base import StorageBackend, long_path_mkdir, long_path_str
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)

_PASSWORD_PLACEHOLDER = "****************"

# Patterns that identify a Backup Manager backup in an S3 bucket.
_BACKUP_PATTERNS = ("_FULL_", "_DIFF_", ".wbverify")


def _is_within_restore_dir(target: Path, restore_dir: Path) -> bool:
    """Return True if ``target`` stays inside ``restore_dir`` after resolving.

    Defense against path-traversal in tar archives: a hostile archive
    could hold members with absolute paths or ``..`` segments that
    would extract outside the user-selected restore directory.
    Backup Manager never produces such archives, but the check is
    cheap and removes the trust requirement on the archive source.
    """
    try:
        target_abs = target.resolve()
        restore_abs = restore_dir.resolve()
    except (OSError, RuntimeError):
        return False
    try:
        target_abs.relative_to(restore_abs)
    except ValueError:
        return False
    return True


def _parse_backup_type(name: str) -> str:
    """Extract backup type (FULL / DIFF) from a backup name.

    Args:
        name: Backup name, e.g. 'loicata_FULL_2026-04-10_120000'.

    Returns:
        'FULL', 'DIFF', or '' if not detected.
    """
    upper = name.upper()
    if "_FULL_" in upper or upper.startswith("FULL_"):
        return "FULL"
    if "_DIFF_" in upper or upper.startswith("DIFF_"):
        return "DIFF"
    return ""


def _is_encrypted_name(name: str) -> bool:
    """Check if a backup name indicates an encrypted archive.

    Args:
        name: Backup name or filename.

    Returns:
        True if the name contains '.wbenc'.
    """
    return ".wbenc" in name.lower()


def _human_size(size_bytes: int) -> str:
    """Format byte count as human-readable string.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Formatted string, e.g. '2.1 GB', '340 MB'.
    """
    if size_bytes < 0:
        return "?"
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(value) < 1024:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"


def _format_date(timestamp: float) -> str:
    """Format a Unix timestamp as a short date string.

    Args:
        timestamp: Unix timestamp (seconds since epoch).

    Returns:
        Formatted string, e.g. '10/04/2026'.
    """
    if not timestamp or timestamp <= 0:
        return ""
    try:
        dt = datetime.fromtimestamp(timestamp, tz=UTC)
        return dt.strftime("%d/%m/%Y")
    except (OSError, ValueError):
        return ""


def _is_backup_object(key: str) -> bool:
    """Check if an S3 object key looks like a Backup Manager backup.

    Args:
        key: S3 object key.

    Returns:
        True if the key matches known backup patterns.
    """
    upper = key.upper()
    return any(pat in upper for pat in ("_FULL_", "_DIFF_")) or key.endswith(".wbverify")


class RecoveryTab(ScrollableTab):
    """Unified restore / retrieve tab."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._stored_password: str = ""
        self._user_modified_pw = False
        self._profile: BackupProfile | None = None
        self._features = get_available_features()
        self._config_frames: dict[str, ttk.Frame] = {}
        self._filling = False
        self._listed_backups: list[dict] = []
        self._selected_backups: set[str] = set()
        self._scan_animation_id: str | None = None
        self._build_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        """Build all UI sections.

        The backup list, destination, encryption, and execute sections
        are built first (unpacked) so that _build_source_section can
        reference them when it triggers _on_source_type_changed.
        """
        # Build unpacked sections first (order doesn't matter for these)
        self._build_backup_list_section()
        self._build_destination_section()
        self._build_encryption_section()
        self._build_execute_section()

        # Build and pack the source section (calls _on_source_type_changed)
        self._build_source_section()

    # --- Step 1: Source ---

    def _build_source_section(self) -> None:
        """Build the source selection section with 4 storage types."""
        self._source_frame = ttk.LabelFrame(self.inner, text="1. Source", padding=Spacing.PAD)
        self._source_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE)

        # Auto-fill combo (hidden when no profile)
        self._autofill_frame = ttk.Frame(self._source_frame)
        self._autofill_frame.pack(fill="x", pady=(0, Spacing.MEDIUM))

        ttk.Label(self._autofill_frame, text="Auto-fill from profile:").pack(side="left")
        self.source_var = tk.StringVar(value="Storage")
        self._source_combo = ttk.Combobox(
            self._autofill_frame,
            textvariable=self.source_var,
            values=["Storage", "Mirror 1", "Mirror 2"],
            state="readonly",
            width=20,
        )
        self._source_combo.pack(side="left", padx=(Spacing.SMALL, 0))
        self._source_combo.bind("<<ComboboxSelected>>", self._on_source_changed)

        # Storage type radio buttons
        self.source_type_var = tk.StringVar(value=StorageType.LOCAL.value)
        self.source_type_var.trace_add("write", self._on_source_type_changed)

        type_options = [
            (StorageType.LOCAL, "External drive", True),
            (StorageType.NETWORK, "Network folder", True),
            (StorageType.SFTP, "Remote server (SFTP)", FEAT_SFTP in self._features),
            (StorageType.S3, "S3 cloud", FEAT_S3 in self._features),
        ]
        for stype, label, available in type_options:
            ttk.Radiobutton(
                self._source_frame,
                text=label,
                value=stype.value,
                variable=self.source_type_var,
                state="normal" if available else "disabled",
            ).pack(anchor="w", pady=2)

        # Dynamic config container
        self._config_container = ttk.Frame(self._source_frame)
        self._config_container.pack(fill="x", padx=(Spacing.XLARGE, 0))

        self._build_local_config()
        self._build_network_config()
        self._build_sftp_config()
        self._build_s3_config()

        # List / Scan button (for remote types) — Accent style, full width
        self._list_btn = ttk.Button(
            self._source_frame,
            text="List available backups",
            style="Accent.TButton",
            command=self._list_remote_backups,
        )
        # Not packed initially

        # Scan status label (animated for S3 scan)
        self._scan_label = ttk.Label(
            self._source_frame, text="", foreground=Colors.TEXT_SECONDARY, font=Fonts.small()
        )

        # Show initial config frame
        self._on_source_type_changed()

    def _build_local_config(self) -> None:
        """Build config fields for external drive."""
        frame = ttk.Frame(self._config_container)
        self._config_frames["local"] = frame

        row = ttk.Frame(frame)
        row.pack(fill="x", pady=(Spacing.SMALL, 0))
        self.backup_path_var = tk.StringVar()
        self.backup_path_var.trace_add("write", self._on_backup_path_changed)
        ttk.Entry(row, textvariable=self.backup_path_var).pack(side="left", fill="x", expand=True)
        ttk.Button(row, text="Browse encrypted backup", command=self._browse_encrypted).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )
        ttk.Button(row, text="Browse backup", command=self._browse_folder).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )

    def _build_network_config(self) -> None:
        """Build config fields for network folder."""
        frame = ttk.Frame(self._config_container)
        self._config_frames["network"] = frame

        ttk.Label(frame, text="Network path (UNC):").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._net_path_var = tk.StringVar()
        self._net_path_var.trace_add("write", self._on_backup_path_changed)
        ttk.Entry(frame, textvariable=self._net_path_var).pack(fill="x")

        ttk.Label(frame, text="Username:").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._net_user_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self._net_user_var).pack(fill="x")

        ttk.Label(frame, text="Password:").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._net_pass_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self._net_pass_var, show="\u25cf").pack(fill="x")

    def _build_sftp_config(self) -> None:
        """Build config fields for SFTP."""
        frame = ttk.Frame(self._config_container)
        self._config_frames["sftp"] = frame

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
                row = ttk.Frame(frame)
                row.pack(fill="x")
                ttk.Entry(row, textvariable=var).pack(side="left", fill="x", expand=True)
                ttk.Button(
                    row,
                    text="Browse...",
                    command=lambda v=var: self._browse_key_to_var(v),
                ).pack(side="right", padx=(Spacing.SMALL, 0))
            elif "password" in key or "passphrase" in key:
                ttk.Entry(frame, textvariable=var, show="\u25cf").pack(fill="x")
            elif key == "sftp_port":
                ttk.Spinbox(frame, textvariable=var, from_=1, to=65535, width=8).pack(anchor="w")
            else:
                ttk.Entry(frame, textvariable=var).pack(fill="x")

    def _build_s3_config(self) -> None:
        """Build config fields for S3 cloud.

        Layout: Provider, Access Key, Secret Key, then Region label
        that changes between 'Region (optional):' for Amazon and
        'Region:' for other providers.  Followed by optional Bucket,
        Prefix, and Endpoint URL.
        """
        frame = ttk.Frame(self._config_container)
        self._config_frames["s3"] = frame

        self._ret_s3_vars: dict[str, tk.StringVar] = {}

        # Provider
        ttk.Label(frame, text="Provider:").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._ret_s3_provider_var = tk.StringVar(value="Amazon AWS")
        self._ret_s3_provider_var.trace_add("write", self._on_s3_provider_changed)
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
        self._ret_s3_provider_cb = ttk.Combobox(
            frame,
            textvariable=self._ret_s3_provider_var,
            values=providers,
            state="readonly",
        )
        self._ret_s3_provider_cb.pack(fill="x")

        # Access Key
        ttk.Label(frame, text="Access Key:").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._ret_s3_vars["s3_access_key"] = tk.StringVar()
        ttk.Entry(frame, textvariable=self._ret_s3_vars["s3_access_key"]).pack(fill="x")

        # Secret Key
        ttk.Label(frame, text="Secret Key:").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._ret_s3_vars["s3_secret_key"] = tk.StringVar()
        ttk.Entry(frame, textvariable=self._ret_s3_vars["s3_secret_key"], show="\u25cf").pack(
            fill="x"
        )

        # Region (label changes depending on provider)
        self._s3_region_label = ttk.Label(frame, text="Region (optional):")
        self._s3_region_label.pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._ret_s3_vars["s3_region"] = tk.StringVar(value="")
        ttk.Entry(frame, textvariable=self._ret_s3_vars["s3_region"]).pack(fill="x")

        # Bucket (optional)
        ttk.Label(frame, text="Bucket (optional):").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._ret_s3_vars["s3_bucket"] = tk.StringVar()
        ttk.Entry(frame, textvariable=self._ret_s3_vars["s3_bucket"]).pack(fill="x")

        # Prefix (optional)
        ttk.Label(frame, text="Prefix (optional):").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._ret_s3_vars["s3_prefix"] = tk.StringVar()
        ttk.Entry(frame, textvariable=self._ret_s3_vars["s3_prefix"]).pack(fill="x")

        # Endpoint URL (optional)
        ttk.Label(frame, text="Endpoint URL (optional):").pack(anchor="w", pady=(Spacing.SMALL, 0))
        self._ret_s3_vars["s3_endpoint_url"] = tk.StringVar()
        ttk.Entry(frame, textvariable=self._ret_s3_vars["s3_endpoint_url"]).pack(fill="x")

    def _on_s3_provider_changed(self, *_args) -> None:
        """Update Region label and re-fill fields when provider changes.

        Amazon AWS uses a global endpoint so region is optional.
        All other providers require region to build the endpoint URL.

        When the user manually changes provider, try to find a matching
        config in the profile.  If found, fill from it.  Otherwise clear
        all S3 fields so stale credentials from another provider are not
        shown.
        """
        provider = self._ret_s3_provider_var.get()
        if provider == "Amazon AWS":
            self._s3_region_label.config(text="Region (optional):")
        else:
            self._s3_region_label.config(text="Region:")

        # Skip field updates during programmatic fills
        if self._filling:
            return

        # Try to find a matching config in profile for the new provider
        if self._profile:
            configs = [self._profile.storage] + list(self._profile.mirror_destinations)
            for cfg in configs:
                if cfg.storage_type == StorageType.S3 and cfg.s3_provider == provider:
                    self._filling = True
                    try:
                        self._fill_fields(cfg)
                    finally:
                        self._filling = False
                    return

        # No matching config — clear all S3 fields. The previous
        # code had an if/else where both branches called ``var.set("")``,
        # which served no purpose beyond suggesting a distinction that
        # did not exist.
        for var in self._ret_s3_vars.values():
            var.set("")

    # --- Step 2: Select backups ---

    def _build_backup_list_section(self) -> None:
        """Build the backup selection treeview."""
        self._list_frame = ttk.LabelFrame(self.inner, text="2. Select backups", padding=Spacing.PAD)

        ttk.Label(
            self._list_frame,
            text="Each DIFF contains all changes since the last FULL. "
            "To restore: select 1 FULL + 1 DIFF.",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w", pady=(0, Spacing.SMALL))

        tree_container = ttk.Frame(self._list_frame)
        tree_container.pack(fill="both", expand=True)

        columns = ("type", "encrypted", "size", "date")
        self._tree = ttk.Treeview(
            tree_container,
            columns=columns,
            show="tree headings",
            height=12,
            selectmode="none",
        )
        self._tree.heading("#0", text="Name", anchor="w")
        self._tree.heading("type", text="Type", anchor="w")
        self._tree.heading("encrypted", text="Encrypted", anchor="w")
        self._tree.heading("size", text="Size", anchor="e")
        self._tree.heading("date", text="Date", anchor="w")

        self._tree.column("#0", width=350, minwidth=200)
        self._tree.column("type", width=50, minwidth=40)
        self._tree.column("encrypted", width=70, minwidth=55)
        self._tree.column("size", width=90, minwidth=70, anchor="e")
        self._tree.column("date", width=100, minwidth=80)

        tree_scroll = ttk.Scrollbar(tree_container, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=tree_scroll.set)
        self._tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")
        self._tree.bind("<ButtonRelease-1>", self._on_tree_click)

        bottom_row = ttk.Frame(self._list_frame)
        bottom_row.pack(fill="x", pady=(Spacing.SMALL, 0))

        self._selection_summary = ttk.Label(
            bottom_row, text="", foreground=Colors.TEXT_SECONDARY, font=Fonts.small()
        )
        self._selection_summary.pack(side="left")

        ttk.Button(bottom_row, text="None", command=self._select_none).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )
        ttk.Button(bottom_row, text="All", command=self._select_all).pack(side="right")
        ttk.Button(bottom_row, text="\u21bb", width=3, command=self._list_remote_backups).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )

    # --- Step 3: Encryption password ---

    def _build_destination_section(self) -> None:
        """Build the destination selection."""
        self._dest_frame = ttk.LabelFrame(self.inner, text="4. Destination", padding=Spacing.PAD)
        row = ttk.Frame(self._dest_frame)
        row.pack(fill="x")
        self.dest_path_var = tk.StringVar()
        ttk.Entry(row, textvariable=self.dest_path_var).pack(side="left", fill="x", expand=True)
        ttk.Button(row, text="Browse...", command=self._browse_dest).pack(
            side="right", padx=(Spacing.SMALL, 0)
        )

    # --- Step 4: Encryption password ---

    def _build_encryption_section(self) -> None:
        """Build the encryption password input."""
        self._pw_frame = ttk.LabelFrame(
            self.inner, text="3. Encryption password", padding=Spacing.PAD
        )
        self.password_var = tk.StringVar()
        self.password_var.trace_add("write", self._on_password_changed)
        self._pw_entry = ttk.Entry(self._pw_frame, textvariable=self.password_var, show="\u25cf")
        self._pw_entry.pack(fill="x")
        self._pw_hint = ttk.Label(
            self._pw_frame, text="", foreground=Colors.TEXT_SECONDARY, font=Fonts.small()
        )
        self._pw_hint.pack(anchor="w", pady=(Spacing.SMALL, 0))

    # --- Execute button ---

    def _build_execute_section(self) -> None:
        """Build the main action button and progress label."""
        self._exec_frame = ttk.Frame(self.inner)
        self._exec_btn = ttk.Button(
            self._exec_frame,
            text="Restore",
            style="Accent.TButton",
            command=self._execute,
        )
        self._exec_btn.pack(side="left")
        self.status_label = ttk.Label(self._exec_frame, text="", foreground=Colors.TEXT_SECONDARY)
        self.status_label.pack(side="left", padx=Spacing.LARGE)

    # ------------------------------------------------------------------
    # Visibility management
    # ------------------------------------------------------------------

    def _on_source_type_changed(self, *_args) -> None:
        """Show config for selected type, update button visibility."""
        for frame in self._config_frames.values():
            frame.pack_forget()

        stype = self.source_type_var.get()
        frame = self._config_frames.get(stype)
        if frame:
            frame.pack(fill="x", pady=(Spacing.SMALL, 0))

        # Show list/scan button for remote types
        is_remote = stype in (StorageType.SFTP.value, StorageType.S3.value)
        if is_remote:
            btn_text = (
                "Scan for backups" if stype == StorageType.S3.value else "List available backups"
            )
            self._list_btn.config(text=btn_text)
            self._list_btn.pack(anchor="w", pady=(Spacing.LARGE, 0))
            self._scan_label.pack(anchor="w", pady=(Spacing.SMALL, 0))
        else:
            self._list_btn.pack_forget()
            self._scan_label.pack_forget()

        # Hide treeview when type changes
        self._list_frame.pack_forget()
        self._listed_backups.clear()
        self._selected_backups.clear()

        # Update downstream sections
        if is_remote:
            self._hide_post_source_sections()
        else:
            self._update_post_source_sections()

    def _update_post_source_sections(self) -> None:
        """Show or hide destination, password, execute based on state."""
        stype = self.source_type_var.get()
        has_source = False
        has_encrypted = False

        if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            path = self._get_local_path().strip()
            if path:
                has_source = True
                src = Path(path)
                if src.exists():
                    has_encrypted = src.suffix == ".wbenc" or (
                        src.is_dir() and any(src.rglob("*.wbenc"))
                    )
        else:
            has_source = len(self._selected_backups) > 0
            has_encrypted = any(
                b.get("encrypted", False)
                for b in self._listed_backups
                if b.get("name") in self._selected_backups
            )

        if has_source:
            # Always unpack all, then re-pack in correct order: pw → dest → exec
            self._pw_frame.pack_forget()
            self._dest_frame.pack_forget()
            self._exec_frame.pack_forget()

            if has_encrypted:
                self._pw_frame.pack(fill="x", padx=Spacing.LARGE, pady=(Spacing.MEDIUM, 0))
                self._update_password_hint()
            self._dest_frame.pack(fill="x", padx=Spacing.LARGE, pady=(Spacing.MEDIUM, 0))
            self._exec_frame.pack(
                fill="x", padx=Spacing.LARGE, pady=(Spacing.MEDIUM, Spacing.LARGE)
            )
        else:
            self._hide_post_source_sections()

    def _hide_post_source_sections(self) -> None:
        """Hide destination, password, and execute sections."""
        self._dest_frame.pack_forget()
        self._pw_frame.pack_forget()
        self._exec_frame.pack_forget()

    def _update_password_hint(self) -> None:
        """Update the hint text below the password field."""
        if self._stored_password:
            self._pw_hint.config(text="Saved password will be used if left unchanged")
        else:
            self._pw_hint.config(text="")

    def _get_local_path(self) -> str:
        """Return the browse path for local/network source types.

        Returns:
            Path string from the appropriate field.
        """
        stype = self.source_type_var.get()
        if stype == StorageType.NETWORK.value:
            return self._net_path_var.get()
        return self.backup_path_var.get()

    # ------------------------------------------------------------------
    # Auto-fill from profile
    # ------------------------------------------------------------------

    def _on_source_changed(self, _event=None) -> None:
        """Pre-fill type and config from profile when combo changes."""
        config = self._get_source_storage_config()
        if not config:
            return
        self._filling = True
        try:
            self.source_type_var.set(config.storage_type.value)
            self._fill_fields(config)
        finally:
            self._filling = False

    def _find_profile_config_by_type(self, stype: StorageType) -> StorageConfig | None:
        """Search profile configs for one matching the given type.

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
        """Get storage config from profile based on combo selection.

        Returns:
            StorageConfig or None.
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

    def _fill_fields(self, config: StorageConfig) -> None:
        """Fill UI fields from a StorageConfig.

        Args:
            config: Storage configuration to read from.
        """
        stype = config.storage_type

        if stype == StorageType.LOCAL:
            self.backup_path_var.set(config.destination_path or "")
        elif stype == StorageType.NETWORK:
            self._net_path_var.set(config.destination_path or "")
            self._net_user_var.set(config.network_username or "")
            self._net_pass_var.set(config.network_password or "")
        elif stype == StorageType.SFTP:
            for key, var in self._ret_sftp_vars.items():
                val = getattr(config, key, "")
                var.set(str(val) if val else "")
        elif stype == StorageType.S3:
            provider = config.s3_provider or "Amazon AWS"
            for key, var in self._ret_s3_vars.items():
                # For Amazon AWS, skip bucket/prefix/region to encourage
                # full-account scan (user may not know which bucket).
                if provider == "Amazon AWS" and key in (
                    "s3_bucket",
                    "s3_prefix",
                    "s3_region",
                ):
                    var.set("")
                    continue
                val = getattr(config, key, "")
                var.set(str(val) if val else "")
            self._ret_s3_provider_var.set(provider)
            self._ret_s3_provider_cb.set(provider)

    def _build_storage_config(self) -> StorageConfig:
        """Build a StorageConfig from current UI fields.

        Returns:
            Configured StorageConfig.
        """
        stype = StorageType(self.source_type_var.get())
        config = StorageConfig()

        if stype == StorageType.LOCAL:
            config.destination_path = self.backup_path_var.get()
        elif stype == StorageType.NETWORK:
            config.destination_path = self._net_path_var.get()
            config.network_username = self._net_user_var.get()
            config.network_password = self._net_pass_var.get()
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

    # ------------------------------------------------------------------
    # Backup listing (SFTP) / Scan (S3)
    # ------------------------------------------------------------------

    def _list_remote_backups(self) -> None:
        """List or scan backups depending on source type."""
        stype = self.source_type_var.get()
        self._list_btn.state(["disabled"])
        self._hide_post_source_sections()
        self._list_frame.pack_forget()

        if stype == StorageType.S3.value:
            bucket = self._ret_s3_vars.get("s3_bucket", tk.StringVar()).get().strip()
            if bucket:
                # Direct listing on a specific bucket
                self._scan_label.config(text="Listing...", foreground=Colors.WARNING)
                threading.Thread(target=self._do_s3_direct_list, daemon=True).start()
            else:
                # Scan all buckets
                self._start_scan_animation("Listing buckets")
                threading.Thread(target=self._do_s3_scan, daemon=True).start()
        else:
            self._scan_label.config(text="Listing...", foreground=Colors.WARNING)
            threading.Thread(target=self._do_sftp_list, daemon=True).start()

    def _do_s3_direct_list(self) -> None:
        """List backups on a specific S3 bucket (bucket is known)."""
        try:
            config = self._build_storage_config()
            backend = self._create_backend(config)
            backups = backend.list_backups()
            self.after(0, lambda: self._on_list_done(backups, grouped=False))
        except Exception as e:
            logger.error("Failed to list S3 backups: %s", e)
            self.after(0, lambda _e=e: self._on_list_error(str(_e)))

    def _do_sftp_list(self) -> None:
        """List backups on SFTP in background."""
        try:
            config = self._build_storage_config()
            backend = self._create_backend(config)
            backups = backend.list_backups()
            self.after(0, lambda: self._on_list_done(backups, grouped=False))
        except Exception as e:
            logger.error("Failed to list backups: %s", e)
            self.after(0, lambda _e=e: self._on_list_error(str(_e)))

    def _do_s3_scan(self) -> None:
        """Scan all S3 buckets for Backup Manager backups in background."""
        try:
            import boto3
            from botocore.config import Config as BotoConfig

            from src.storage.s3 import PROVIDER_ENDPOINTS

            provider = self._ret_s3_provider_var.get()
            access_key = self._ret_s3_vars["s3_access_key"].get()
            secret_key = self._ret_s3_vars["s3_secret_key"].get()
            endpoint_url = self._ret_s3_vars["s3_endpoint_url"].get().strip()
            user_region = self._ret_s3_vars.get("s3_region", tk.StringVar()).get().strip()

            if not endpoint_url:
                template = PROVIDER_ENDPOINTS.get(provider)
                if template:
                    region_for_url = user_region or "us-east-1"
                    endpoint_url = template.format(region=region_for_url, account_id="")

            boto_config = BotoConfig(
                connect_timeout=30,
                read_timeout=60,
                retries={"max_attempts": 2, "mode": "adaptive"},
            )
            client_kwargs = {
                "service_name": "s3",
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key,
                "config": boto_config,
            }
            if endpoint_url:
                client_kwargs["endpoint_url"] = endpoint_url

            client = boto3.client(**client_kwargs)

            # Step 1: List all buckets
            response = client.list_buckets()
            buckets = [b["Name"] for b in response.get("Buckets", [])]

            if not buckets:
                self.after(0, lambda: self._on_list_error("No buckets found"))
                return

            total = len(buckets)
            all_backups: list[dict] = []
            found_count = 0

            # Step 2: Scan each bucket in parallel
            def _scan_one_bucket(bucket_name: str) -> list[dict]:
                try:
                    # boto3 clients are not safe to share across
                    # threads: their underlying session can mutate
                    # (STS/SSO credential refresh) and the endpoint
                    # resolver holds per-call state.  Each worker
                    # therefore gets its own global client for the
                    # region lookup, independent of the outer ``client``
                    # used earlier from the main thread.
                    local_client = boto3.client(**client_kwargs)
                    region_resp = local_client.get_bucket_location(Bucket=bucket_name)
                    region = region_resp.get("LocationConstraint") or "us-east-1"

                    regional_kwargs = dict(client_kwargs)
                    regional_kwargs["region_name"] = region
                    if endpoint_url and provider != "Amazon AWS":
                        tpl = PROVIDER_ENDPOINTS.get(provider)
                        if tpl:
                            regional_kwargs["endpoint_url"] = tpl.format(
                                region=region, account_id=""
                            )

                    regional_client = boto3.client(**regional_kwargs)

                    resp = regional_client.list_objects_v2(Bucket=bucket_name, MaxKeys=50)
                    keys = [o["Key"] for o in resp.get("Contents", [])]
                    has_backups = any(_is_backup_object(k) for k in keys)
                    if not has_backups:
                        return []

                    # This bucket has backups — do a full listing
                    from src.storage.s3 import S3Storage

                    storage = S3Storage(
                        bucket=bucket_name,
                        prefix="",
                        region=region,
                        access_key=access_key,
                        secret_key=secret_key,
                        endpoint_url=regional_kwargs.get("endpoint_url", ""),
                        provider=provider,
                    )
                    backup_list = storage.list_backups()
                    resolved_endpoint = regional_kwargs.get("endpoint_url", "")
                    for b in backup_list:
                        b["_bucket"] = bucket_name
                        b["_region"] = region
                        b["_endpoint"] = resolved_endpoint
                    return backup_list
                except Exception as exc:
                    logger.warning("Failed to scan bucket %s: %s", bucket_name, exc)
                    return []

            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = {pool.submit(_scan_one_bucket, name): name for name in buckets}
                for scanned, future in enumerate(as_completed(futures), 1):
                    result = future.result()
                    if result:
                        found_count += len(result)
                        all_backups.extend(result)
                    self.after(
                        0,
                        lambda s=scanned, t=total, f=found_count: (
                            self._update_scan_animation(
                                f"Scanning bucket {s}/{t}",
                                f"found {f} backups" if f > 0 else "",
                            )
                        ),
                    )

            self.after(0, lambda: self._on_list_done(all_backups, grouped=True))

        except Exception as e:
            logger.error("S3 scan failed: %s", e)
            self.after(0, lambda _e=e: self._on_list_error(str(_e)))

    # ------------------------------------------------------------------
    # Scan animation
    # ------------------------------------------------------------------

    def _start_scan_animation(self, base_text: str) -> None:
        """Start animated scanning message.

        Args:
            base_text: Base text to animate with dots.
        """
        self._scan_base_text = base_text
        self._scan_extra = ""
        self._scan_dot_count = 0
        self._animate_scan()

    # Braille spinner: 10 frames at 100 ms gives a clearly moving glyph
    # even on low-refresh displays, unlike the old "cycling dots" which
    # was too subtle to tell apart from a frozen UI.
    _SPINNER_FRAMES = (
        "\u280b",
        "\u2819",
        "\u2839",
        "\u2838",
        "\u283c",
        "\u2834",
        "\u2826",
        "\u2827",
        "\u2807",
        "\u280f",
    )

    def _animate_scan(self) -> None:
        """Animate the scan label with a visible braille spinner."""
        self._scan_dot_count = (self._scan_dot_count + 1) % len(self._SPINNER_FRAMES)
        frame = self._SPINNER_FRAMES[self._scan_dot_count]
        text = f"{frame} {self._scan_base_text}..."
        if self._scan_extra:
            text += f" {self._scan_extra}"
        self._scan_label.config(text=text, foreground=Colors.WARNING)
        self._scan_animation_id = self.after(100, self._animate_scan)

    def _update_scan_animation(self, base_text: str, extra: str = "") -> None:
        """Update scan animation text from main thread.

        Args:
            base_text: New base text.
            extra: Extra info (e.g. 'found 5 backups').
        """
        self._scan_base_text = base_text
        self._scan_extra = extra

    def _stop_scan_animation(self) -> None:
        """Stop the scan animation."""
        if self._scan_animation_id:
            self.after_cancel(self._scan_animation_id)
            self._scan_animation_id = None

    # ------------------------------------------------------------------
    # Download animation (SFTP / unknown size)
    # ------------------------------------------------------------------

    def _start_download_animation(self, base_text: str) -> None:
        """Start animated download message with cycling dots.

        Args:
            base_text: Base text, e.g. 'Downloading 1/2... MyBackup'.
        """
        self._dl_base_text = base_text
        self._dl_dot_count = 0
        self._dl_animation_id: str | None = None
        self._animate_download()

    def _animate_download(self) -> None:
        """Animate the status label with a visible braille spinner.

        Same frames and interval as the scan animation so the user sees
        a consistent "something is happening" indicator throughout. The
        previous three-dots-every-500-ms was too subtle — several users
        reported "I don't know if it's working" when an SFTP download
        pulled several thousand small files.
        """
        self._dl_dot_count = (self._dl_dot_count + 1) % len(self._SPINNER_FRAMES)
        frame = self._SPINNER_FRAMES[self._dl_dot_count]
        self.status_label.config(
            text=f"{frame} {self._dl_base_text}...",
            foreground=Colors.WARNING,
        )
        self._dl_animation_id = self.after(100, self._animate_download)

    def _stop_download_animation(self) -> None:
        """Stop the download animation."""
        anim_id = getattr(self, "_dl_animation_id", None)
        if anim_id:
            self.after_cancel(anim_id)
            self._dl_animation_id = None

    # ------------------------------------------------------------------
    # Listing results
    # ------------------------------------------------------------------

    def _on_list_done(self, backups: list[dict], grouped: bool = False) -> None:
        """Handle successful backup listing.

        Args:
            backups: List of backup dicts.
            grouped: If True, backups have '_bucket' key for grouping.
        """
        self._stop_scan_animation()
        self._list_btn.state(["!disabled"])
        self._listed_backups = backups
        self._selected_backups.clear()

        if grouped:
            bucket_count = len({b.get("_bucket", "") for b in backups})
            self._scan_label.config(
                text=(
                    f"\u2713 Scan complete \u2014 "
                    f"{len(backups)} backups in {bucket_count} bucket(s)"
                ),
                foreground=Colors.SUCCESS,
            )
        else:
            self._scan_label.config(text="", foreground=Colors.TEXT_SECONDARY)

        self._populate_tree(grouped=grouped)
        self._list_frame.pack(
            fill="x",
            padx=Spacing.LARGE,
            pady=(Spacing.MEDIUM, 0),
            after=self._source_frame,
        )
        self._update_post_source_sections()
        # The "List available backups" button sits at the bottom of
        # section 1. Without this scroll the freshly revealed selection
        # tree lands below the fold and the user thinks nothing happened.
        # Defer by 1 event loop tick so geometry managers finish laying
        # out the newly packed frame before we ask for coordinates.
        self.after_idle(lambda: self.scroll_to_widget(self._list_frame))

    def _on_list_error(self, error: str) -> None:
        """Handle listing error.

        Args:
            error: Error message.
        """
        self._stop_scan_animation()
        self._list_btn.state(["!disabled"])
        self._listed_backups.clear()
        self._selected_backups.clear()
        self._scan_label.config(text=f"\u2717 {error}", foreground=Colors.DANGER)
        self._populate_tree()
        self._list_frame.pack(
            fill="x",
            padx=Spacing.LARGE,
            pady=(Spacing.MEDIUM, 0),
            after=self._source_frame,
        )
        self._hide_post_source_sections()
        self._selection_summary.config(text="", foreground=Colors.TEXT_SECONDARY)
        # Scroll the error banner into view too — silently failing below
        # the fold would be worse than the original no-feedback problem.
        self.after_idle(lambda: self.scroll_to_widget(self._list_frame))

    def _populate_tree(self, grouped: bool = False) -> None:
        """Populate the treeview with listed backups.

        Args:
            grouped: If True, group by _bucket field.
        """
        for item in self._tree.get_children():
            self._tree.delete(item)

        sorted_backups = sorted(
            self._listed_backups,
            key=lambda b: b.get("modified", 0),
            reverse=True,
        )

        if grouped:
            # Group by bucket
            buckets: dict[str, list[dict]] = {}
            for b in sorted_backups:
                bucket = b.get("_bucket", "unknown")
                buckets.setdefault(bucket, []).append(b)

            for bucket_name, bucket_backups in buckets.items():
                bucket_id = f"_bucket_{bucket_name}"
                self._tree.insert(
                    "",
                    "end",
                    iid=bucket_id,
                    text=f"\u25b8 {bucket_name}",
                    values=("", "", "", ""),
                    open=True,
                )
                for backup in bucket_backups:
                    name = backup.get("name", "")
                    btype = _parse_backup_type(name)
                    enc = "Yes" if backup.get("encrypted") else "No"
                    size = _human_size(backup.get("size", 0))
                    date = _format_date(backup.get("modified", 0))
                    self._tree.insert(
                        bucket_id,
                        "end",
                        iid=name,
                        text=f"  {name}",
                        values=(btype, enc, size, date),
                    )
        else:
            for backup in sorted_backups:
                name = backup.get("name", "")
                btype = _parse_backup_type(name)
                enc = "Yes" if backup.get("encrypted") else "No"
                size = _human_size(backup.get("size", 0))
                date = _format_date(backup.get("modified", 0))
                self._tree.insert(
                    "",
                    "end",
                    iid=name,
                    text=f"  {name}",
                    values=(btype, enc, size, date),
                )

        self._update_selection_summary()

    # ------------------------------------------------------------------
    # Treeview selection
    # ------------------------------------------------------------------

    def _on_tree_click(self, event) -> None:
        """Toggle backup selection on click."""
        item = self._tree.identify_row(event.y)
        if not item or item.startswith("_bucket_"):
            return

        if item in self._selected_backups:
            self._selected_backups.discard(item)
            self._tree.item(item, text=f"  {item}")
        else:
            self._selected_backups.add(item)
            self._tree.item(item, text=f"\u2713 {item}")

        self._update_selection_summary()
        self._update_post_source_sections()

    def _select_all(self) -> None:
        """Select all listed backups (skip bucket headers)."""
        self._selected_backups.clear()
        for item in self._get_all_backup_items():
            self._selected_backups.add(item)
            self._tree.item(item, text=f"\u2713 {item}")
        self._update_selection_summary()
        self._update_post_source_sections()

    def _select_none(self) -> None:
        """Deselect all backups."""
        for item in self._selected_backups:
            if self._tree.exists(item):
                self._tree.item(item, text=f"  {item}")
        self._selected_backups.clear()
        self._update_selection_summary()
        self._update_post_source_sections()

    def _get_all_backup_items(self) -> list[str]:
        """Get all selectable backup item IDs (skip bucket headers).

        Returns:
            List of treeview item IDs that are actual backups.
        """
        items = []
        for item in self._tree.get_children():
            if item.startswith("_bucket_"):
                for child in self._tree.get_children(item):
                    items.append(child)
            else:
                items.append(item)
        return items

    def _update_selection_summary(self) -> None:
        """Update the selection summary label."""
        if not self._listed_backups and not self._selected_backups:
            self._selection_summary.config(
                text="No backups found", foreground=Colors.TEXT_SECONDARY
            )
            return

        count = len(self._selected_backups)
        if count == 0:
            self._selection_summary.config(
                text="No backups selected", foreground=Colors.TEXT_SECONDARY
            )
            return

        total_size = sum(
            b.get("size", 0)
            for b in self._listed_backups
            if b.get("name") in self._selected_backups
        )
        self._selection_summary.config(
            text=f"Selected: {count} backup(s) ({_human_size(total_size)})",
            foreground=Colors.TEXT,
        )

    # ------------------------------------------------------------------
    # Browse helpers
    # ------------------------------------------------------------------

    def _default_backup_initialdir(self) -> str | None:
        """Return a sensible ``initialdir`` for the backup-source Browse dialog.

        Without this, Tk falls back on the OS "last used directory" —
        after a user has done one restore, the Browse dialog for the
        NEXT restore opens at the previous destination (where the files
        were extracted) instead of where the backups actually live.

        Priority:
            1. Parent directory of whatever is already in the backup
               path field (user typed or a previous Browse succeeded).
            2. The profile's primary storage path when it is local —
               that's where backups for this profile live.
            3. ``None`` — Tk uses its default.
        """
        try:
            existing = self.backup_path_var.get().strip()
        except (tk.TclError, AttributeError):
            existing = ""
        if existing:
            p = Path(existing)
            # Always jump to the parent so the user sees sibling backups.
            # When the field already holds ``G:\Backup Manager\BackupTest_*``
            # we want the dialog to open at ``G:\Backup Manager``; opening
            # inside the selected backup folder hides the alternatives.
            parent = p.parent
            if parent.exists() and parent != p:
                return str(parent)
        if self._profile is not None:
            storage = self._profile.storage
            if storage.storage_type == StorageType.LOCAL and storage.destination_path:
                dest = Path(storage.destination_path)
                if dest.exists():
                    return str(dest)
        return None

    def _browse_folder(self) -> None:
        """Browse for a backup folder (unencrypted backup)."""
        kwargs = {"title": "Select backup folder"}
        initialdir = self._default_backup_initialdir()
        if initialdir:
            kwargs["initialdir"] = initialdir
        path = filedialog.askdirectory(**kwargs)
        if path:
            self.backup_path_var.set(path)

    def _browse_encrypted(self) -> None:
        """Browse for an encrypted .tar.wbenc backup file."""
        kwargs = {
            "title": "Select encrypted backup",
            "filetypes": [
                ("Encrypted backups", "*.wbenc"),
                ("All files", "*.*"),
            ],
        }
        initialdir = self._default_backup_initialdir()
        if initialdir:
            kwargs["initialdir"] = initialdir
        path = filedialog.askopenfilename(**kwargs)
        if path:
            self.backup_path_var.set(path)

    def _browse_dest(self) -> None:
        """Browse for a destination folder."""
        path = filedialog.askdirectory(title="Select destination")
        if path:
            self.dest_path_var.set(path)

    @staticmethod
    def _browse_key_to_var(var: tk.StringVar) -> None:
        """Browse for an SSH key file.

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
        """Update sections when backup path changes (local/network mode)."""
        stype = self.source_type_var.get()
        if stype not in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            return

        path = self._get_local_path().strip()
        if not path:
            self.password_var.set("")
            self._user_modified_pw = False
            self._update_post_source_sections()
            return

        src = Path(path)
        if not src.exists():
            self._update_post_source_sections()
            return

        has_encrypted = src.suffix == ".wbenc" or (src.is_dir() and any(src.rglob("*.wbenc")))
        if has_encrypted and self._stored_password:
            self._user_modified_pw = False
            self.password_var.set(_PASSWORD_PLACEHOLDER)
            self._user_modified_pw = False
        elif not has_encrypted:
            self.password_var.set("")
            self._user_modified_pw = False

        self._update_post_source_sections()

    def _on_password_changed(self, *_args) -> None:
        """Track manual password edits."""
        if self.password_var.get() != _PASSWORD_PLACEHOLDER:
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
    # Execute (restore)
    # ------------------------------------------------------------------

    def _execute(self) -> None:
        """Validate inputs and launch restore."""
        stype = self.source_type_var.get()
        dest_path = self.dest_path_var.get().strip()

        if not dest_path:
            messagebox.showwarning("Recovery", "Please select a destination.")
            return

        if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            self._execute_local(dest_path)
        else:
            self._execute_remote(dest_path)

    def _execute_local(self, dest_path: str) -> None:
        """Handle local/network backup restore.

        Args:
            dest_path: Destination directory path.
        """
        backup_path = self._get_local_path().strip()
        if not backup_path:
            messagebox.showwarning("Recovery", "Please select a backup.")
            return
        src = Path(backup_path)
        if not src.exists():
            messagebox.showwarning("Recovery", f"Backup does not exist:\n{backup_path}")
            return

        password = self._get_effective_password()
        has_encrypted = src.suffix == ".wbenc" or (
            src.is_dir() and any(f.suffix == ".wbenc" for f in src.rglob("*"))
        )
        if has_encrypted and not password:
            messagebox.showwarning(
                "Recovery",
                "This backup contains encrypted files but no password "
                "was provided.\nPlease enter your encryption password.",
            )
            return

        self._set_executing(True, "Restoring...")
        threading.Thread(
            target=self._do_local_restore,
            args=(src, Path(dest_path), password),
            daemon=True,
        ).start()

    def _execute_remote(self, dest_path: str) -> None:
        """Handle remote backup retrieve + restore.

        Args:
            dest_path: Destination directory path.
        """
        if not self._selected_backups:
            messagebox.showwarning("Recovery", "Please select at least one backup.")
            return

        password = self._get_effective_password()

        # Build config for each selected backup (may span multiple buckets)
        selected_with_config = []
        for name in self._selected_backups:
            backup_info = next((b for b in self._listed_backups if b.get("name") == name), None)
            if not backup_info:
                continue
            bucket = backup_info.get("_bucket")
            if bucket:
                # Backup came from S3 scan — use stored region and endpoint
                cfg = self._build_storage_config()
                cfg.s3_bucket = bucket
                cfg.s3_prefix = ""
                cfg.s3_region = backup_info.get("_region", "")
                cfg.s3_endpoint_url = backup_info.get("_endpoint", "")
            else:
                cfg = self._build_storage_config()
            selected_with_config.append((name, cfg, backup_info.get("modified", 0)))

        selected_with_config.sort(key=lambda x: x[2])
        dest = Path(dest_path)

        self._set_executing(True, "Restoring...")
        threading.Thread(
            target=self._do_remote_restore,
            args=(selected_with_config, dest, password),
            daemon=True,
        ).start()

    def _set_executing(self, running: bool, text: str = "") -> None:
        """Enable/disable the execute button and update status.

        Args:
            running: True to disable button, False to re-enable.
            text: Status text.
        """
        if running:
            self._exec_btn.state(["disabled"])
            self.status_label.config(text=text, foreground=Colors.WARNING)
        else:
            self._exec_btn.state(["!disabled"])

    # ------------------------------------------------------------------
    # Local restore (background thread)
    # ------------------------------------------------------------------

    def _do_local_restore(self, src: Path, dst: Path, password: str) -> None:
        """Restore files from a local backup to destination.

        Args:
            src: Source backup (directory or .tar.wbenc file).
            dst: Destination directory.
            password: Decryption password (empty string if not needed).
        """
        try:
            tar_wbenc = None
            if src.is_file() and src.name.endswith(".tar.wbenc"):
                tar_wbenc = src
            elif src.is_dir():
                candidate = src.with_suffix(".tar.wbenc")
                if candidate.is_file():
                    tar_wbenc = candidate

            if tar_wbenc is not None:
                count = self._decrypt_and_extract(tar_wbenc, dst, password)
                msg = f"Restore complete \u2014 {count} files decrypted"
                self.after(0, lambda: self._on_done(True, msg))
                return

            files = [f for f in src.rglob("*") if f.is_file() and not f.name.endswith(".wbverify")]
            if not files:
                self.after(0, lambda: self._on_done(False, "No files found in backup"))
                return

            copied = 0
            for f in files:
                rel = f.relative_to(src)
                target = dst / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(f, target)
                copied += 1

            msg = f"Restore complete \u2014 {copied} files copied"
            self.after(0, lambda: self._on_done(True, msg))

        except Exception as e:
            logger.exception("Restore failed")
            self.after(0, lambda _e=e: self._on_done(False, str(_e)))

    # ------------------------------------------------------------------
    # Remote restore (background thread)
    # ------------------------------------------------------------------

    def _do_remote_restore(
        self,
        selected_with_config: list[tuple[str, StorageConfig, float]],
        dest: Path,
        password: str,
    ) -> None:
        """Download and restore selected backups from remote.

        For S3 backups, shows real-time MB progress.
        For SFTP backups, shows animated dots.

        Args:
            selected_with_config: List of (name, config, modified) tuples.
            dest: Local destination directory.
            password: Decryption password.
        """
        try:
            total = len(selected_with_config)

            for idx, (name, config, _modified) in enumerate(selected_with_config, 1):
                backup_info = next((b for b in self._listed_backups if b.get("name") == name), {})
                backup_size = backup_info.get("size", 0)
                is_s3 = config.storage_type == StorageType.S3

                backend = self._create_backend(config)

                if is_s3 and backup_size > 0:
                    # S3: real-time MB progress via callback
                    def _s3_progress(bytes_sent, total_bytes, n=name, i=idx):
                        sent_mb = bytes_sent / (1024 * 1024)
                        total_mb = total_bytes / (1024 * 1024)
                        self.after(
                            0,
                            lambda s=sent_mb, t=total_mb, nn=n, ii=i: (
                                self.status_label.config(
                                    text=(
                                        f"Downloading {ii}/{total}... {nn}"
                                        f"  ({s:.1f} / {t:.1f} MB)"
                                    ),
                                    foreground=Colors.WARNING,
                                )
                            ),
                        )

                    backend.set_progress_callback(_s3_progress)
                    total_mb = backup_size / (1024 * 1024)
                    self.after(
                        0,
                        lambda n=name, i=idx, t=total_mb: self.status_label.config(
                            text=f"Downloading {i}/{total}... {n}  (0 / {t:.1f} MB)",
                            foreground=Colors.WARNING,
                        ),
                    )
                else:
                    # SFTP or unknown size: animated dots
                    self._start_download_animation(f"Downloading {idx}/{total}... {name}")

                local_path = backend.download_backup(name, dest)
                # Clear the progress callback so a lingering reference
                # from this iteration cannot fire later (boto3
                # s3transfer can dispatch a final callback after the
                # download returns) and overwrite the status label
                # while the next backup is being prepared.
                if is_s3:
                    backend.set_progress_callback(None)
                self._stop_download_animation()
                logger.info("Downloaded %s to %s", name, local_path)

                # Detect encrypted content in the downloaded backup
                tar_file = self._find_wbenc_file(local_path, name)
                if tar_file:
                    if not password:
                        msg = (
                            f"Backup '{name}' is encrypted. "
                            "Please provide your encryption password."
                        )
                        self.after(0, lambda m=msg: self._on_done(False, m))
                        return
                    self.after(
                        0,
                        lambda n=name, i=idx: self.status_label.config(
                            text=f"Decrypting {i}/{total}... {n}",
                            foreground=Colors.WARNING,
                        ),
                    )
                    self._decrypt_and_extract(tar_file, dest, password)

            msg = f"Restore complete \u2014 {total} backup(s) processed"
            self.after(0, lambda: self._on_done(True, msg))

        except Exception as e:
            self._stop_download_animation()
            logger.error("Remote restore failed: %s", e)
            self.after(0, lambda _e=e: self._on_done(False, str(_e)))

    # ------------------------------------------------------------------
    # Shared decryption
    # ------------------------------------------------------------------

    @staticmethod
    def _find_wbenc_file(local_path: Path, name: str) -> Path | None:
        """Find the .tar.wbenc file for a downloaded backup.

        Searches the downloaded path itself, sibling files in the parent
        directory, and recursively inside the directory if it is a folder.

        Args:
            local_path: Path returned by download_backup().
            name: Original backup name.

        Returns:
            Path to .tar.wbenc file, or None.
        """
        logger.debug(
            "_find_wbenc_file: local_path=%s is_file=%s is_dir=%s name=%s",
            local_path,
            local_path.is_file() if local_path.exists() else "N/A",
            local_path.is_dir() if local_path.exists() else "N/A",
            name,
        )
        if local_path.is_file() and str(local_path).endswith(".tar.wbenc"):
            return local_path
        if local_path.is_dir():
            # Check inside the directory
            internal = list(local_path.rglob("*.wbenc"))
            logger.debug(
                "_find_wbenc_file: internal rglob found %d files: %s", len(internal), internal[:5]
            )
            if internal:
                return internal[0]
            # Check sibling files (e.g. name.tar.wbenc next to name/)
            siblings = list(local_path.parent.glob(f"{name}*.tar.wbenc"))
            logger.debug(
                "_find_wbenc_file: sibling glob found %d files: %s", len(siblings), siblings[:5]
            )
            if siblings:
                return siblings[0]
        # Also check if the path doesn't exist yet but a .tar.wbenc sibling does
        if not local_path.exists() and local_path.parent.exists():
            siblings = list(local_path.parent.glob(f"{name}*.tar.wbenc"))
            if siblings:
                return siblings[0]
        logger.warning("_find_wbenc_file: no .wbenc file found for %s", name)
        return None

    @staticmethod
    def _decrypt_and_extract(tar_path: Path, dst: Path, password: str) -> int:
        """Decrypt and extract a .tar.wbenc archive.

        Args:
            tar_path: Path to the .tar.wbenc file.
            dst: Destination directory.
            password: Decryption password.

        Returns:
            Number of files extracted.

        Raises:
            RuntimeError: On wrong password.
            Exception: On other failures.
        """
        import tarfile

        backup_name = tar_path.name
        if backup_name.endswith(".tar.wbenc"):
            backup_name = backup_name[: -len(".tar.wbenc")]
        restore_dir = dst / backup_name
        long_path_mkdir(restore_dir)

        count = 0
        strip_prefix = ""

        try:
            from cryptography.exceptions import InvalidTag
        except ImportError:
            InvalidTag = None

        try:
            with open(tar_path, "rb") as f:
                reader = DecryptingReader(f, password)
                with tarfile.open(fileobj=reader, mode="r|") as tar:
                    for member in tar:
                        if member.name.endswith(".wbverify"):
                            continue
                        if not strip_prefix and "/" in member.name:
                            strip_prefix = member.name.split("/")[0] + "/"
                        name = member.name
                        if strip_prefix and name.startswith(strip_prefix):
                            name = name[len(strip_prefix) :]
                        if not name:
                            continue
                        target = restore_dir / name
                        if not _is_within_restore_dir(target, restore_dir):
                            logger.warning(
                                "Skipping archive member %r: would extract "
                                "outside the restore directory",
                                member.name,
                            )
                            continue
                        if member.isdir():
                            long_path_mkdir(target)
                            continue
                        long_path_mkdir(target.parent)
                        fileobj = tar.extractfile(member)
                        if fileobj is not None:
                            with open(long_path_str(target), "wb") as out:
                                shutil.copyfileobj(fileobj, out)
                            count += 1
                # Force HMAC trailer verification: if ``tarfile`` exited
                # early (e.g. reached its own internal EOF without
                # consuming every chunk), the DecryptingReader has NOT
                # seen the EOF sentinel and the HMAC was never checked.
                # A truncated archive would silently deliver N files
                # without any tamper alert. Raise explicitly here.
                reader.verify_complete()
        except Exception as e:
            if InvalidTag is not None and isinstance(e, InvalidTag):
                raise RuntimeError("The password you provided is incorrect") from e
            err_msg = str(e)
            if "HMAC mismatch" in err_msg:
                raise RuntimeError(
                    "Archive integrity check FAILED — truncation or tamper "
                    "detected. Already-extracted files in the restore "
                    "directory must NOT be trusted; delete them and "
                    "restore from another copy."
                ) from e
            if "tag" in err_msg.lower() or "authentication" in err_msg.lower():
                raise RuntimeError("The password you provided is incorrect") from e
            raise

        return count

    # ------------------------------------------------------------------
    # Completion handler
    # ------------------------------------------------------------------

    def _on_done(self, ok: bool, msg: str) -> None:
        """Display result.

        Args:
            ok: True if operation succeeded.
            msg: Status message.
        """
        self._set_executing(False)
        color = Colors.SUCCESS if ok else Colors.DANGER
        self.status_label.config(text=msg, foreground=color)

    # ------------------------------------------------------------------
    # Backend factory
    # ------------------------------------------------------------------

    @staticmethod
    def _create_backend(config: StorageConfig) -> StorageBackend:
        """Create a storage backend from config.

        Args:
            config: Storage configuration.

        Returns:
            Configured StorageBackend instance.
        """
        from src.core.backup_engine import create_backend

        return create_backend(config)

    # ------------------------------------------------------------------
    # Profile loading
    # ------------------------------------------------------------------

    def load_profile(self, profile: BackupProfile) -> None:
        """Load recovery settings from profile.

        Args:
            profile: The active backup profile.
        """
        # Skip full reset if reloading the same profile (e.g. after silent save).
        # This preserves user edits in the source section.
        same_profile = (
            self._profile is not None
            and hasattr(profile, "id")
            and hasattr(self._profile, "id")
            and profile.id == self._profile.id
        )
        if same_profile:
            self._profile = profile
            self._stored_password = profile.encryption.stored_password or ""
            return

        self._profile = profile
        self._stored_password = profile.encryption.stored_password or ""
        self._user_modified_pw = False
        self.password_var.set("")
        self.backup_path_var.set("")
        self.dest_path_var.set("")
        self.status_label.config(text="")
        self._scan_label.config(text="")
        self._listed_backups.clear()
        self._selected_backups.clear()
        self._stop_scan_animation()

        # Show auto-fill combo
        self._autofill_frame.pack(fill="x", pady=(0, Spacing.MEDIUM))

        # Auto-fill from main storage
        self._list_frame.pack_forget()
        self._hide_post_source_sections()
        self.source_var.set("Storage")
        config = self._get_source_storage_config()
        if config:
            self._filling = True
            try:
                self.source_type_var.set(config.storage_type.value)
                self._fill_fields(config)
            finally:
                self._filling = False

        self.scroll_to_top()

    def load_no_profile(self) -> None:
        """Reset tab for use without a profile (fresh install)."""
        self._profile = None
        self._stored_password = ""
        self._user_modified_pw = False
        self.password_var.set("")
        self.backup_path_var.set("")
        self.dest_path_var.set("")
        self.status_label.config(text="")
        self._scan_label.config(text="")
        self._listed_backups.clear()
        self._selected_backups.clear()
        self._stop_scan_animation()

        # Hide auto-fill combo
        self._autofill_frame.pack_forget()

        self._list_frame.pack_forget()
        self._hide_post_source_sections()
        self.source_type_var.set(StorageType.LOCAL.value)

    def collect_config(self) -> dict:
        """Recovery tab has no persistent config.

        Returns:
            Empty dict.
        """
        return {}
