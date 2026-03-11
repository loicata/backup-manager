"""
Mirror Tab — Mirror destinations for 3-2-1 backup rule.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from src.core.config import StorageConfig, StorageType
from src.security.encryption import store_password


class MirrorTab:
    """Mirror destinations tab: add/remove mirror storage targets."""

    def __init__(self, app, parent_frame):
        self.app = app
        self.root = app.root
        self._build(parent_frame)

    # ──────────────────────────────────────────
    #  Build UI
    # ──────────────────────────────────────────
    def _build(self, parent):
        container = ttk.Frame(parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text="Mirror Destinations \u2014 3-2-1 Rule",
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))

        # Explanation card
        info = tk.Frame(container, bg="#f0f4f8", padx=15, pady=10, relief=tk.SOLID, bd=1)
        info.pack(fill=tk.X, pady=(0, 10))
        tk.Label(info, bg="#f0f4f8", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="The 3-2-1 rule recommends keeping at least 3 copies of your data, "
                      "on 2 different media types, with 1 copy off-site.\n"
                      "Mirror destinations automatically receive a copy of each backup "
                      "after it is created on the primary destination (configured in the Storage tab).\n\n"
                      "Common 3-2-1 setups:\n"
                      "  \u2022 Primary: external drive  +  Mirror: cloud S3  (protects against fire/theft)\n"
                      "  \u2022 Primary: NAS  +  Mirror: SFTP server  (protects against local disaster)\n"
                      "  \u2022 Primary: external drive  +  Mirror 1: NAS  +  Mirror 2: Proton Drive"
                 ).pack(anchor="w")

        # Mirror treeview
        mirror_cols = ("type", "destination", "detail")
        self.mirror_tree = ttk.Treeview(
            container, columns=mirror_cols, show="headings", height=6)
        self.mirror_tree.heading("type", text="Type")
        self.mirror_tree.heading("destination", text="Destination")
        self.mirror_tree.heading("detail", text="Detail")
        self.mirror_tree.column("type", width=150)
        self.mirror_tree.column("destination", width=350)
        self.mirror_tree.column("detail", width=250)
        self.mirror_tree.pack(fill=tk.BOTH, expand=True, pady=(0, 8))

        # Buttons
        mirror_btn_frame = ttk.Frame(container)
        mirror_btn_frame.pack(fill=tk.X)
        ttk.Button(mirror_btn_frame, text="+ Add a mirror destination...",
                    command=self._add_mirror_destination).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(mirror_btn_frame, text="\u2715 Remove selected",
                    command=self._remove_mirror_destination).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(mirror_btn_frame, text="\U0001f4be Save",
                    command=self.app._save_profile, style="Accent.TButton").pack(side=tk.RIGHT)

    # ──────────────────────────────────────────
    #  Mirror Helpers
    # ──────────────────────────────────────────
    def _get_mirror_label(self, cfg) -> tuple[str, str, str]:
        """Get display strings for a mirror StorageConfig."""
        type_labels = {
            StorageType.LOCAL.value:   "\U0001f4bf External drive",
            StorageType.NETWORK.value: "\U0001f310 Network",
            StorageType.SFTP.value:    "\U0001f512 SFTP",
            StorageType.S3.value:      "\u2601 S3",
            StorageType.AZURE.value:   "\u2601 Azure",
            StorageType.GCS.value:     "\u2601 GCS",
            StorageType.PROTON.value:  "\U0001f512 Proton Drive",
        }
        stype = cfg.storage_type if hasattr(cfg, "storage_type") else cfg.get("storage_type", "")
        type_str = type_labels.get(stype, stype)

        if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            dest = cfg.destination_path if hasattr(cfg, "destination_path") else cfg.get("destination_path", "")
            return type_str, dest, ""
        elif stype == StorageType.SFTP.value:
            host = cfg.sftp_host if hasattr(cfg, "sftp_host") else cfg.get("sftp_host", "")
            rpath = cfg.sftp_remote_path if hasattr(cfg, "sftp_remote_path") else cfg.get("sftp_remote_path", "")
            return type_str, host, rpath
        elif stype == StorageType.S3.value:
            bucket = cfg.s3_bucket if hasattr(cfg, "s3_bucket") else cfg.get("s3_bucket", "")
            return type_str, bucket, ""
        elif stype == StorageType.AZURE.value:
            container = cfg.azure_container if hasattr(cfg, "azure_container") else cfg.get("azure_container", "")
            return type_str, container, ""
        elif stype == StorageType.GCS.value:
            bucket = cfg.gcs_bucket if hasattr(cfg, "gcs_bucket") else cfg.get("gcs_bucket", "")
            return type_str, bucket, ""
        elif stype == StorageType.PROTON.value:
            user = cfg.proton_username if hasattr(cfg, "proton_username") else cfg.get("proton_username", "")
            rpath = cfg.proton_remote_path if hasattr(cfg, "proton_remote_path") else cfg.get("proton_remote_path", "")
            return type_str, user, rpath
        return type_str, "", ""

    def _refresh_mirror_tree(self):
        """Refresh the mirror destinations treeview from current profile."""
        for item in self.mirror_tree.get_children():
            self.mirror_tree.delete(item)
        if not self.app.current_profile:
            return
        for cfg in self.app.current_profile.mirror_destinations:
            type_str, dest, detail = self._get_mirror_label(cfg)
            self.mirror_tree.insert("", tk.END, values=(type_str, dest, detail))

    def _add_mirror_destination(self):
        """Open a dialog to configure a new mirror destination."""
        dialog = tk.Toplevel(self.app.root)
        dialog.title("Add a mirror destination")
        dialog.geometry("550x400")
        dialog.transient(self.app.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Configure mirror destination",
                  font=("Segoe UI", 12, "bold")).pack(padx=15, pady=(10, 5), anchor="w")

        ttk.Label(dialog,
                  text="This destination will receive a copy of the backup after creation "
                       "on the primary destination.",
                  font=("Segoe UI", 9), wraplength=500).pack(padx=15, anchor="w", pady=(0, 10))

        # Type selector
        ttk.Label(dialog, text="Storage type :").pack(padx=15, anchor="w")
        mirror_type_var = tk.StringVar(value=StorageType.LOCAL.value)
        types = [
            (StorageType.LOCAL.value, "\U0001f4bf External drive / USB stick"),
            (StorageType.NETWORK.value, "\U0001f310 Network folder"),
            (StorageType.SFTP.value, "\U0001f512 SFTP"),
            (StorageType.S3.value, "\u2601 Amazon S3 / S3-compatible"),
            (StorageType.AZURE.value, "\u2601 Azure Blob"),
            (StorageType.GCS.value, "\u2601 Google Cloud Storage"),
            (StorageType.PROTON.value, "\U0001f512 Proton Drive"),
        ]
        type_combo = ttk.Combobox(
            dialog, textvariable=mirror_type_var,
            values=[t[0] for t in types], state="readonly", width=20)
        type_combo.pack(padx=15, anchor="w", pady=(3, 10))

        # Dynamic config fields
        fields_frame = ttk.LabelFrame(dialog, text="Configuration", padding=10)
        fields_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 10))

        field_vars: dict[str, tk.StringVar] = {}

        def update_fields(*args):
            for w in fields_frame.winfo_children():
                w.destroy()
            field_vars.clear()
            stype = mirror_type_var.get()

            if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
                label = "Network path:" if stype == StorageType.NETWORK.value else "Path:"
                ttk.Label(fields_frame, text=label).pack(anchor="w")
                row = ttk.Frame(fields_frame)
                row.pack(fill=tk.X)
                v = tk.StringVar()
                field_vars["destination_path"] = v
                ttk.Entry(row, textvariable=v, font=("Consolas", 9)).pack(
                    side=tk.LEFT, fill=tk.X, expand=True)
                if stype == StorageType.LOCAL.value:
                    ttk.Button(row, text="Browse...",
                                command=lambda: v.set(
                                    filedialog.askdirectory(parent=dialog) or v.get())
                                ).pack(side=tk.RIGHT, padx=(5, 0))

            elif stype == StorageType.SFTP.value:
                for label, key in [("Host :", "sftp_host"), ("Username :", "sftp_username"),
                                    ("Password :", "sftp_password"), ("Remote path :", "sftp_remote_path")]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(3, 0))
                    v = tk.StringVar(value="/backups" if key == "sftp_remote_path" else "")
                    field_vars[key] = v
                    show = "\u2022" if "password" in key else ""
                    ttk.Entry(fields_frame, textvariable=v, font=("Consolas", 9),
                              show=show).pack(fill=tk.X)

            elif stype == StorageType.S3.value:
                for label, key in [("Bucket:", "s3_bucket"), ("Prefix:", "s3_prefix"),
                                    ("Region :", "s3_region"), ("Access Key :", "s3_access_key"),
                                    ("Secret Key :", "s3_secret_key")]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar(value="eu-west-1" if key == "s3_region" else "")
                    field_vars[key] = v
                    show = "\u2022" if "secret" in key else ""
                    ttk.Entry(fields_frame, textvariable=v, font=("Consolas", 9),
                              show=show).pack(fill=tk.X)

            elif stype == StorageType.AZURE.value:
                for label, key in [("Connection String :", "azure_connection_string"),
                                    ("Container :", "azure_container")]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar()
                    field_vars[key] = v
                    show = "\u2022" if "string" in key.lower() else ""
                    ttk.Entry(fields_frame, textvariable=v, font=("Consolas", 9),
                              show=show).pack(fill=tk.X)

            elif stype == StorageType.GCS.value:
                for label, key in [("Bucket:", "gcs_bucket"), ("Credentials JSON :", "gcs_credentials_path")]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar()
                    field_vars[key] = v
                    ttk.Entry(fields_frame, textvariable=v, font=("Consolas", 9)).pack(fill=tk.X)

            elif stype == StorageType.PROTON.value:
                for label, key, show in [
                    ("Proton email:", "proton_username", ""),
                    ("Password :", "proton_password", "\u2022"),
                    ("2FA TOTP secret:", "proton_2fa", "\u2022"),
                    ("Remote folder:", "proton_remote_path", ""),
                ]:
                    ttk.Label(fields_frame, text=label).pack(anchor="w", pady=(2, 0))
                    v = tk.StringVar(value="/Backups" if key == "proton_remote_path" else "")
                    field_vars[key] = v
                    kwargs = {"font": ("Consolas", 9)}
                    if show:
                        kwargs["show"] = show
                    ttk.Entry(fields_frame, textvariable=v, **kwargs).pack(fill=tk.X)
                ttk.Label(fields_frame,
                          text="\U0001f4a1 Requires rclone installed (rclone.org)",
                          foreground="#95a5a6", font=("Segoe UI", 8)).pack(anchor="w", pady=(3, 0))

        mirror_type_var.trace_add("write", update_fields)
        update_fields()

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=15, pady=(0, 10))

        def on_add():
            cfg = StorageConfig(storage_type=mirror_type_var.get())
            for key, var in field_vars.items():
                if hasattr(cfg, key):
                    val = var.get()
                    # Encrypt passwords with DPAPI
                    if key in ("sftp_password", "proton_password") and val:
                        val = store_password(val)
                    setattr(cfg, key, val)

            if self.app.current_profile:
                self.app.current_profile.mirror_destinations.append(cfg)
                self._refresh_mirror_tree()
            dialog.destroy()

        ttk.Button(btn_frame, text="\u2705 Add", command=on_add).pack(side=tk.RIGHT)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.RIGHT, padx=(0, 5))

    def _remove_mirror_destination(self):
        """Remove the selected mirror destination."""
        selected = self.mirror_tree.selection()
        if not selected or not self.app.current_profile:
            return
        idx = self.mirror_tree.index(selected[0])
        if idx < len(self.app.current_profile.mirror_destinations):
            self.app.current_profile.mirror_destinations.pop(idx)
            self._refresh_mirror_tree()

    # ──────────────────────────────────────────
    #  Profile load / collect
    # ──────────────────────────────────────────
    def load_profile(self, p):
        """Load profile data into the tab's UI widgets."""
        self._refresh_mirror_tree()

    def collect_config(self, p):
        """Save tab's UI state into profile p.
        Mirror destinations are already modified in-place on the profile object."""
        return True
