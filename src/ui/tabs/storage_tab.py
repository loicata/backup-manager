"""
Storage Tab — Storage destination configuration (local, network, S3, Azure, SFTP, GCS, Proton).
"""

import threading

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from src.core.config import StorageConfig, StorageType
from src.storage import get_storage_backend
from src.installer import FEAT_S3, FEAT_AZURE, FEAT_GCS, FEAT_SFTP


class StorageTab:
    """Storage destination tab: type selection, credentials, bandwidth limit."""

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

        ttk.Label(container, text='Storage destination',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 10))

        # Storage type selector — scrollable frame for many options
        type_frame = ttk.LabelFrame(container, text="Storage type", padding=10)
        type_frame.pack(fill=tk.X, pady=(0, 10))

        self.var_storage_type = tk.StringVar(value=StorageType.LOCAL.value)

        # All storage options in a single column
        all_options = [
            (StorageType.LOCAL.value,   "\U0001f4bf External drive / USB stick",  None),
            (StorageType.NETWORK.value, "\U0001f310 Network folder (UNC)",        None),
            (StorageType.SFTP.value,    "\U0001f512 SFTP (SSH)",                  FEAT_SFTP),
            (StorageType.S3.value,      "\u2601 Amazon S3 / S3-compatible",       FEAT_S3),
            (StorageType.AZURE.value,   "\u2601 Azure Blob Storage",              FEAT_AZURE),
            (StorageType.GCS.value,     "\u2601 Google Cloud Storage",            FEAT_GCS),
            (StorageType.PROTON.value,  "\U0001f512 Proton Drive",                None),
        ]

        self._storage_radio_buttons = {}

        for val, label, feat_id in all_options:
            rb = ttk.Radiobutton(
                type_frame, text=label, variable=self.var_storage_type,
                value=val, command=self._update_storage_fields,
            )
            rb.pack(anchor="w", pady=1)
            self._storage_radio_buttons[val] = (rb, feat_id)

        # Dynamic fields container
        self.storage_fields_frame = ttk.LabelFrame(container, text="Configuration", padding=10)
        self.storage_fields_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # All field variables — existing
        self.var_dest_path = tk.StringVar()
        self.var_s3_bucket = tk.StringVar()
        self.var_s3_prefix = tk.StringVar()
        self.var_s3_region = tk.StringVar(value="eu-west-1")
        self.var_s3_access_key = tk.StringVar()
        self.var_s3_secret_key = tk.StringVar()
        self.var_s3_endpoint = tk.StringVar()
        self.var_s3_provider = tk.StringVar(value="aws")
        self.var_azure_conn = tk.StringVar()
        self.var_azure_container = tk.StringVar()
        self.var_azure_prefix = tk.StringVar()
        # SFTP / FTP
        self.var_sftp_host = tk.StringVar()
        self.var_sftp_port = tk.IntVar(value=22)
        self.var_sftp_username = tk.StringVar()
        self.var_sftp_password = tk.StringVar()
        self.var_sftp_key_path = tk.StringVar()
        self.var_sftp_remote_path = tk.StringVar(value="/backups")
        # GCS
        self.var_gcs_bucket = tk.StringVar()
        self.var_gcs_prefix = tk.StringVar()
        self.var_gcs_credentials = tk.StringVar()
        # Proton Drive
        self.var_proton_username = tk.StringVar()
        self.var_proton_password = tk.StringVar()
        self.var_proton_2fa = tk.StringVar()
        self.var_proton_remote_path = tk.StringVar(value="/Backups")
        self.var_proton_rclone_path = tk.StringVar()

        self._update_storage_fields()

        # ── Bandwidth Limit ──
        bw_frame = ttk.LabelFrame(container, text="Bandwidth limit", padding=10)
        bw_frame.pack(fill=tk.X, pady=(10, 5))

        bw_row = ttk.Frame(bw_frame)
        bw_row.pack(fill=tk.X)
        self.var_bandwidth_limit = tk.IntVar(value=0)
        ttk.Label(bw_row, text="Max upload speed:").pack(side=tk.LEFT)
        ttk.Spinbox(bw_row, from_=0, to=1000000, width=8,
                      textvariable=self.var_bandwidth_limit).pack(side=tk.LEFT, padx=(5, 5))
        ttk.Label(bw_row, text="KB/s   (0 = unlimited)",
                  font=("Segoe UI", 9)).pack(side=tk.LEFT, padx=(0, 15))

        # Buttons
        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X)
        self.btn_test = ttk.Button(btn_frame, text='\U0001f50c Test connection',
                                    command=self._test_storage)
        self.btn_test.pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_frame, text='\U0001f4be Save',
                    command=self.app._save_profile, style="Accent.TButton").pack(side=tk.RIGHT)

    # ──────────────────────────────────────────
    #  Helpers
    # ──────────────────────────────────────────
    def _add_field(self, frame, label: str, var, show: str = "", browse: str = ""):
        """Helper to add a labeled entry field to a frame."""
        ttk.Label(frame, text=label).pack(anchor="w", pady=(5, 0))
        entry_frame = ttk.Frame(frame)
        entry_frame.pack(fill=tk.X, pady=(2, 0))
        kwargs = {"font": ("Consolas", 10)}
        if show:
            kwargs["show"] = show
        ttk.Entry(entry_frame, textvariable=var, **kwargs).pack(
            side=tk.LEFT, fill=tk.X, expand=True)
        if browse:
            ttk.Button(entry_frame, text="Browse...",
                        command=lambda: self._browse_for_var(var, browse)
                        ).pack(side=tk.RIGHT, padx=(5, 0))

    def _browse_for_var(self, var: tk.StringVar, mode: str):
        """Browse for a file or directory and set the variable."""
        if mode == "dir":
            path = filedialog.askdirectory(title="Choose a folder")
        else:
            path = filedialog.askopenfilename(
                title="Choose a file",
                filetypes=[(mode, mode), ("All files", "*.*")],
            )
        if path:
            var.set(path)

    def _update_storage_fields(self):
        """Update visible storage fields based on selected type."""
        for widget in self.storage_fields_frame.winfo_children():
            widget.destroy()

        stype = self.var_storage_type.get()
        frame = self.storage_fields_frame

        if stype in (StorageType.LOCAL.value, StorageType.NETWORK.value):
            label = "Network path (e.g. \\\\server\\share)" if stype == StorageType.NETWORK.value else "Destination path"
            ttk.Label(frame, text=label).pack(anchor="w")
            path_frame = ttk.Frame(frame)
            path_frame.pack(fill=tk.X, pady=(3, 0))
            ttk.Entry(path_frame, textvariable=self.var_dest_path,
                      font=("Consolas", 10)).pack(side=tk.LEFT, fill=tk.X, expand=True)
            if stype == StorageType.LOCAL.value:
                ttk.Button(path_frame, text="Browse...",
                           command=self._browse_dest).pack(side=tk.RIGHT, padx=(5, 0))

        elif stype == StorageType.S3.value:
            # S3 provider selector
            ttk.Label(frame, text="S3 Provider").pack(anchor="w", pady=(5, 0))
            provider_frame = ttk.Frame(frame)
            provider_frame.pack(fill=tk.X, pady=(2, 0))

            s3_providers = [
                ("aws",          "Amazon AWS S3"),
                ("minio",        "MinIO (self-hosted)"),
                ("wasabi",       "Wasabi"),
                ("ovh",          "OVH Object Storage"),
                ("scaleway",     "Scaleway Object Storage"),
                ("digitalocean", "DigitalOcean Spaces"),
                ("cloudflare",   "Cloudflare R2"),
                ("backblaze_s3", "Backblaze B2 (mode S3)"),
                ("other",        "Other (custom endpoint)"),
            ]
            provider_combo = ttk.Combobox(
                provider_frame, textvariable=self.var_s3_provider,
                values=[p[0] for p in s3_providers],
                state="readonly", width=20,
            )
            provider_combo.pack(side=tk.LEFT)

            # Display label for selected provider
            provider_labels = {p[0]: p[1] for p in s3_providers}
            lbl_provider_name = ttk.Label(
                provider_frame,
                text=f"  \u2014 {provider_labels.get(self.var_s3_provider.get(), '')}",
                style="SubHeader.TLabel",
            )
            lbl_provider_name.pack(side=tk.LEFT, padx=5)

            def on_provider_change(*args):
                prov = self.var_s3_provider.get()
                lbl_provider_name.configure(
                    text=f"  \u2014 {provider_labels.get(prov, '')}"
                )
                # Auto-fill endpoint template
                from src.storage.s3 import S3Storage
                template = S3Storage.PROVIDER_ENDPOINTS.get(prov, "")
                if template and prov != "aws":
                    region = self.var_s3_region.get() or "us-east-1"
                    self.var_s3_endpoint.set(
                        template.format(region=region, account_id="")
                    )
                elif prov == "aws":
                    self.var_s3_endpoint.set("")

            self.var_s3_provider.trace_add("write", on_provider_change)

            # Standard S3 fields
            for label, var in [
                ("Bucket", self.var_s3_bucket),
                ("Prefix (subfolder)", self.var_s3_prefix),
                ("Region", self.var_s3_region),
                ("Access Key ID", self.var_s3_access_key),
            ]:
                self._add_field(frame, label, var)
            self._add_field(frame, "Secret Access Key", self.var_s3_secret_key, show="\u2022")

            # Custom endpoint URL
            self._add_field(frame, "Endpoint URL (empty = AWS default)", self.var_s3_endpoint)
            ttk.Label(frame,
                      text="\U0001f4a1 Ex: https://s3.eu-west-1.wasabisys.com, http://minio:9000, ...",
                      style="SubHeader.TLabel").pack(anchor="w", pady=(2, 0))

        elif stype == StorageType.AZURE.value:
            self._add_field(frame, "Connection String", self.var_azure_conn, show="\u2022")
            self._add_field(frame, "Container", self.var_azure_container)
            self._add_field(frame, "Prefix (subfolder)", self.var_azure_prefix)

        elif stype == StorageType.SFTP.value:
            self.var_sftp_port.set(22)
            self._add_field(frame, "Host SFTP", self.var_sftp_host)

            ttk.Label(frame, text="Port").pack(anchor="w", pady=(5, 0))
            ttk.Spinbox(frame, from_=1, to=65535, textvariable=self.var_sftp_port,
                         width=8).pack(anchor="w", pady=(2, 0))

            self._add_field(frame, "Username", self.var_sftp_username)
            self._add_field(frame, "Password (leave empty if using SSH key)",
                            self.var_sftp_password, show="\u2022")
            self._add_field(frame, "SSH private key (optional \u2014 replaces password)",
                            self.var_sftp_key_path, browse="*.pem *.key *.ppk *.id_rsa")
            tk.Label(frame, text="Supports RSA, Ed25519, ECDSA keys (.pem, .key, .ppk, id_rsa).",
                     font=("Segoe UI", 8), fg="#95a5a6").pack(anchor="w", pady=(0, 5))
            self._add_field(frame, "Remote path", self.var_sftp_remote_path)

        elif stype == StorageType.GCS.value:
            self._add_field(frame, "GCS Bucket", self.var_gcs_bucket)
            self._add_field(frame, "Prefix (subfolder)", self.var_gcs_prefix)
            self._add_field(frame, "Credentials JSON file (service account)",
                            self.var_gcs_credentials, browse="*.json")
            ttk.Label(frame,
                      text="\U0001f4a1 If empty, uses GOOGLE_APPLICATION_CREDENTIALS or gcloud CLI.",
                      style="SubHeader.TLabel").pack(anchor="w", pady=(3, 0))

        elif stype == StorageType.PROTON.value:
            # Step-by-step setup guide (scrollable, compact)
            tk.Label(frame, text="\U0001f4cb Proton Drive Setup Guide",
                     fg="#2c3e50", font=("Segoe UI", 10, "bold")).pack(anchor="w")

            guide_frame = tk.Frame(frame)
            guide_frame.pack(fill=tk.X, pady=(2, 8))

            guide_text = tk.Text(guide_frame, wrap=tk.WORD, font=("Segoe UI", 8),
                                  bg="#eaf2f8", fg="#2c3e50", relief=tk.SOLID, bd=1,
                                  height=7, padx=10, pady=6, cursor="arrow")
            guide_scroll = ttk.Scrollbar(guide_frame, orient=tk.VERTICAL,
                                          command=guide_text.yview)
            guide_text.configure(yscrollcommand=guide_scroll.set)
            guide_text.pack(side=tk.LEFT, fill=tk.X, expand=True)
            guide_scroll.pack(side=tk.RIGHT, fill=tk.Y)

            guide_content = (
                "Proton Drive uses rclone (a free open-source tool) to transfer your backups.\n"
                "Your files are end-to-end encrypted with the same keys as the official Proton apps.\n\n"
                "Step 1 \u2014 Install rclone\n"
                "   Download from https://rclone.org/install/\n"
                "   On Windows: download the .zip, extract it, and place rclone.exe\n"
                "   somewhere in your PATH (e.g. C:\\Windows or C:\\rclone).\n"
                "   To verify: open a terminal and type: rclone version\n\n"
                "Step 2 \u2014 Log in to Proton web at least once\n"
                "   Go to https://mail.proton.me and log in with your account.\n"
                "   This generates the encryption keys needed by rclone.\n"
                "   Without this step, rclone cannot access your Drive.\n\n"
                "Step 3 \u2014 Fill in the fields below\n"
                "   \u2022 Proton email: your full Proton email (e.g. user@proton.me)\n"
                "   \u2022 Password: your Proton account password\n"
                "   \u2022 2FA / TOTP secret: ONLY if you have 2-factor authentication\n"
                "     enabled on your Proton account. This is NOT the 6-digit code\n"
                "     that changes every 30 seconds in your authenticator app.\n"
                "     It is the long base32 string (e.g. JBSWY3DPEHPK3PXP) that\n"
                "     was shown ONCE when you first enabled 2FA.\n\n"
                "     \u26a0 If you don't have this secret anymore:\n"
                "     1. Log in to https://account.proton.me/u/0/mail/security\n"
                "     2. Disable 2-factor authentication\n"
                "     3. Re-enable it \u2014 this time SAVE the secret key that appears\n"
                "        (the text string, not the QR code)\n"
                "     4. Scan the QR code in your authenticator app as usual\n"
                "     5. Paste the saved secret key here\n\n"
                "   \u2022 Remote folder: the folder in Proton Drive where backups will go\n"
                "     (e.g. /Backups). It will be created automatically if it doesn't exist.\n\n"
                "Step 4 \u2014 Click 'Test connection' below to verify everything works.\n\n"
                "\U0001f512 Security: Your Proton password is stored securely on this computer\n"
                "   using Windows DPAPI encryption. It is never transmitted in plain text."
            )
            guide_text.insert("1.0", guide_content)
            guide_text.configure(state=tk.DISABLED)  # Read-only

            self._add_field(frame, "Proton email", self.var_proton_username)
            self._add_field(frame, "Password Proton", self.var_proton_password, show="\u2022")
            self._add_field(frame, "2FA TOTP secret (only if 2FA enabled \u2014 see guide above)",
                            self.var_proton_2fa, show="\u2022")
            self._add_field(frame, "Remote folder in Proton Drive", self.var_proton_remote_path)
            self._add_field(frame, "Path to rclone (empty = auto-detect)",
                            self.var_proton_rclone_path, browse="*.exe")

    def _browse_dest(self):
        path = filedialog.askdirectory(title="Choose destination folder")
        if path:
            self.var_dest_path.set(path)

    def _test_storage(self):
        """Test the storage connection in a background thread (non-blocking)."""
        config = self._build_storage_config()

        # Disable button and show testing state
        self.btn_test.configure(state=tk.DISABLED, text="\u23f3 Testing...")

        def _do_test():
            try:
                backend = get_storage_backend(config)
                success, message = backend.test_connection()
                if success:
                    self.root.after(0, lambda: messagebox.showinfo("Connection test", message))
                else:
                    self.root.after(0, lambda: messagebox.showwarning("Connection test", message))
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", str(e)))
            finally:
                self.root.after(0, lambda: self.btn_test.configure(
                    state=tk.NORMAL, text="\U0001f50c Test connection"))

        threading.Thread(target=_do_test, daemon=True).start()

    def _build_storage_config(self) -> StorageConfig:
        """Build StorageConfig from current UI fields."""
        return StorageConfig(
            storage_type=self.var_storage_type.get(),
            destination_path=self.var_dest_path.get(),
            # S3
            s3_bucket=self.var_s3_bucket.get(),
            s3_prefix=self.var_s3_prefix.get(),
            s3_region=self.var_s3_region.get(),
            s3_access_key=self.var_s3_access_key.get(),
            s3_secret_key=self.var_s3_secret_key.get(),
            s3_endpoint_url=self.var_s3_endpoint.get(),
            s3_provider=self.var_s3_provider.get(),
            # Azure
            azure_connection_string=self.var_azure_conn.get(),
            azure_container=self.var_azure_container.get(),
            azure_prefix=self.var_azure_prefix.get(),
            # SFTP / FTP
            sftp_host=self.var_sftp_host.get(),
            sftp_port=self.var_sftp_port.get(),
            sftp_username=self.var_sftp_username.get(),
            sftp_password=self.var_sftp_password.get(),
            sftp_key_path=self.var_sftp_key_path.get(),
            sftp_remote_path=self.var_sftp_remote_path.get(),
            # GCS
            gcs_bucket=self.var_gcs_bucket.get(),
            gcs_prefix=self.var_gcs_prefix.get(),
            gcs_credentials_path=self.var_gcs_credentials.get(),
            # Proton Drive
            proton_username=self.var_proton_username.get(),
            proton_password=self.var_proton_password.get(),
            proton_2fa=self.var_proton_2fa.get(),
            proton_remote_path=self.var_proton_remote_path.get(),
            proton_rclone_path=self.var_proton_rclone_path.get(),
        )

    # ──────────────────────────────────────────
    #  Profile load / collect
    # ──────────────────────────────────────────
    def load_profile(self, p):
        """Load profile data into the tab's UI widgets."""
        s = p.storage
        self.var_storage_type.set(s.storage_type)
        self.var_dest_path.set(s.destination_path)
        self.var_s3_bucket.set(s.s3_bucket)
        self.var_s3_prefix.set(s.s3_prefix)
        self.var_s3_region.set(s.s3_region)
        self.var_s3_access_key.set(s.s3_access_key)
        self.var_s3_secret_key.set(s.s3_secret_key)
        self.var_s3_endpoint.set(s.s3_endpoint_url)
        self.var_s3_provider.set(s.s3_provider)
        self.var_azure_conn.set(s.azure_connection_string)
        self.var_azure_container.set(s.azure_container)
        self.var_azure_prefix.set(s.azure_prefix)
        self.var_sftp_host.set(s.sftp_host)
        self.var_sftp_port.set(s.sftp_port)
        self.var_sftp_username.set(s.sftp_username)
        self.var_sftp_password.set(s.sftp_password)
        self.var_sftp_key_path.set(s.sftp_key_path)
        self.var_sftp_remote_path.set(s.sftp_remote_path)
        self.var_gcs_bucket.set(s.gcs_bucket)
        self.var_gcs_prefix.set(s.gcs_prefix)
        self.var_gcs_credentials.set(s.gcs_credentials_path)
        self.var_proton_username.set(s.proton_username)
        self.var_proton_password.set(s.proton_password)
        self.var_proton_2fa.set(s.proton_2fa)
        self.var_proton_remote_path.set(s.proton_remote_path)
        self.var_proton_rclone_path.set(s.proton_rclone_path)
        self._update_storage_fields()
        self.var_bandwidth_limit.set(p.bandwidth_limit_kbps)

    def collect_config(self, p):
        """Save tab's UI state into profile p."""
        p.storage = self._build_storage_config()
        p.bandwidth_limit_kbps = self.var_bandwidth_limit.get()
        return True
