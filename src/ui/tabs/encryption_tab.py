"""
Encryption Tab — Encryption mode selection and password management.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from src.core.config import StorageType
from src.security.encryption import EncryptionConfig, EncryptionAlgorithm, evaluate_password, get_crypto_engine
from src.installer import FEAT_ENCRYPTION


class EncryptionTab:
    """Encryption tab: mode selection (none / mirrors only / all), password entry."""

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

        ttk.Label(container, text='Backup encryption',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))

        # Crypto availability check
        crypto = get_crypto_engine()
        if not crypto.is_available:
            warning_frame = tk.Frame(container, bg="#fff3cd", padx=15, pady=10)
            warning_frame.pack(fill=tk.X, pady=(0, 10))
            tk.Label(warning_frame, text="\u26a0 'cryptography' library not installed.",
                     bg="#fff3cd", fg="#856404", font=("Segoe UI", 10, "bold")).pack(anchor="w")
            tk.Label(warning_frame,
                     text="Install it with: pip install cryptography",
                     bg="#fff3cd", fg="#856404", font=("Consolas", 9)).pack(anchor="w", pady=(3, 0))

        # Encryption mode — 3 radio buttons
        self.var_encryption_mode = tk.StringVar(value="none")
        self.var_enc_algo = tk.StringVar(value=EncryptionAlgorithm.AES_256_GCM.value)
        self.var_enc_env_var = tk.StringVar()  # Hidden, kept for compatibility
        # Hidden var for backward compat with encryption.enabled
        self.var_encrypt_enabled = tk.BooleanVar(value=False)

        # Option 1: No encryption
        card1 = tk.Frame(container, bg="white", padx=15, pady=10, relief=tk.SOLID, bd=1)
        card1.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card1, text="\U0001f513 No encryption",
                         variable=self.var_encryption_mode, value="none",
                         command=lambda: self._toggle_enc_pwd_frame(False)
                         ).pack(anchor="w")
        tk.Label(card1, bg="white", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="All backups and mirrors are stored in plain text. Fastest option.\n"
                      "\u2705 Recommended if: local drive at home, non-sensitive data"
                 ).pack(anchor="w", padx=(20, 0))

        # Option 2: Mirrors only
        card2 = tk.Frame(container, bg="white", padx=15, pady=10, relief=tk.SOLID, bd=1)
        card2.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card2, text="\U0001f510 Encrypt mirrors only",
                         variable=self.var_encryption_mode, value="mirrors_only",
                         command=lambda: self._toggle_enc_pwd_frame(True)
                         ).pack(anchor="w")
        tk.Label(card2, bg="white", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="The primary backup stays plain (fast local restore), but all mirror copies "
                      "are encrypted before upload.\n"
                      "\u2705 Recommended if: primary on local drive + mirrors on cloud or off-site\n"
                      "Best of both worlds: fast local access + secure remote copies"
                 ).pack(anchor="w", padx=(20, 0))

        # Option 3: Encrypt everything
        card3 = tk.Frame(container, bg="white", padx=15, pady=10, relief=tk.SOLID, bd=1)
        card3.pack(fill=tk.X, pady=3)
        ttk.Radiobutton(card3, text="\U0001f512 Encrypt everything",
                         variable=self.var_encryption_mode, value="all",
                         command=lambda: self._toggle_enc_pwd_frame(True)
                         ).pack(anchor="w")
        tk.Label(card3, bg="white", fg="#2c3e50",
                 font=("Segoe UI", 9), wraplength=1100, justify=tk.LEFT,
                 text="All backups are encrypted \u2014 primary destination AND all mirrors.\n"
                      "\u2705 Recommended if: confidential data, GDPR compliance, shared drives\n"
                      "\u26a0 WARNING: losing the password = PERMANENT data loss"
                 ).pack(anchor="w", padx=(20, 0))

        # Password entry frame
        self._encrypt_settings_frame = ttk.LabelFrame(
            container, text="Encryption password (16 characters min.)", padding=10)
        self._encrypt_settings_frame.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(self._encrypt_settings_frame, text="Password:").pack(anchor="w", pady=(0, 2))
        self.var_enc_password = tk.StringVar()
        self.entry_enc_password = ttk.Entry(
            self._encrypt_settings_frame, textvariable=self.var_enc_password,
            show="\u2022", font=("Consolas", 11))
        self.entry_enc_password.pack(fill=tk.X, pady=(0, 3))

        # Character counter
        self.lbl_password_strength = ttk.Label(
            self._encrypt_settings_frame, text="0 / 16 characters", font=("Segoe UI", 8))
        self.lbl_password_strength.pack(anchor="w")
        self.var_enc_password.trace_add("write", self._update_password_strength)

        ttk.Label(self._encrypt_settings_frame, text="Confirmation:").pack(anchor="w", pady=(5, 2))
        self.var_enc_password_confirm = tk.StringVar()
        ttk.Entry(
            self._encrypt_settings_frame, textvariable=self.var_enc_password_confirm,
            show="\u2022", font=("Consolas", 11)).pack(fill=tk.X)

        # Initially hidden
        self._toggle_enc_pwd_frame(False)

        # Save button
        self._enc_save_btn = ttk.Button(container, text='\U0001f4be Save',
                    command=self.app._save_profile, style="Accent.TButton")
        self._enc_save_btn.pack(anchor="e", pady=(10, 0))

    # ──────────────────────────────────────────
    #  Helpers
    # ──────────────────────────────────────────
    def _toggle_enc_pwd_frame(self, show: bool):
        """Show or hide the password entry frame."""
        if show:
            self._encrypt_settings_frame.pack(fill=tk.X, pady=(8, 0))
            # Re-pack save button at the end
            if hasattr(self, '_enc_save_btn'):
                self._enc_save_btn.pack_forget()
                self._enc_save_btn.pack(anchor="e", pady=(10, 0))
        else:
            self._encrypt_settings_frame.pack_forget()

    def _update_password_strength(self, *args):
        """Update the password character counter."""
        password = self.var_enc_password.get()
        n = len(password)
        feedback = evaluate_password(password)
        color = "#e74c3c" if n < 16 else "#27ae60"
        text = f"{n} / 16 characters"
        if feedback:
            text += f" \u2014 {feedback}"
        self.lbl_password_strength.configure(text=text, foreground=color)

    # ──────────────────────────────────────────
    #  Profile load / collect
    # ──────────────────────────────────────────
    def load_profile(self, p):
        """Load profile data into the tab's UI widgets."""
        enc = p.encryption
        self.var_encryption_mode.set(p.encryption_mode)
        self.var_encrypt_enabled.set(enc.enabled)  # backward compat
        self.var_enc_algo.set(enc.algorithm)
        self.var_enc_env_var.set(enc.key_env_variable)
        self.var_enc_password.set("")  # Never load password (not stored)
        self.var_enc_password_confirm.set("")
        self._toggle_enc_pwd_frame(p.encryption_mode != "none")

    def collect_config(self, p):
        """Save tab's UI state into profile p."""
        from src.security.encryption import store_password

        stored_pwd = p.encryption.stored_password_b64 if p.encryption else ""
        tab_pwd = self.var_enc_password.get()
        tab_confirm = self.var_enc_password_confirm.get()
        if tab_pwd and tab_pwd == tab_confirm and len(tab_pwd) >= 16:
            stored_pwd = store_password(tab_pwd)
        enc_mode = self.var_encryption_mode.get()
        p.encryption_mode = enc_mode
        p.encryption = EncryptionConfig(
            enabled=(enc_mode == "all"),  # Only "all" encrypts the primary backup
            algorithm=self.var_enc_algo.get(),
            key_env_variable=self.var_enc_env_var.get(),
            stored_password_b64=stored_pwd,
        )
        return True
