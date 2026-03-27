"""Encryption tab: independent encryption checkboxes and password."""

import tkinter as tk
from tkinter import ttk

from src.core.config import BackupProfile, EncryptionConfig
from src.security.encryption import _has_dpapi, evaluate_password
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing


class EncryptionTab(ScrollableTab):
    """Encryption configuration with independent per-destination checkboxes.

    UI logic:
    - "No encryption" checked → the 3 encrypt checkboxes are greyed out
    - Any encrypt checkbox checked → "No encryption" is greyed out
    - All 3 unchecked → "No encryption" auto-checked, encrypt boxes greyed
    - Password frame visible only when at least one encrypt is checked
    """

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._updating = False  # Guard against recursive trace calls
        self._build_ui()

    def _build_ui(self):
        # Encryption mode
        mode_frame = ttk.LabelFrame(self.inner, text="Encryption mode", padding=Spacing.PAD)
        mode_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE)

        # No encryption checkbox
        self.no_enc_var = tk.BooleanVar(value=True)
        self._no_enc_cb = ttk.Checkbutton(
            mode_frame,
            text="No encryption",
            variable=self.no_enc_var,
            command=self._on_no_enc_toggled,
        )
        self._no_enc_cb.pack(anchor="w", pady=2)

        # Separator
        ttk.Separator(mode_frame, orient="horizontal").pack(fill="x", pady=Spacing.SMALL)

        # Encrypt Primary
        self.primary_var = tk.BooleanVar(value=False)
        self._primary_cb = ttk.Checkbutton(
            mode_frame,
            text="Encrypt Primary",
            variable=self.primary_var,
            command=self._on_encrypt_toggled,
        )
        self._primary_cb.pack(anchor="w", pady=2)

        # Encrypt Mirror 1
        self.mirror1_var = tk.BooleanVar(value=False)
        self._mirror1_cb = ttk.Checkbutton(
            mode_frame,
            text="Encrypt Mirror 1",
            variable=self.mirror1_var,
            command=self._on_encrypt_toggled,
        )
        self._mirror1_cb.pack(anchor="w", pady=2)

        # Encrypt Mirror 2
        self.mirror2_var = tk.BooleanVar(value=False)
        self._mirror2_cb = ttk.Checkbutton(
            mode_frame,
            text="Encrypt Mirror 2",
            variable=self.mirror2_var,
            command=self._on_encrypt_toggled,
        )
        self._mirror2_cb.pack(anchor="w", pady=2)

        # Encryption description
        desc_line1 = (
            "This software uses AES-256-GCM encryption with "
            "PBKDF2-HMAC-SHA256 key derivation (600,000 iterations), "
            "following OWASP 2024 recommendations."
        )
        desc_line2 = "Your backups are protected with military-grade " "authenticated encryption."
        tk.Label(
            mode_frame,
            text=desc_line1,
            fg="#cc7700",
            wraplength=900,
            justify="left",
            anchor="w",
        ).pack(fill="x", pady=(8, 0))
        tk.Label(
            mode_frame,
            text=desc_line2,
            fg="#cc7700",
            wraplength=900,
            justify="left",
            anchor="w",
        ).pack(fill="x")

        # Warning text
        warning_line1 = "But encryption introduces risk."
        warning_line2 = (
            "If you enable encryption, always perform a manual test "
            "using the Recovery tab to verify that you can restore "
            "your encrypted backups."
        )
        tk.Label(
            mode_frame,
            text=warning_line1,
            fg="#cc7700",
            anchor="w",
        ).pack(fill="x", pady=(12, 0))
        tk.Label(
            mode_frame,
            text=warning_line2,
            fg="#cc7700",
            wraplength=900,
            justify="left",
            anchor="w",
        ).pack(fill="x")

        # Password frame (shown/hidden based on encryption selection)
        self._pw_frame = ttk.LabelFrame(self.inner, text="Encryption password", padding=Spacing.PAD)

        ttk.Label(self._pw_frame, text="Password:").pack(anchor="w")
        self.password_var = tk.StringVar()
        self.password_var.trace_add("write", self._on_password_changed)

        pw_row = ttk.Frame(self._pw_frame)
        pw_row.pack(fill="x")
        self._pw_entry = ttk.Entry(pw_row, textvariable=self.password_var, show="●")
        self._pw_entry.pack(side="left", fill="x", expand=True)

        self.show_pw_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            pw_row,
            text="Show",
            variable=self.show_pw_var,
            command=self._toggle_show,
        ).pack(side="right", padx=(Spacing.SMALL, 0))

        ttk.Label(self._pw_frame, text="Confirm password:").pack(
            anchor="w", pady=(Spacing.MEDIUM, 0)
        )
        self.confirm_var = tk.StringVar()
        ttk.Entry(self._pw_frame, textvariable=self.confirm_var, show="●").pack(fill="x")

        # Strength indicator
        self.strength_label = ttk.Label(
            self._pw_frame, text="", foreground=Colors.TEXT_SECONDARY, font=Fonts.small()
        )
        self.strength_label.pack(anchor="w", pady=(Spacing.SMALL, 0))

        # DPAPI info
        dpapi_text = (
            "Password protected by Windows DPAPI"
            if _has_dpapi()
            else "DPAPI unavailable — password protected by AES-256-GCM"
        )
        ttk.Label(
            self._pw_frame, text=dpapi_text, foreground=Colors.TEXT_SECONDARY, font=Fonts.small()
        ).pack(anchor="w", pady=(Spacing.MEDIUM, 0))

        # Apply initial state
        self._update_ui_state()

    def _on_no_enc_toggled(self):
        """Handle 'No encryption' checkbox toggled by user."""
        if self._updating:
            return
        self._updating = True

        if self.no_enc_var.get():
            # No encryption selected → uncheck all encrypt boxes
            self.primary_var.set(False)
            self.mirror1_var.set(False)
            self.mirror2_var.set(False)

        self._update_ui_state()
        self._updating = False

    def _on_encrypt_toggled(self):
        """Handle any encrypt checkbox toggled by user."""
        if self._updating:
            return
        self._updating = True

        any_encrypt = self.primary_var.get() or self.mirror1_var.get() or self.mirror2_var.get()

        if any_encrypt:
            # At least one encrypt active → uncheck No encryption
            self.no_enc_var.set(False)
        else:
            # All unchecked → auto-check No encryption
            self.no_enc_var.set(True)

        self._update_ui_state()
        self._updating = False

    def _update_ui_state(self):
        """Update password frame visibility."""
        any_encrypt = self.primary_var.get() or self.mirror1_var.get() or self.mirror2_var.get()

        # Show/hide password frame
        if any_encrypt:
            self._pw_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)
        else:
            self._pw_frame.pack_forget()

    def _on_password_changed(self, *args):
        """Evaluate password strength on input."""
        pw = self.password_var.get()
        if not pw:
            self.strength_label.config(text="")
            return
        warning = evaluate_password(pw)
        if warning:
            self.strength_label.config(text=warning, foreground=Colors.WARNING)
        else:
            self.strength_label.config(text="Strong password", foreground=Colors.SUCCESS)

    def _toggle_show(self):
        """Toggle password visibility."""
        show = "" if self.show_pw_var.get() else "●"
        self._pw_entry.config(show=show)

    def load_profile(self, profile: BackupProfile):
        """Load encryption settings from profile."""
        self._updating = True

        self.primary_var.set(profile.encrypt_primary)
        self.mirror1_var.set(profile.encrypt_mirror1)
        self.mirror2_var.set(profile.encrypt_mirror2)

        any_encrypt = profile.encrypt_primary or profile.encrypt_mirror1 or profile.encrypt_mirror2
        self.no_enc_var.set(not any_encrypt)

        if profile.encryption.stored_password:
            self.password_var.set(profile.encryption.stored_password)
            self.confirm_var.set(profile.encryption.stored_password)
        else:
            self.password_var.set("")
            self.confirm_var.set("")

        self._update_ui_state()
        self._updating = False

    def collect_config(self) -> dict:
        """Collect encryption configuration.

        Returns:
            Dict with encrypt_primary, encrypt_mirror1, encrypt_mirror2,
            and encryption (EncryptionConfig).
        """
        enc_primary = self.primary_var.get()
        enc_mirror1 = self.mirror1_var.get()
        enc_mirror2 = self.mirror2_var.get()
        pw = self.password_var.get()
        any_encrypt = enc_primary or enc_mirror1 or enc_mirror2
        enabled = any_encrypt and bool(pw)

        return {
            "encrypt_primary": enc_primary,
            "encrypt_mirror1": enc_mirror1,
            "encrypt_mirror2": enc_mirror2,
            "encryption": EncryptionConfig(
                enabled=enabled,
                stored_password=pw if enabled else "",
            ),
        }

    def validate(self) -> str | None:
        """Validate encryption config.

        Returns:
            Error message string, or None if valid.
        """
        any_encrypt = self.primary_var.get() or self.mirror1_var.get() or self.mirror2_var.get()
        if not any_encrypt:
            return None
        pw = self.password_var.get()
        confirm = self.confirm_var.get()
        if not pw:
            return "Encryption password is required"
        if pw != confirm:
            return "Passwords do not match"
        return None
