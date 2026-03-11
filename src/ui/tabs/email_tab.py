"""
Email Tab
=========
Email notification settings: trigger mode, SMTP configuration,
test email, and DPAPI warning.
"""

import threading
import tkinter as tk
from tkinter import ttk, messagebox

from src.notifications.email_notifier import EmailConfig, send_backup_report, send_test_email
from src.security.encryption import store_password, retrieve_password

COLORS = {
    "bg":        "#f5f6fa",
    "sidebar":   "#2c3e50",
    "sidebar_fg": "#ecf0f1",
    "accent":    "#3498db",
    "success":   "#27ae60",
    "warning":   "#f39c12",
    "danger":    "#e74c3c",
    "text":      "#2c3e50",
    "muted":     "#95a5a6",
}


class EmailTab:
    """Email notifications tab: trigger, SMTP, test, DPAPI warning."""

    def __init__(self, app, parent_frame):
        self.app = app
        self.parent = parent_frame
        self._build()

    # ── Build ──────────────────────────────────
    def _build(self):
        container = ttk.Frame(self.parent)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        ttk.Label(container, text='Email notifications',
                  style="Header.TLabel").pack(anchor="w", pady=(0, 5))
        ttk.Label(container,
                  text="Receive email reports after scheduled backups \u2014 on success, failure, or both.",
                  style="SubHeader.TLabel").pack(anchor="w", pady=(0, 15))

        # ── When to send ──
        trigger_frame = ttk.LabelFrame(container, text="When to send emails", padding=15)
        trigger_frame.pack(fill=tk.X, pady=(0, 10))

        self.var_email_trigger = tk.StringVar(value="disabled")
        for value, label in [
            ("disabled",    "\U0001f515 Disabled \u2014 no email notifications"),
            ("failure",     "\u274c On failure only \u2014 email when a backup fails"),
            ("success",     "\u2705 On success only \u2014 email when a backup succeeds"),
            ("always",      "\U0001f4e7 Always \u2014 email after every backup (success or failure)"),
        ]:
            ttk.Radiobutton(trigger_frame, text=label, value=value,
                             variable=self.var_email_trigger).pack(anchor="w", pady=2)

        # Hidden vars mapped from radio selection (for EmailConfig compatibility)
        self.var_email_enabled = tk.BooleanVar(value=False)
        self.var_email_on_success = tk.BooleanVar(value=True)
        self.var_email_on_failure = tk.BooleanVar(value=True)

        # ── SMTP Configuration ──
        smtp_frame = ttk.LabelFrame(container, text="SMTP Server", padding=15)
        smtp_frame.pack(fill=tk.X, pady=(0, 10))

        # Row 1: Host + Port + TLS
        row1 = ttk.Frame(smtp_frame)
        row1.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row1, text="SMTP Host:").pack(side=tk.LEFT)
        self.var_smtp_host = tk.StringVar()
        ttk.Entry(row1, textvariable=self.var_smtp_host, width=30,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 15))
        ttk.Label(row1, text="Port:").pack(side=tk.LEFT)
        self.var_smtp_port = tk.IntVar(value=587)
        ttk.Spinbox(row1, from_=1, to=65535, width=6,
                      textvariable=self.var_smtp_port).pack(side=tk.LEFT, padx=(5, 15))
        self.var_smtp_tls = tk.BooleanVar(value=True)
        ttk.Checkbutton(row1, text="Use TLS", variable=self.var_smtp_tls).pack(side=tk.LEFT)

        # Row 2: Username + Password
        row2 = ttk.Frame(smtp_frame)
        row2.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row2, text="Username:").pack(side=tk.LEFT)
        self.var_smtp_user = tk.StringVar()
        ttk.Entry(row2, textvariable=self.var_smtp_user, width=25,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 15))
        ttk.Label(row2, text="Password:").pack(side=tk.LEFT)
        self.var_smtp_password = tk.StringVar()
        ttk.Entry(row2, textvariable=self.var_smtp_password, show="\u2022", width=25,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 0))

        # Row 3: From + To
        row3 = ttk.Frame(smtp_frame)
        row3.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(row3, text="From:").pack(side=tk.LEFT)
        self.var_email_from = tk.StringVar()
        ttk.Entry(row3, textvariable=self.var_email_from, width=25,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 15))
        ttk.Label(row3, text="To:").pack(side=tk.LEFT)
        self.var_email_to = tk.StringVar()
        ttk.Entry(row3, textvariable=self.var_email_to, width=30,
                  font=("Consolas", 10)).pack(side=tk.LEFT, padx=(5, 0))

        ttk.Label(smtp_frame,
                  text="\U0001f4a1 Common SMTP: Gmail \u2192 smtp.gmail.com:587 | Outlook \u2192 smtp.office365.com:587 | "
                       "ProtonMail Bridge \u2192 127.0.0.1:1025",
                  font=("Segoe UI", 8), foreground="#95a5a6"
                  ).pack(anchor="w", pady=(5, 0))

        # ── Buttons ──
        btn_frame = ttk.Frame(container)
        btn_frame.pack(fill=tk.X, pady=(5, 10))
        ttk.Button(btn_frame, text="\U0001f4e7 Send test email",
                    command=self._send_test_email).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(btn_frame, text="\U0001f4be Save",
                    command=self.app._save_profile, style="Accent.TButton").pack(side=tk.LEFT)
        self.lbl_email_status = ttk.Label(btn_frame, text="", font=("Segoe UI", 9))
        self.lbl_email_status.pack(side=tk.LEFT, padx=(15, 0))

        # ── DPAPI Security Warning ──
        self._email_dpapi_warning = ttk.Label(
            container, text="", font=("Segoe UI", 8), foreground=COLORS["warning"])
        self._email_dpapi_warning.pack(anchor="w")
        self._check_dpapi_warning()

        # Hidden vars for backward compatibility (verification always on)
        self.var_auto_verify = tk.BooleanVar(value=True)
        self.var_alert_on_failure = tk.BooleanVar(value=True)

    # ── Helpers ────────────────────────────────

    def _check_dpapi_warning(self):
        """Show a warning if password storage is not secured by DPAPI."""
        test = store_password("test_check")
        if test.startswith("b64:"):
            self._email_dpapi_warning.configure(
                text="\u26a0 Warning: Passwords are stored with BASE64 encoding only (DPAPI unavailable). "
                     "This offers NO real protection. Consider using environment variables for sensitive credentials.")
        else:
            self._email_dpapi_warning.configure(text="")

    def _send_test_email(self):
        """Send a test email with current SMTP settings."""
        config = self._build_email_config()
        if not config.smtp_host:
            messagebox.showwarning("Email", "Please fill in the SMTP settings first.")
            return
        self.lbl_email_status.configure(text="Sending...", foreground=COLORS["warning"])
        self.app.root.update()

        def run():
            success, msg = send_test_email(config)
            self.app.root.after(0, lambda: self.lbl_email_status.configure(
                text=msg,
                foreground=COLORS["success"] if success else COLORS["danger"]
            ))
        threading.Thread(target=run, daemon=True).start()

    def _build_email_config(self) -> EmailConfig:
        """Build EmailConfig from current UI fields."""
        trigger = self.var_email_trigger.get()
        enabled = trigger != "disabled"
        on_success = trigger in ("success", "always")
        on_failure = trigger in ("failure", "always")

        return EmailConfig(
            enabled=enabled,
            smtp_host=self.var_smtp_host.get().strip(),
            smtp_port=self.var_smtp_port.get(),
            use_tls=self.var_smtp_tls.get(),
            username=self.var_smtp_user.get().strip(),
            password=self.var_smtp_password.get(),
            from_address=self.var_email_from.get().strip(),
            to_address=self.var_email_to.get().strip(),
            send_on_success=on_success,
            send_on_failure=on_failure,
        )

    def _send_backup_email(self, profile_name: str, success: bool,
                            summary: str, details: str = ""):
        """Send backup report email in background thread."""
        if not self.app.current_profile:
            return
        config = self.app.current_profile.email
        if not config.enabled:
            return

        def run():
            ok, msg = send_backup_report(config, profile_name, success, summary, details)
            if ok:
                self.app.root.after(0, lambda: self.app._show_status(f"\U0001f4e7 Email sent to {config.to_address}"))
            else:
                self.app.root.after(0, lambda: self.app._show_status(f"\U0001f4e7 Email failed: {msg}"))
        threading.Thread(target=run, daemon=True).start()

    # ── Profile load / collect ─────────────────

    def load_profile(self, profile):
        """Populate email fields from profile data."""
        em = profile.email
        # Map config fields to radio selection
        if not em.enabled:
            self.var_email_trigger.set("disabled")
        elif em.send_on_success and em.send_on_failure:
            self.var_email_trigger.set("always")
        elif em.send_on_failure:
            self.var_email_trigger.set("failure")
        elif em.send_on_success:
            self.var_email_trigger.set("success")
        else:
            self.var_email_trigger.set("disabled")
        self.var_smtp_host.set(em.smtp_host)
        self.var_smtp_port.set(em.smtp_port)
        self.var_smtp_tls.set(em.use_tls)
        self.var_smtp_user.set(em.username)
        self.var_smtp_password.set(em.password)
        self.var_email_from.set(em.from_address)
        self.var_email_to.set(em.to_address)

    def collect_config(self, profile):
        """Read email vars into profile.email."""
        profile.email = self._build_email_config()
