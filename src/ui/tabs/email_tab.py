"""Email tab: SMTP notification configuration."""

import threading
import tkinter as tk
from tkinter import ttk

from src.core.config import BackupProfile, EmailConfig
from src.notifications.email_notifier import SMTP_PRESETS, send_test_email
from src.ui.tabs import ScrollableTab
from src.ui.theme import Colors, Fonts, Spacing


class EmailTab(ScrollableTab):
    """Email notification configuration."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._build_ui()

    def _build_ui(self):
        # Trigger mode
        mode_frame = ttk.LabelFrame(self.inner, text="Send notifications", padding=Spacing.PAD)
        mode_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.LARGE)

        self.trigger_var = tk.StringVar(value="disabled")
        modes = [
            ("disabled", "Disabled"),
            ("failure", "On failure only"),
            ("success", "On success only"),
            ("always", "Always (success + failure)"),
        ]
        for value, label in modes:
            ttk.Radiobutton(
                mode_frame,
                text=label,
                value=value,
                variable=self.trigger_var,
            ).pack(anchor="w", pady=2)

        # SMTP configuration
        smtp_frame = ttk.LabelFrame(self.inner, text="SMTP server", padding=Spacing.PAD)
        smtp_frame.pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        # Presets
        preset_row = ttk.Frame(smtp_frame)
        preset_row.pack(fill="x", pady=(0, Spacing.MEDIUM))
        ttk.Label(preset_row, text="Presets:").pack(side="left")
        for name in ["Gmail", "Outlook", "ProtonMail"]:
            key = name.lower()
            ttk.Button(
                preset_row,
                text=name,
                command=lambda n=key: self._apply_preset(n),
            ).pack(side="left", padx=2)

        # Fields
        fields = [
            ("SMTP Host:", "host", ""),
            ("Port:", "port", "587"),
            ("Username:", "username", ""),
            ("Password:", "password", ""),
            ("From address:", "from_addr", ""),
            ("To address:", "to_addr", ""),
        ]

        self._vars = {}
        for label, key, default in fields:
            row = ttk.Frame(smtp_frame)
            row.pack(fill="x", pady=2)
            ttk.Label(row, text=label, width=15).pack(side="left")
            var = tk.StringVar(value=default)
            self._vars[key] = var
            if key == "password":
                ttk.Entry(row, textvariable=var, show="●").pack(side="left", fill="x", expand=True)
            elif key == "port":
                ttk.Spinbox(row, textvariable=var, from_=1, to=65535, width=8).pack(side="left")
            else:
                ttk.Entry(row, textvariable=var).pack(side="left", fill="x", expand=True)

        # TLS
        self.tls_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(smtp_frame, text="Use TLS (STARTTLS)", variable=self.tls_var).pack(
            anchor="w", pady=(Spacing.SMALL, 0)
        )

        ttk.Label(
            smtp_frame,
            text="For multiple recipients, separate with commas",
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w", pady=(Spacing.SMALL, 0))

        # Test button
        btn_frame = ttk.Frame(self.inner)
        btn_frame.pack(fill="x", padx=Spacing.LARGE, pady=(0, Spacing.LARGE))

        self.test_btn = ttk.Button(
            btn_frame,
            text="Send test email",
            command=self._test_email,
        )
        self.test_btn.pack(side="left")

        self.test_label = ttk.Label(btn_frame, text="", foreground=Colors.TEXT_SECONDARY)
        self.test_label.pack(side="left", padx=Spacing.LARGE)

    def _apply_preset(self, name: str):
        preset = SMTP_PRESETS.get(name, {})
        self._vars["host"].set(preset.get("host", ""))
        self._vars["port"].set(str(preset.get("port", 587)))
        self.tls_var.set(preset.get("tls", True))

    def _test_email(self):
        self.test_label.config(text="Sending...", foreground=Colors.WARNING)
        self.test_btn.state(["disabled"])

        def _send():
            config = self._build_email_config()
            ok, msg = send_test_email(config)
            self.after(0, lambda: self._show_test_result(ok, msg))

        threading.Thread(target=_send, daemon=True).start()

    def _show_test_result(self, ok: bool, msg: str):
        self.test_btn.state(["!disabled"])
        color = Colors.SUCCESS if ok else Colors.DANGER
        self.test_label.config(text=msg, foreground=color)

    def _build_email_config(self) -> EmailConfig:
        trigger = self.trigger_var.get()
        return EmailConfig(
            enabled=trigger != "disabled",
            smtp_host=self._vars["host"].get(),
            smtp_port=int(self._vars["port"].get() or 587),
            use_tls=self.tls_var.get(),
            username=self._vars["username"].get(),
            password=self._vars["password"].get(),
            from_address=self._vars["from_addr"].get(),
            to_address=self._vars["to_addr"].get(),
            send_on_success=trigger in ("success", "always"),
            send_on_failure=trigger in ("failure", "always"),
        )

    def load_profile(self, profile: BackupProfile):
        e = profile.email
        if not e.enabled:
            self.trigger_var.set("disabled")
        elif e.send_on_success and e.send_on_failure:
            self.trigger_var.set("always")
        elif e.send_on_success:
            self.trigger_var.set("success")
        elif e.send_on_failure:
            self.trigger_var.set("failure")
        else:
            self.trigger_var.set("disabled")

        self._vars["host"].set(e.smtp_host)
        self._vars["port"].set(str(e.smtp_port))
        self._vars["username"].set(e.username)
        self._vars["password"].set(e.password)
        self._vars["from_addr"].set(e.from_address)
        self._vars["to_addr"].set(e.to_address)
        self.tls_var.set(e.use_tls)

    def collect_config(self) -> dict:
        return {"email": self._build_email_config()}
