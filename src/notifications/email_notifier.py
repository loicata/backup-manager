"""
Backup Manager - Email Notification Module
============================================
Sends HTML-formatted backup reports via SMTP.

Trigger modes (configured per profile):
  disabled   → no emails
  failure    → only when backup fails
  success    → only when backup succeeds
  always     → after every backup

SMTP support:
  - STARTTLS (port 587) and SSL/TLS (port 465)
  - Username/password authentication
  - Multiple recipients (comma-separated To field)
  - Password cleared from memory after sending (secure_clear)

Common SMTP servers:
  Gmail:      smtp.gmail.com:587      (requires app password)
  Outlook:    smtp.office365.com:587
  ProtonMail: 127.0.0.1:1025          (via Proton Bridge)

The send_test_email() function allows verifying config from the UI.
Reports include a styled HTML template with status color, summary, and details.
"""

import logging
import smtplib
import ssl
from dataclasses import dataclass
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from src.security.secure_memory import secure_clear

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
#  Configuration
# ──────────────────────────────────────────────
@dataclass
class EmailConfig:
    """Email notification settings for a backup profile."""
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    use_tls: bool = True
    username: str = ""
    password: str = ""           # Protected via DPAPI when saved
    from_address: str = ""
    to_address: str = ""         # Comma-separated for multiple recipients
    send_on_success: bool = True
    send_on_failure: bool = True


# ──────────────────────────────────────────────
#  Email Sending
# ──────────────────────────────────────────────
def send_backup_report(
    config: EmailConfig,
    profile_name: str,
    success: bool,
    summary: str,
    details: str = "",
) -> tuple[bool, str]:
    """
    Send a backup report email.

    Returns (success, message).
    """
    if not config.enabled:
        return False, "Email notifications disabled"

    # Check trigger conditions
    if success and not config.send_on_success:
        return False, "Email on success disabled"
    if not success and not config.send_on_failure:
        return False, "Email on failure disabled"

    # Validate config
    if not config.smtp_host or not config.to_address:
        return False, "Incomplete email configuration"

    try:
        status_icon = "✅" if success else "❌"
        status_text = "SUCCESS" if success else "FAILED"

        subject = f"{status_icon} Backup Manager — {profile_name} — {status_text}"

        # Build HTML body
        status_color = "#27ae60" if success else "#e74c3c"
        html = _build_html_report(
            profile_name=profile_name,
            status_text=status_text,
            status_color=status_color,
            summary=summary,
            details=details,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        # Plain text fallback
        plain = (
            f"Backup Manager Report\n"
            f"{'=' * 40}\n"
            f"Profile: {profile_name}\n"
            f"Status: {status_text}\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"{summary}\n\n"
            f"{details}"
        )

        return _send_email(config, subject, plain, html)

    except Exception as e:
        msg = f"Failed to send email: {e}"
        logger.error(msg)
        return False, msg


def send_test_email(config: EmailConfig) -> tuple[bool, str]:
    """Send a test email to verify SMTP configuration."""
    if not config.smtp_host or not config.to_address:
        return False, "Please fill in all SMTP fields first."

    subject = "🧪 Backup Manager — Test Email"
    plain = (
        "This is a test email from Backup Manager.\n\n"
        "If you received this, your email configuration is working correctly!\n\n"
        f"SMTP: {config.smtp_host}:{config.smtp_port}\n"
        f"TLS: {'Yes' if config.use_tls else 'No'}\n"
        f"From: {config.from_address}\n"
        f"To: {config.to_address}\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    html = _build_html_report(
        profile_name="Test",
        status_text="TEST",
        status_color="#3498db",
        summary="This is a test email from Backup Manager.",
        details=f"SMTP: {config.smtp_host}:{config.smtp_port} | TLS: {config.use_tls}",
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    return _send_email(config, subject, plain, html)


# ──────────────────────────────────────────────
#  Internal Helpers
# ──────────────────────────────────────────────
def _send_email(
    config: EmailConfig,
    subject: str,
    plain_body: str,
    html_body: str,
) -> tuple[bool, str]:
    """Send an email via SMTP. Returns (success, message)."""
    # Take a local reference to the password (will be cleared after use)
    password = config.password
    username = config.username

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = config.from_address or username
        msg["To"] = config.to_address

        msg.attach(MIMEText(plain_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        recipients = [addr.strip() for addr in config.to_address.split(",") if addr.strip()]

        if config.use_tls:
            # Create SSL context for secure connection
            context = ssl.create_default_context()
            # Port 465 = implicit SSL (SMTP_SSL), port 587 = STARTTLS
            if config.smtp_port == 465:
                with smtplib.SMTP_SSL(config.smtp_host, config.smtp_port,
                                       context=context, timeout=30) as server:
                    if username and password:
                        server.login(username, password)
                    server.sendmail(msg["From"], recipients, msg.as_string())
            else:
                with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as server:
                    server.starttls(context=context)
                    if username and password:
                        server.login(username, password)
                    server.sendmail(msg["From"], recipients, msg.as_string())
        else:
            with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as server:
                if username and password:
                    server.login(username, password)
                server.sendmail(msg["From"], recipients, msg.as_string())

        logger.info(f"Email sent to {config.to_address}")
        return True, f"Email sent to {config.to_address}"

    # ── SMTP error handling: specific messages for common failures ──
    except smtplib.SMTPAuthenticationError:
        return False, "Authentication failed — check username and password."
    except smtplib.SMTPConnectError:
        return False, f"Cannot connect to {config.smtp_host}:{config.smtp_port}"
    except smtplib.SMTPRecipientsRefused:
        return False, f"Recipient refused: {config.to_address}"
    except TimeoutError:
        return False, f"Connection timeout — {config.smtp_host}:{config.smtp_port}"
    except Exception as e:
        return False, f"SMTP error: {e}"
    finally:
        # Clear password from local memory
        secure_clear(password)
        password = None


# ── HTML email template ──
# Generates a styled HTML report with status color, summary table,
# and error details. Falls back to plain text if email client doesn't support HTML.
def _build_html_report(
    profile_name: str,
    status_text: str,
    status_color: str,
    summary: str,
    details: str,
    timestamp: str,
) -> str:
    """Build an HTML email report."""
    details_html = details.replace("\n", "<br>") if details else ""

    return f"""<!DOCTYPE html>
<html>
<body style="font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f6fa;">
  <div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 8px;
              box-shadow: 0 2px 10px rgba(0,0,0,0.1); overflow: hidden;">
    <div style="background: #2c3e50; padding: 20px; text-align: center;">
      <h1 style="color: white; margin: 0; font-size: 20px;">Backup Manager</h1>
    </div>
    <div style="padding: 25px;">
      <div style="background: {status_color}; color: white; padding: 12px 20px;
                  border-radius: 6px; text-align: center; font-size: 18px; font-weight: bold;">
        {status_text}
      </div>
      <table style="width: 100%; margin-top: 20px; border-collapse: collapse;">
        <tr>
          <td style="padding: 8px 0; color: #7f8c8d; width: 120px;">Profile:</td>
          <td style="padding: 8px 0; font-weight: bold;">{profile_name}</td>
        </tr>
        <tr>
          <td style="padding: 8px 0; color: #7f8c8d;">Time:</td>
          <td style="padding: 8px 0;">{timestamp}</td>
        </tr>
      </table>
      <div style="margin-top: 15px; padding: 15px; background: #f8f9fa; border-radius: 6px;
                  border-left: 4px solid {status_color};">
        <p style="margin: 0; white-space: pre-line;">{summary}</p>
      </div>
      {"<div style='margin-top: 15px; padding: 10px; background: #fafafa; border-radius: 4px; font-size: 13px; color: #555;'>" + details_html + "</div>" if details_html else ""}
    </div>
    <div style="background: #ecf0f1; padding: 12px; text-align: center; font-size: 12px; color: #95a5a6;">
      Backup Manager v2.2.9 — Automatic notification
    </div>
  </div>
</body>
</html>"""
