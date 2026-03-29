"""Email notification system for backup reports.

Sends HTML-formatted backup reports via SMTP.
Supports Gmail, Outlook, ProtonMail (via Bridge), and custom servers.
"""

import logging
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.core.config import EmailConfig

logger = logging.getLogger(__name__)

# SMTP presets
SMTP_PRESETS = {
    "gmail": {"host": "smtp.gmail.com", "port": 587, "tls": True},
    "outlook": {"host": "smtp.office365.com", "port": 587, "tls": True},
    "protonmail": {"host": "127.0.0.1", "port": 1025, "tls": False},
}


def send_backup_report(
    config: EmailConfig,
    profile_name: str,
    success: bool,
    summary: str,
    details: str = "",
    cancelled: bool = False,
) -> tuple[bool, str]:
    """Send backup report email.

    Args:
        config: Email configuration.
        profile_name: Name of the backup profile.
        success: Whether the backup succeeded.
        summary: Short summary text.
        details: Optional detailed text.
        cancelled: Whether the backup was cancelled by the user.

    Returns:
        (sent, message) tuple.
    """
    if not config.enabled:
        return False, "Email notifications disabled"

    # Cancelled follows the same trigger as failure
    if cancelled:
        if not config.send_on_failure:
            return False, "Failure notification disabled"
    elif success and not config.send_on_success:
        return False, "Success notification disabled"
    elif not success and not config.send_on_failure:
        return False, "Failure notification disabled"

    if cancelled:
        status_emoji = "⚠️"
        status_text = "CANCELLED"
    elif success:
        status_emoji = "✅"
        status_text = "SUCCESS"
    else:
        status_emoji = "❌"
        status_text = "FAILED"

    subject = f"{status_emoji} Backup Manager — {profile_name} — {status_text}"
    html_body = _build_html(profile_name, success, summary, details, cancelled=cancelled)

    return _send_email(config, subject, html_body)


def send_test_email(config: EmailConfig) -> tuple[bool, str]:
    """Send a test email to verify SMTP configuration."""
    subject = "🔧 Backup Manager — Test Email"
    html_body = _build_html(
        "Test",
        True,
        "This is a test email from Backup Manager.",
        f"Sent at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    )
    return _send_email(config, subject, html_body)


def _send_email(
    config: EmailConfig,
    subject: str,
    html_body: str,
) -> tuple[bool, str]:
    """Send an email via SMTP."""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = config.from_address
        msg["To"] = config.to_address
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        recipients = [addr.strip() for addr in config.to_address.split(",") if addr.strip()]

        if config.use_tls and config.smtp_port == 465:
            # SSL connection
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(
                config.smtp_host,
                config.smtp_port,
                context=context,
                timeout=30,
            ) as server:
                if config.username:
                    server.login(config.username, config.password)
                server.sendmail(config.from_address, recipients, msg.as_string())
        else:
            # STARTTLS or plain
            with smtplib.SMTP(
                config.smtp_host,
                config.smtp_port,
                timeout=30,
            ) as server:
                if config.use_tls:
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                if config.username:
                    server.login(config.username, config.password)
                server.sendmail(config.from_address, recipients, msg.as_string())

        logger.info("Email sent to %s", config.to_address)
        return True, "Email sent successfully"

    except smtplib.SMTPAuthenticationError:
        return False, "SMTP authentication failed — check username/password"
    except smtplib.SMTPConnectError:
        return False, f"Could not connect to {config.smtp_host}:{config.smtp_port}"
    except Exception as e:
        logger.exception("Email send failed")
        return False, f"Email error: {type(e).__name__}: {e}"


def _build_html(
    profile_name: str,
    success: bool,
    summary: str,
    details: str = "",
    cancelled: bool = False,
) -> str:
    """Build HTML email body."""
    if cancelled:
        color = "#f39c12"
        status = "CANCELLED"
    elif success:
        color = "#27ae60"
        status = "SUCCESS"
    else:
        color = "#e74c3c"
        status = "FAILED"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    details_section = ""
    if details:
        details_section = f"""
        <tr>
            <td style="padding: 12px; border-top: 1px solid #eee;">
                <strong>Details</strong>
                <pre style="background: #f8f9fa; padding: 10px; border-radius: 4px;
                            font-size: 12px; overflow-x: auto;">{details}</pre>
            </td>
        </tr>
        """

    return f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px;
                 background: #f5f6fa;">
        <table style="max-width: 600px; margin: 0 auto; background: white;
                      border-radius: 8px; overflow: hidden;
                      box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <tr>
                <td style="background: {color}; color: white; padding: 16px 20px;
                           font-size: 18px; font-weight: bold;">
                    Backup Manager — {status}
                </td>
            </tr>
            <tr>
                <td style="padding: 16px 20px;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Profile</td>
                            <td style="padding: 8px 0; font-weight: bold;">{profile_name}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Time</td>
                            <td style="padding: 8px 0;">{timestamp}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Summary</td>
                            <td style="padding: 8px 0;">{summary}</td>
                        </tr>
                    </table>
                </td>
            </tr>
            {details_section}
            <tr>
                <td style="padding: 12px 20px; color: #999; font-size: 11px;
                           border-top: 1px solid #eee; text-align: center;">
                    Backup Manager v3.1.4
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
