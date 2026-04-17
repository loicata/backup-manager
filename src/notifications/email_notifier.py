"""Email notification system for backup reports.

Sends HTML-formatted backup reports via SMTP.
Supports Gmail, Outlook, ProtonMail (via Bridge), and custom servers.
"""

import html
import logging
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.core.config import EmailConfig


def _esc(value) -> str:
    """HTML-escape ``value`` for safe inclusion in a template.

    All user-supplied fields (profile names, file paths, error
    messages from exceptions) flow straight into f-string HTML
    templates. Without escaping, a profile named ``</td><script>``
    or a file path with ``&``/``<`` in it breaks the rendered
    email — and on lax webmail clients could even execute the
    script. ``html.escape`` covers the five metacharacters that
    matter: ``&``, ``<``, ``>``, ``"``, ``'``.
    """
    return html.escape(str(value), quote=True)


logger = logging.getLogger(__name__)

# SMTP presets
SMTP_PRESETS = {
    "gmail": {"host": "smtp.gmail.com", "port": 587, "tls": True},
    "outlook": {"host": "smtp.office365.com", "port": 587, "tls": True},
    "protonmail": {"host": "127.0.0.1", "port": 1025, "tls": False},
}

# Authentication hints displayed under the password field per provider
SMTP_AUTH_HINTS: dict[str, str] = {
    "gmail": (
        "Use an App Password, not your Google password.\n"
        "Google Account \u2192 Security \u2192 2-Step Verification \u2192 App passwords"
    ),
    "outlook": (
        "If MFA is enabled, use an App Password.\n"
        "Microsoft Account \u2192 Security \u2192 App passwords"
    ),
    "protonmail": (
        "Requires Proton Mail Bridge running locally.\n"
        "Use the Bridge password, not your Proton account password."
    ),
}


def send_backup_report(
    config: EmailConfig,
    profile_name: str,
    success: bool,
    summary: str,
    details: str = "",
    cancelled: bool = False,
    result=None,
    backup_type: str = "",
    free_space: int | None = None,
) -> tuple[bool, str]:
    """Send backup report email.

    Args:
        config: Email configuration.
        profile_name: Name of the backup profile.
        success: Whether the backup succeeded.
        summary: Short summary text.
        details: Optional detailed text (log lines).
        cancelled: Whether the backup was cancelled by the user.
        result: Optional BackupResult with full metrics.
        backup_type: "FULL" or "DIFFERENTIAL".
        free_space: Remaining disk space in bytes (primary destination).

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
        status_emoji = "\u26a0\ufe0f"
        status_text = "CANCELLED"
    elif success:
        status_emoji = "\u2705"
        status_text = "SUCCESS"
    else:
        status_emoji = "\u274c"
        status_text = "FAILED"

    subject = f"{status_emoji} Backup Manager \u2014 {profile_name} \u2014 {status_text}"

    if result is not None:
        html_body = _build_backup_html(
            profile_name,
            success,
            summary,
            details,
            cancelled=cancelled,
            result=result,
            backup_type=backup_type,
            free_space=free_space,
        )
    else:
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
    except smtplib.SMTPRecipientsRefused as e:
        # The recipient (user's notification inbox) was rejected by
        # the server. Previously this tumbled into the generic
        # ``except Exception`` and was logged as a one-line warning
        # — the caller kept going, the backup was marked success,
        # and the user never learned their notifications stopped
        # being delivered. Surface a distinct, actionable message
        # AND an ERROR-level log so the operator's log watcher or
        # the bug report surface catches it.
        refused = ", ".join(e.recipients.keys()) if e.recipients else config.to_address
        logger.error(
            "SMTP recipients refused for %s — notifications are NOT being delivered. "
            "Check that the address is valid and the mailbox is not full.",
            refused,
        )
        return False, (
            f"Recipients refused: {refused}. Notifications are not being "
            f"delivered — update the 'To' address."
        )
    except smtplib.SMTPSenderRefused as e:
        logger.error("SMTP sender refused: %s (%s)", config.from_address, e.smtp_error)
        return False, (
            f"Sender refused: {config.from_address}. The SMTP server will not "
            f"relay mail from this address — check the 'From' field and the "
            f"provider's sending policy."
        )
    except Exception as e:
        # Log without traceback to avoid leaking credentials from login() frames
        logger.error("Email send failed: %s: %s", type(e).__name__, e)
        return False, f"Email error: {type(e).__name__}: {e}"


def _build_html(
    profile_name: str,
    success: bool,
    summary: str,
    details: str = "",
    cancelled: bool = False,
) -> str:
    """Build HTML email body."""
    from src import __version__

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
                            font-size: 12px; overflow-x: auto;">{_esc(details)}</pre>
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
                    Backup Manager — {_esc(status)}
                </td>
            </tr>
            <tr>
                <td style="padding: 16px 20px;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Profile</td>
                            <td style="padding: 8px 0; font-weight: bold;">{_esc(profile_name)}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Time</td>
                            <td style="padding: 8px 0;">{_esc(timestamp)}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Summary</td>
                            <td style="padding: 8px 0;">{_esc(summary)}</td>
                        </tr>
                    </table>
                </td>
            </tr>
            {details_section}
            <tr>
                <td style="padding: 12px 20px; color: #999; font-size: 11px;
                           border-top: 1px solid #eee; text-align: center;">
                    Backup Manager v{_esc(__version__)}
                </td>
            </tr>
        </table>
    </body>
    </html>
    """


def _build_verify_html(
    profile_name: str,
    success: bool,
    summary: str,
    results: list,
) -> str:
    """Build HTML email body for verification reports with a results table.

    Args:
        profile_name: Human-readable profile name.
        success: Whether all verifications passed.
        summary: Summary line (e.g. "6 OK, 0 error(s) in 4.4s").
        results: List of BackupVerifyResult objects.

    Returns:
        Complete HTML string.
    """
    from src import __version__

    color = "#27ae60" if success else "#e74c3c"
    status = "VERIFICATION OK" if success else "VERIFICATION FAILED"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Status colors for table cells
    status_colors = {
        "ok": "#27ae60",
        "warning": "#f39c12",
        "corrupted": "#e74c3c",
        "missing": "#f39c12",
        "error": "#e74c3c",
    }

    # Build table rows from results
    table_rows = ""
    for bvr in results:
        s_color = status_colors.get(bvr.status, "#666")
        s_label = _esc(bvr.status.upper())
        table_rows += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{_esc(bvr.destination)}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;
                           font-family: monospace; font-size: 12px;">{_esc(bvr.backup_name)}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;
                           color: {s_color}; font-weight: bold;">{s_label}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;
                           color: #666; font-size: 12px;">{_esc(bvr.message)}</td>
            </tr>"""

    return f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px;
                 background: #f5f6fa;">
        <table style="max-width: 750px; margin: 0 auto; background: white;
                      border-radius: 8px; overflow: hidden;
                      box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <tr>
                <td style="background: {color}; color: white; padding: 16px 20px;
                           font-size: 18px; font-weight: bold;">
                    Backup Manager — {_esc(status)}
                </td>
            </tr>
            <tr>
                <td style="padding: 16px 20px;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Profile</td>
                            <td style="padding: 8px 0; font-weight: bold;">{_esc(profile_name)}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Time</td>
                            <td style="padding: 8px 0;">{_esc(timestamp)}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; color: #666;">Summary</td>
                            <td style="padding: 8px 0;">{_esc(summary)}</td>
                        </tr>
                    </table>
                </td>
            </tr>
            <tr>
                <td style="padding: 12px 20px; border-top: 1px solid #eee;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr style="background: #f8f9fa;">
                            <th style="padding: 8px; text-align: left;
                                       font-size: 12px; color: #333;">Destination</th>
                            <th style="padding: 8px; text-align: left;
                                       font-size: 12px; color: #333;">Backup</th>
                            <th style="padding: 8px; text-align: left;
                                       font-size: 12px; color: #333;">Status</th>
                            <th style="padding: 8px; text-align: left;
                                       font-size: 12px; color: #333;">Details</th>
                        </tr>
                        {table_rows}
                    </table>
                </td>
            </tr>
            <tr>
                <td style="padding: 12px 20px; color: #999; font-size: 11px;
                           border-top: 1px solid #eee; text-align: center;">
                    Backup Manager v{_esc(__version__)}
                </td>
            </tr>
        </table>
    </body>
    </html>
    """


def _format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hours}h {mins:02d}m"


def _format_size(size_bytes: int) -> str:
    """Format bytes into human-readable size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _format_rate(size_bytes: int, seconds: float) -> str:
    """Format transfer rate."""
    if seconds <= 0:
        return "—"
    rate = size_bytes / seconds
    if rate < 1024 * 1024:
        return f"{rate / 1024:.1f} KB/s"
    return f"{rate / (1024 * 1024):.1f} MB/s"


_ROW = """<tr>
    <td style="padding: 6px 0; color: #666; width: 160px;">{label}</td>
    <td style="padding: 6px 0;">{value}</td>
</tr>"""

_SECTION = """<tr>
    <td style="padding: 12px 20px; border-top: 1px solid #eee;">
        <strong style="color: #333; font-size: 13px;">{title}</strong>
        <table style="width: 100%; border-collapse: collapse; margin-top: 6px;">
            {rows}
        </table>
    </td>
</tr>"""


def _build_backup_html(
    profile_name: str,
    success: bool,
    summary: str,
    details: str = "",
    cancelled: bool = False,
    result=None,
    backup_type: str = "",
    free_space: int | None = None,
) -> str:
    """Build enriched HTML email body with full backup metrics.

    Args:
        profile_name: Profile name.
        success: Whether backup succeeded.
        summary: Short summary text.
        details: Log lines.
        cancelled: Whether cancelled.
        result: BackupResult with full metrics.
        backup_type: "FULL" or "DIFFERENTIAL".
        free_space: Remaining disk space in bytes.

    Returns:
        HTML string.
    """
    from src import __version__

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

    # --- Overview section ---
    overview_rows = _ROW.format(label="Profile", value=f"<strong>{_esc(profile_name)}</strong>")
    if backup_type:
        overview_rows += _ROW.format(label="Type", value=_esc(backup_type))
    overview_rows += _ROW.format(label="Time", value=_esc(timestamp))
    if result and result.duration_seconds > 0:
        overview_rows += _ROW.format(
            label="Duration", value=_esc(_format_duration(result.duration_seconds))
        )

    sections = _SECTION.format(title="Overview", rows=overview_rows)

    # --- Statistics section ---
    if result:
        stat_rows = _ROW.format(label="Files found", value=f"{result.files_found:,}")
        stat_rows += _ROW.format(label="Files processed", value=f"{result.files_processed:,}")
        if result.files_skipped > 0:
            stat_rows += _ROW.format(
                label="Files skipped", value=f"{result.files_skipped:,} (unchanged)"
            )
        if result.bytes_source > 0:
            stat_rows += _ROW.format(label="Source size", value=_format_size(result.bytes_source))
        sections += _SECTION.format(title="Statistics", rows=stat_rows)

    # --- Destinations section ---
    if result and result.mirror_results:
        dest_rows = _ROW.format(
            label="Primary",
            value=_esc(result.backup_path or "OK"),
        )
        for mirror_tuple in result.mirror_results:
            name, ok, msg = mirror_tuple[0], mirror_tuple[1], mirror_tuple[2]
            desc = mirror_tuple[3] if len(mirror_tuple) > 3 else ""
            if ok:
                display = _esc(desc if desc else msg)
            else:
                display = f'<span style="color: #e74c3c;">FAILED</span> {_esc(msg)}'
            dest_rows += _ROW.format(label=_esc(name), value=display)
        sections += _SECTION.format(title="Destinations", rows=dest_rows)

    # --- Retention section ---
    if result and (result.rotated_count > 0 or result.backups_available > 0):
        ret_rows = ""
        if result.backups_available > 0:
            ret_rows += _ROW.format(label="Backups available", value=str(result.backups_available))
        ret_rows += _ROW.format(label="Old backups deleted", value=str(result.rotated_count))
        sections += _SECTION.format(title="Retention", rows=ret_rows)

    # --- Errors / warnings sections — kept visually distinct so a
    # successful backup with only warnings does not look like a failure.
    if result and result.phase_errors:
        from src.core.backup_result import ErrorSeverity

        errors = [
            e
            for e in result.phase_errors
            if e.severity in (ErrorSeverity.ERROR, ErrorSeverity.FATAL)
        ]
        warnings = [e for e in result.phase_errors if e.severity == ErrorSeverity.WARNING]

        def _render_lines(items):
            # The result is embedded in a ``<pre>`` so escape everything
            # — file paths and raw exception messages routinely contain
            # ``<`` / ``>`` / ``&`` that would break or inject HTML.
            lines = []
            for err in items[:20]:
                if err.file_path:
                    lines.append(f"[{_esc(err.phase)}] {_esc(err.file_path)}: {_esc(err.message)}")
                else:
                    lines.append(f"[{_esc(err.phase)}] {_esc(err.message)}")
            remaining = len(items) - 20
            if remaining > 0:
                lines.append(f"... and {remaining} more")
            return "\n".join(lines)

        if errors:
            sections += f"""<tr>
    <td style="padding: 12px 20px; border-top: 1px solid #eee;">
        <strong style="color: #e74c3c; font-size: 13px;">Errors ({len(errors)})</strong>
        <pre style="background: #fdf2f2; padding: 10px; border-radius: 4px;
                    font-size: 12px; overflow-x: auto; color: #c0392b;
                    margin-top: 6px;">{_render_lines(errors)}</pre>
    </td>
</tr>"""

        if warnings:
            sections += f"""<tr>
    <td style="padding: 12px 20px; border-top: 1px solid #eee;">
        <strong style="color: #d68910; font-size: 13px;">Warnings ({len(warnings)})</strong>
        <pre style="background: #fdf6e3; padding: 10px; border-radius: 4px;
                    font-size: 12px; overflow-x: auto; color: #9a7d0a;
                    margin-top: 6px;">{_render_lines(warnings)}</pre>
    </td>
</tr>"""

    # --- Log section ---
    log_section = ""
    if details:
        log_section = f"""<tr>
    <td style="padding: 12px 20px; border-top: 1px solid #eee;">
        <strong style="color: #333; font-size: 13px;">Log</strong>
        <pre style="background: #f8f9fa; padding: 10px; border-radius: 4px;
                    font-size: 11px; overflow-x: auto; max-height: 400px;
                    overflow-y: auto; margin-top: 6px;">{_esc(details)}</pre>
    </td>
</tr>"""

    return f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px;
                 background: #f5f6fa;">
        <table style="max-width: 650px; margin: 0 auto; background: white;
                      border-radius: 8px; overflow: hidden;
                      box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <tr>
                <td style="background: {color}; color: white; padding: 16px 20px;
                           font-size: 18px; font-weight: bold;">
                    Backup Manager \u2014 {_esc(status)}
                </td>
            </tr>
            {sections}
            {log_section}
            <tr>
                <td style="padding: 12px 20px; color: #999; font-size: 11px;
                           border-top: 1px solid #eee; text-align: center;">
                    Backup Manager v{_esc(__version__)}
                </td>
            </tr>
        </table>
    </body>
    </html>
    """


def send_verify_report(
    config: EmailConfig,
    profile_name: str,
    result,
) -> tuple[bool, str]:
    """Send integrity verification report email.

    Args:
        config: Email configuration.
        profile_name: Human-readable profile name.
        result: VerifyAllResult with ok_count, error_count, results.

    Returns:
        (success, message) tuple.
    """
    if not config.enabled:
        return False, "Email notifications disabled"

    warning_count = getattr(result, "warning_count", 0)
    has_warnings = warning_count > 0

    # Send when: errors detected, warnings detected, OR success+send_on_success.
    # Warnings MUST trigger a notification even on ``result.success == True``
    # because they indicate integrity cannot be proven — the operator needs
    # to know. Without this branch, a silent "all OK but actually 5 backups
    # unverifiable" state was possible.
    if result.success and not has_warnings and not config.send_on_success:
        return False, "No errors/warnings and success notifications disabled"

    if not result.success:
        subject = f"Backup Manager — Verification FAILED — {profile_name}"
    elif has_warnings:
        subject = (
            f"Backup Manager — Verification with {warning_count} warning(s) " f"— {profile_name}"
        )
    else:
        subject = f"Backup Manager — Verification OK — {profile_name}"

    if has_warnings:
        summary = (
            f"{result.ok_count} OK, {warning_count} warning(s), "
            f"{result.error_count} error(s) in {result.duration_seconds:.1f}s"
        )
    else:
        summary = (
            f"{result.ok_count} OK, {result.error_count} error(s) "
            f"in {result.duration_seconds:.1f}s"
        )

    html = _build_verify_html(
        profile_name,
        success=result.success,
        summary=summary,
        results=result.results,
    )
    return _send_email(config, subject, html)
