"""Main application window with sidebar and tabbed interface."""

import contextlib
import hashlib
import hmac
import json
import logging
import os
import platform
import re
import sys
import threading
import time
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk

from src import __version__
from src.core.backup_engine import BackupEngine, CancelledError
from src.core.config import BackupProfile, BackupType, ConfigManager, StorageConfig
from src.core.events import STATUS, EventBus
from src.core.health_checker import check_destinations_async
from src.core.scheduler import AutoStart, InAppScheduler
from src.ui.tabs.email_tab import EmailTab
from src.ui.tabs.encryption_tab import EncryptionTab
from src.ui.tabs.general_tab import GeneralTab
from src.ui.tabs.history_tab import HistoryTab
from src.ui.tabs.mirror_tab import MirrorTab
from src.ui.tabs.protection_tab import ProtectionTab
from src.ui.tabs.recovery_tab import RecoveryTab
from src.ui.tabs.retention_tab import RetentionTab
from src.ui.tabs.run_tab import RunTab
from src.ui.tabs.schedule_tab import ScheduleTab
from src.ui.tabs.storage_tab import StorageTab
from src.ui.tabs.verify_tab import VerifyTab
from src.ui.theme import (
    APP_TITLE,
    MIN_SIZE,
    WINDOW_SIZE,
    Colors,
    Fonts,
    Spacing,
    setup_theme,
)
from src.ui.tray import BackupTray, TrayState

logger = logging.getLogger(__name__)

# Interval between continuous health checks for destinations (ms)
HEALTH_POLL_INTERVAL_MS = 60_000

# Bug report destination
BUG_REPORT_EMAIL = "loic@loicata.com"

# Regex patterns for log anonymization
_ANON_PATTERNS: list[tuple[re.Pattern, str]] = [
    # Windows user paths (C:\Users\xxx\...)
    (re.compile(r"[A-Za-z]:\\Users\\[^\\]+\\[^\s\"',:]+"), "***"),
    # Windows environment variable paths (%APPDATA%\...)
    (re.compile(r"%[A-Za-z_]+%\\[^\s\"',:]+"), "***"),
    # UNC paths (\\server\share\...)
    (re.compile(r"\\\\[^\\]+\\[^\s\"',:]+"), "\\\\***\\***"),
    # Linux home paths (/home/xxx/...)
    (re.compile(r"/home/[^/]+/[^\s\"',:]+"), "***/***"),
    # IPv4 addresses
    (re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "***.***.***.***"),
    # Email addresses
    (re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"), "***@***.***"),
    # S3 bucket URIs — match AWS naming rules (letters, digits,
    # dots, hyphens). The previous pattern also matched ``[`` and
    # ``]`` which are not valid in bucket names, so it would never
    # legitimately fire and was effectively dead weight.
    (re.compile(r"s3://[a-zA-Z0-9.\-]+"), "s3://[bucket]"),
    # Hostnames (word.word.tld patterns, min 2 dots)
    (re.compile(r"\b[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+\.[a-zA-Z]{2,}\b"), "[host]"),
    # Quoted strings (profile names, paths, etc.)
    (re.compile(r"'[^']{1,100}'"), "'[profile]'"),
]

# Keywords that indicate prompt injection attempts
_INJECTION_KEYWORDS = re.compile(
    r"\[\[\[|OVERRIDE|IGNORE\s+(ALL|PREVIOUS|ABOVE|SAFETY|RULES|INSTRUCTIONS)"
    r"|SYSTEM\s*(PROMPT|MESSAGE|OVERRIDE|INSTRUCTION)"
    r"|CLAUDE\s*(CODE|OVERRIDE|INSTRUCTION)"
    r"|INSTRUCTION\s*:|EXECUTE\s*:|ADMIN\s*:|AUTHORIZED\s*:"
    r"|YOU\s+MUST|YOU\s+SHOULD\s+NOW|DISREGARD"
    r"|PRETEND\s+YOU|ACT\s+AS|ROLE\s*:"
    r"|SECURITY\s+RULES|SAFETY\s+RULES",
    re.IGNORECASE,
)

# Maximum length for user-provided description
_MAX_DESCRIPTION_LEN = 2000


def _is_packaged_build() -> bool:
    """Return True for both PyInstaller and Nuitka packaged builds.

    PyInstaller sets ``sys.frozen = True`` at runtime. Nuitka does
    NOT — it exposes ``__compiled__`` on the main module instead.
    Relying on ``sys.frozen`` alone treats Nuitka binaries as dev
    builds, which makes the bug report attempt git lookups and
    source-file reads that will not work next to a compiled .exe.

    Returns:
        True if running from a packaged binary (PyInstaller or Nuitka).
    """
    if getattr(sys, "frozen", False):
        return True
    try:
        from src.__main__ import _is_nuitka

        return _is_nuitka()
    except ImportError:
        return False


def _normalize_unicode(text: str) -> str:
    """Normalize Unicode lookalike characters to ASCII equivalents.

    Prevents bypass of anonymization patterns using fullwidth @,
    homoglyph characters, or other Unicode tricks.

    Args:
        text: Input text potentially containing Unicode tricks.

    Returns:
        Normalized ASCII-equivalent text.
    """
    import unicodedata

    # NFKC normalization converts fullwidth chars to ASCII equivalents
    return unicodedata.normalize("NFKC", text)


def anonymize_log_lines(lines: list[str]) -> list[str]:
    """Anonymize sensitive data in log lines.

    Applies Unicode normalization first, then removes file paths,
    IP addresses, email addresses, bucket names, hostnames,
    and profile names while preserving timestamps and log levels.

    Args:
        lines: Raw log lines to anonymize.

    Returns:
        List of anonymized log lines.
    """
    result = []
    for line in lines:
        line = _normalize_unicode(line)
        for pattern, replacement in _ANON_PATTERNS:
            line = pattern.sub(replacement, line)
        result.append(line)
    return result


def _sanitize_user_text(text: str) -> str:
    """Sanitize user-provided text to prevent prompt injection.

    Truncates to a safe length, normalizes Unicode, applies
    anonymization, and strips injection-like patterns.

    Args:
        text: Raw user description from the bug report form.

    Returns:
        Sanitized text safe for inclusion in diagnostic files.
    """
    if not text:
        return ""
    # Truncate
    text = text[:_MAX_DESCRIPTION_LEN]
    # Unicode normalization
    text = _normalize_unicode(text)
    # Strip injection keywords
    text = _INJECTION_KEYWORDS.sub("[REMOVED]", text)
    # Anonymize any sensitive data the user may have pasted
    lines = text.splitlines()
    lines = anonymize_log_lines(lines)
    return "\n".join(lines)


# -- Bug report helper functions (module-level for testability) --

_PROJECT_DEPS = [
    "cryptography",
    "pystray",
    "Pillow",
    "paramiko",
    "boto3",
    "sv_ttk",
]


def _collect_dependency_versions() -> str:
    """Collect installed versions of project dependencies.

    Returns:
        Comma-separated list of package==version strings.
    """
    parts = []
    for pkg in _PROJECT_DEPS:
        try:
            from importlib.metadata import version

            parts.append(f"{pkg}=={version(pkg)}")
        except Exception:
            parts.append(f"{pkg}=?")
    return ", ".join(parts)


def _extract_recent_errors(log_file: Path, count: int = 3) -> str | None:
    """Extract the last N ERROR/CRITICAL lines with context from a log file.

    Args:
        log_file: Path to the log file.
        count: Number of recent errors to extract.

    Returns:
        Anonymized error context string, or None if no errors found.
    """
    if not log_file.exists():
        return None
    try:
        # ``errors="replace"`` tolerates mixed encodings (UTF-8 + CP1252)
        # that can occur in Windows log tails. Without it, a single bad
        # byte would crash the whole report generation.
        raw_lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None

    # Find all ERROR/CRITICAL line indices
    error_indices = [
        i for i, line in enumerate(raw_lines) if "[ERROR]" in line or "[CRITICAL]" in line
    ]
    if not error_indices:
        return None

    # Take the last N errors
    sections: list[str] = []
    for idx in error_indices[-count:]:
        start = max(0, idx - 2)
        end = min(len(raw_lines), idx + 4)
        context = raw_lines[start:end]
        sections.append("\n".join(anonymize_log_lines(context)))

    return "\n---\n".join(sections)


def _extract_traceback_info(crash_file: Path) -> str | None:
    """Extract anonymized traceback from crash.log.

    Preserves file references (src/...) but anonymizes user paths.
    Extracts: exception type, error message, source file + line.

    Args:
        crash_file: Path to the crash log file.

    Returns:
        Anonymized traceback string, or None if no crash log.
    """
    if not crash_file.exists():
        return None
    try:
        raw_lines = crash_file.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    if not raw_lines:
        return None
    return "\n".join(anonymize_log_lines(raw_lines))


def _get_git_commit() -> str:
    """Get the current git commit hash, or 'frozen_build' in packaged builds."""
    if _is_packaged_build():
        return "frozen_build"
    try:
        import subprocess

        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=Path(__file__).resolve().parent.parent,
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def _parse_traceback_structured(crash_file: Path) -> list[dict]:
    """Parse crash.log into structured traceback entries.

    Args:
        crash_file: Path to the crash log file.

    Returns:
        List of dicts with keys: file, line, function, exception, message.
    """
    if not crash_file.exists():
        return []
    try:
        raw = crash_file.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    entries: list[dict] = []
    frame_pattern = re.compile(r'File "([^"]+)", line (\d+)(?:, in (.+))?')
    for match in frame_pattern.finditer(raw):
        filepath = match.group(1)
        # Keep only src/ relative paths, anonymize the rest
        if "src" in filepath:
            idx = filepath.find("src")
            filepath = filepath[idx:]
        else:
            filepath = "***"
        entries.append(
            {
                "file": filepath,
                "line": int(match.group(2)),
                "function": match.group(3) or "unknown",
            }
        )

    # Extract the final exception line (anonymize the message)
    lines = raw.strip().splitlines()
    if lines:
        last_line = lines[-1].strip()
        if ":" in last_line and not last_line.startswith(" "):
            exc_type, _, exc_msg = last_line.partition(":")
            # Anonymize exception message — it may contain
            # user paths, IPs, or injected content
            anon_msg = anonymize_log_lines([exc_msg.strip()])
            entries.append(
                {
                    "exception_type": exc_type.strip(),
                    "exception_message": anon_msg[0],
                }
            )

    return entries


def _build_machine_readable(diagnostic: str, include_logs: bool = False) -> dict:
    """Build a structured machine-readable dictionary from diagnostic text.

    Args:
        diagnostic: The human-readable diagnostic string.
        include_logs: If True, include logs/crash/errors from disk
            (advanced mode). If False (default), only runtime system
            info is included — zero injection risk.

    Returns:
        Dictionary with structured fields for Claude Code consumption.
    """
    data: dict = {
        "format_version": 2,
        "app_version": __version__,
        "git_commit": _get_git_commit(),
        "python_version": platform.python_version(),
        "os": f"{platform.system()} {platform.release()}",
        "os_build": platform.version(),
        "frozen": _is_packaged_build(),
        "generated_at": datetime.now().isoformat(),
    }

    # Parse fields from diagnostic text
    profile_entries: list[str] = []
    for line in diagnostic.splitlines():
        stripped = line.strip("- ")
        if stripped.startswith("Mode:"):
            data["mode"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Profiles:"):
            data["profiles_summary"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Dependencies:"):
            data["dependencies"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Active tab:"):
            data["active_tab"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Install ID:"):
            data["install_id"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Screen:"):
            data["screen"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Memory usage:"):
            data["memory_mb"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Uptime:"):
            data["uptime"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Scheduler:"):
            data["scheduler"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Profile "):
            profile_entries.append(stripped)
    if profile_entries:
        data["profiles_detail"] = profile_entries

    # Report mode indicator
    data["report_mode"] = "advanced" if include_logs else "standard"

    if not include_logs:
        # Standard mode: system info only, zero disk data, zero risk.
        return data

    # --- Advanced mode: untrusted data (from disk files) ---
    # These are placed INSIDE the signed JSON block so they cannot
    # be modified after report generation. All content is anonymized
    # and sanitized before inclusion.
    appdata = os.environ.get("APPDATA", "")

    # Structured traceback from crash.log
    crash_file = Path(appdata) / "BackupManager" / "crash.log"
    structured_tb = _parse_traceback_structured(crash_file)
    if structured_tb:
        data["structured_traceback"] = structured_tb

    # Raw crash traceback (anonymized)
    crash_text = _extract_traceback_info(crash_file)
    if crash_text:
        crash_lines = crash_text.splitlines()
        safe_crash = []
        for line in crash_lines:
            safe_crash.append(_INJECTION_KEYWORDS.sub("[REMOVED]", line))
        data["crash_traceback"] = safe_crash

    # Recent errors from log file (anonymized, last 3)
    log_file = Path(appdata) / "BackupManager" / "logs" / "backup_manager.log"
    recent_errors = _extract_recent_errors(log_file, count=3)
    if recent_errors:
        error_lines = recent_errors.splitlines()
        safe_errors = []
        for line in error_lines:
            safe_errors.append(_INJECTION_KEYWORDS.sub("[REMOVED]", line))
        data["recent_errors"] = safe_errors

    # Recent log tail (anonymized, last 50 lines)
    if log_file.exists():
        try:
            raw_lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
            tail = raw_lines[-50:] if len(raw_lines) > 50 else raw_lines
            anon = anonymize_log_lines(tail)
            safe_log = []
            for line in anon:
                safe_log.append(_INJECTION_KEYWORDS.sub("[REMOVED]", line))
            data["recent_log"] = safe_log
        except OSError:
            data["recent_log"] = ["(could not read log file)"]

    return data


def _compute_source_hashes() -> dict[str, str]:
    """Compute SHA-256 hashes of key source files for cross-validation.

    When Claude receives this report, it can compare these hashes against
    the current source to verify the report matches the codebase version.

    Returns:
        Dictionary mapping relative file paths to their SHA-256 hashes.
    """
    # Determine source root. Packaged builds (PyInstaller + Nuitka)
    # do not ship source files next to the binary, so skip hashing.
    if _is_packaged_build():
        return {"note": "frozen build — source hashes not available"}

    src_root = Path(__file__).resolve().parent.parent
    project_root = src_root.parent

    critical_files = [
        "src/core/backup_engine.py",
        "src/core/config.py",
        "src/core/phases/writer.py",
        "src/core/phases/remote_writer.py",
        "src/core/phases/collector.py",
        "src/core/phases/filter.py",
        "src/core/phases/rotator.py",
        "src/storage/base.py",
        "src/storage/local.py",
        "src/storage/sftp.py",
        "src/storage/s3.py",
        "src/security/encryption.py",
        "src/core/scheduler.py",
        "src/notifications/email_notifier.py",
    ]

    hashes: dict[str, str] = {}
    for rel_path in critical_files:
        full_path = project_root / rel_path
        if full_path.exists():
            try:
                content = full_path.read_bytes()
                hashes[rel_path] = hashlib.sha256(content).hexdigest()
            except OSError:
                hashes[rel_path] = "read_error"
        else:
            hashes[rel_path] = "not_found"
    return hashes


def _compute_report_hmac(machine_json: str) -> str:
    """Compute HMAC-SHA256 of the machine-readable block.

    The key is derived from the app version + a fixed salt. This allows
    Claude to verify that the diagnostic data was generated by the app
    and not manually crafted or tampered with.

    Args:
        machine_json: JSON string of the machine-readable block.

    Returns:
        Hex-encoded HMAC-SHA256 signature.
    """
    # Key = SHA-256(version + salt). The salt is in the source code,
    # so Claude can recompute it to verify the signature.
    salt = b"BackupManager-BugReport-Integrity-v1"
    key_material = f"{__version__}".encode() + salt
    key = hashlib.sha256(key_material).digest()
    return hmac.new(key, machine_json.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_report_hmac(machine_json: str, expected_hmac: str) -> bool:
    """Verify the HMAC signature of a bug report's machine-readable block.

    This function is intended to be called by Claude Code when processing
    a bug report to confirm the diagnostic data has not been tampered with.

    Args:
        machine_json: The JSON string from the MACHINE READABLE section.
        expected_hmac: The HMAC-SHA256 hex string from the report.

    Returns:
        True if the signature is valid, False if tampered.
    """
    computed = _compute_report_hmac(machine_json)
    return hmac.compare_digest(computed, expected_hmac)


def verify_full_report_hmac(
    description: str, diagnostic: str, machine_json: str, expected_hmac: str
) -> bool:
    """Verify the full-report HMAC covering all sections.

    Unlike verify_report_hmac which only covers the machine-readable
    block, this verifies that the description, diagnostic text, AND
    machine JSON have not been modified after generation.

    Args:
        description: The USER DESCRIPTION section from the report.
        diagnostic: The diagnostic text section from the report.
        machine_json: The MACHINE READABLE JSON section.
        expected_hmac: The HMAC-FULL-SHA256 hex string from the report.

    Returns:
        True if the full report is intact, False if any section was tampered.
    """
    full_content = f"{description}\n{diagnostic}\n{machine_json}"
    computed = _compute_report_hmac(full_content)
    return hmac.compare_digest(computed, expected_hmac)


# -- Ed25519 asymmetric signing (proof of origin) --
# The public key is hardcoded here. The private key is ONLY in the
# compiled binary (assets/report_signing_key.pem, gitignored).
# An attacker with source code access CANNOT forge signatures.

_REPORT_PUBLIC_KEY_HEX = "1cb94d3db792886c883643da7c1a274624740216e0a27996617f5954ef173cb2"


def _load_signing_key():
    """Load the Ed25519 private key from the embedded asset.

    Returns:
        Ed25519PrivateKey instance, or None if not available.
    """
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    from src.__main__ import _get_base_dir

    key_path = _get_base_dir() / "assets" / "report_signing_key.pem"
    if not key_path.exists():
        return None
    try:
        pem_data = key_path.read_bytes()
        return load_pem_private_key(pem_data, password=None)
    except Exception:
        logger.warning("Could not load report signing key", exc_info=True)
        return None


def _sign_report_ed25519(content: str) -> str | None:
    """Sign report content with the embedded Ed25519 private key.

    Args:
        content: The full report content to sign.

    Returns:
        Hex-encoded Ed25519 signature, or None if key unavailable.
    """
    private_key = _load_signing_key()
    if private_key is None:
        return None
    try:
        signature = private_key.sign(content.encode("utf-8"))
        return signature.hex()
    except Exception:
        logger.warning("Failed to sign report", exc_info=True)
        return None


def verify_report_signature(content: str, signature_hex: str) -> bool:
    """Verify Ed25519 signature proving the report was generated by the app.

    This uses the hardcoded public key — it does NOT need the private key.
    If the signature is valid, the report was generated by a binary that
    contains the private key (i.e., an official build, not a forge).

    Args:
        content: The full report content that was signed.
        signature_hex: The hex-encoded Ed25519 signature.

    Returns:
        True if the signature is valid (report is from official app).
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PublicKey,
    )

    try:
        pub_bytes = bytes.fromhex(_REPORT_PUBLIC_KEY_HEX)
        public_key = Ed25519PublicKey.from_public_bytes(pub_bytes)
        signature = bytes.fromhex(signature_hex)
        public_key.verify(signature, content.encode("utf-8"))
        return True
    except Exception:
        return False


class BackupManagerApp:
    """Main application with sidebar profile list and tabbed configuration."""

    def __init__(self, root: tk.Tk, from_wizard: bool = False):
        import time

        self.root = root
        self._start_time = time.time()
        self._from_wizard = from_wizard
        self.root.title(f"{APP_TITLE} v{__version__}")
        self.root.geometry(WINDOW_SIZE)
        self.root.minsize(*MIN_SIZE)

        # Core components
        self.config_manager = ConfigManager()
        self.events = EventBus()
        self.engine: BackupEngine | None = None
        self._current_profile: BackupProfile | None = None
        # True while a backup thread is active. Read by _save_profile
        # to avoid mutating the shared BackupProfile instance that the
        # engine is currently pipelining — a concurrent UI save would
        # overwrite engine-managed fields (backup_type after auto-promotion,
        # profile_hash, retention reference) and corrupt pipeline decisions
        # like _phase_update_delta's FULL/DIFF branch.
        self._backup_running: bool = False

        # Setup theme
        self.style = setup_theme(root)

        # Setup scheduler
        self.scheduler = InAppScheduler(
            self.config_manager.config_dir,
            get_profiles=self.config_manager.get_all_profiles,
            backup_callback=self._scheduled_backup,
            config_manager=self.config_manager,
        )

        # Setup tray
        self.tray = BackupTray(
            show_callback=self._show_window,
            run_backup_callback=self._run_backup,
            quit_callback=self._quit_app,
        )

        # Load active mode from settings
        app_settings = self.config_manager.load_app_settings()
        self._current_mode = app_settings.get("mode", "classic")

        # Build UI
        self._build_ui()

        # Connect mode change callback
        self.tab_general.mode_var.set(self._current_mode)
        self.tab_general.mode_var.trace_add("write", self._on_mode_changed)

        self._load_profiles()
        if "last_verify" in app_settings:
            self.tab_verify.update_last_verify(app_settings["last_verify"])

        # After wizard: switch to Run tab, mark new profiles as
        # already triggered so the scheduler won't auto-run them
        if self._from_wizard:
            self.notebook.select(self.tab_run)
            self.scheduler.skip_startup_check = True
            from datetime import datetime

            for p in self.config_manager.get_all_profiles():
                self.scheduler._state.set_last_trigger(p.id, datetime.now())

        # Start services
        self.scheduler.start()
        self.tray.start()

        # Window close handler
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        # Subscribe to status events
        self.events.subscribe(STATUS, self._on_status_change)

        # Listen for single-instance "show me" message from second launch
        self._setup_single_instance_listener()

    def _build_ui(self):
        """Build the main window layout."""
        main = ttk.Frame(self.root)
        main.pack(fill="both", expand=True)
        self._main_frame = main

        # Sidebar
        self._build_sidebar(main)

        # Notebook (tabs)
        self._build_tabs(main)

        # Alert frame placeholder (shown when targets are unavailable)
        self._alert_frame: tk.Frame | None = None

    def _build_sidebar(self, parent):
        """Build the left sidebar with profile list."""
        sidebar = tk.Frame(parent, bg=Colors.SIDEBAR_BG, width=200)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)

        # App title
        tk.Label(
            sidebar,
            text="Backup\nManager",
            bg=Colors.SIDEBAR_BG,
            fg=Colors.SIDEBAR_TEXT,
            font=Fonts.title(),
        ).pack(pady=(Spacing.XLARGE, Spacing.SMALL))

        tk.Label(
            sidebar,
            text=f"v{__version__}",
            bg=Colors.SIDEBAR_BG,
            fg=Colors.TEXT_DISABLED,
            font=Fonts.small(),
        ).pack()

        import webbrowser

        link = tk.Label(
            sidebar,
            text="loicata.com",
            bg=Colors.SIDEBAR_BG,
            fg="#5dade2",
            font=Fonts.small(),
            cursor="hand2",
        )
        link.pack()
        link.bind(
            "<Button-1>",
            lambda e: webbrowser.open("https://loicata.com"),
        )

        # Profile listbox with section headers.
        # exportselection=0: without this, Tk clears the selection when
        # focus moves to another widget (changing tabs, clicking an
        # entry field). The visual deselection confuses users into
        # thinking no profile is loaded, and downstream code that reads
        # ``curselection()`` gets an empty tuple.
        self.profile_listbox = tk.Listbox(
            sidebar,
            bg=Colors.SIDEBAR_BG,
            fg=Colors.SIDEBAR_TEXT,
            selectbackground=Colors.SIDEBAR_ACTIVE,
            selectforeground="white",
            highlightthickness=0,
            borderwidth=0,
            font=Fonts.normal(),
            activestyle="none",
            exportselection=0,
        )
        self.profile_listbox.pack(
            fill="both",
            expand=True,
            padx=Spacing.MEDIUM,
            pady=Spacing.MEDIUM,
        )
        self.profile_listbox.bind("<<ListboxSelect>>", self._on_profile_selected)
        # Track which listbox indices are headers (non-selectable)
        self._header_indices: set[int] = set()
        self._listbox_profile_map: list[tuple[int, BackupProfile | None]] = []

        # Buttons
        btn_frame = tk.Frame(sidebar, bg=Colors.SIDEBAR_BG)
        btn_frame.pack(fill="x", padx=Spacing.MEDIUM, pady=Spacing.MEDIUM)

        tk.Button(
            btn_frame,
            text="New profile",
            bg=Colors.ACCENT,
            fg="white",
            activebackground=Colors.ACCENT_HOVER,
            activeforeground="white",
            relief="flat",
            font=Fonts.normal(),
            command=self._new_profile,
        ).pack(fill="x", pady=2)

        # Move buttons row
        move_frame = tk.Frame(btn_frame, bg=Colors.SIDEBAR_BG)
        move_frame.pack(fill="x", pady=2)

        tk.Button(
            move_frame,
            text="▲ Up",
            bg=Colors.SIDEBAR_HOVER,
            fg=Colors.SIDEBAR_TEXT,
            activebackground=Colors.SIDEBAR_BG,
            relief="flat",
            font=Fonts.small(),
            command=self._move_profile_up,
        ).pack(side="left", expand=True, fill="x", padx=(0, 1))

        tk.Button(
            move_frame,
            text="▼ Down",
            bg=Colors.SIDEBAR_HOVER,
            fg=Colors.SIDEBAR_TEXT,
            activebackground=Colors.SIDEBAR_BG,
            relief="flat",
            font=Fonts.small(),
            command=self._move_profile_down,
        ).pack(side="left", expand=True, fill="x", padx=(1, 0))

        tk.Button(
            btn_frame,
            text="Delete profile",
            bg=Colors.DANGER,
            fg="white",
            activebackground="#c0392b",
            activeforeground="white",
            relief="flat",
            font=Fonts.normal(),
            command=self._delete_profile,
        ).pack(fill="x", pady=2)

        # Bottom buttons
        bottom = tk.Frame(sidebar, bg=Colors.SIDEBAR_BG)
        bottom.pack(fill="x", padx=Spacing.MEDIUM, pady=Spacing.MEDIUM)

        tk.Button(
            bottom,
            text="About",
            bg=Colors.SIDEBAR_HOVER,
            fg=Colors.SIDEBAR_TEXT,
            activebackground=Colors.SIDEBAR_BG,
            relief="flat",
            font=Fonts.small(),
            command=self._show_about,
        ).pack(fill="x", pady=2)

    def _build_tabs(self, parent):
        """Build the right-side tabbed notebook."""
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill="both", expand=True)

        # Create tabs
        self.tab_run = RunTab(self.notebook, events=self.events)
        self.tab_general = GeneralTab(self.notebook)
        self.tab_storage = StorageTab(self.notebook)
        self.tab_mirror1 = MirrorTab(self.notebook, mirror_index=0)
        self.tab_mirror2 = MirrorTab(self.notebook, mirror_index=1)
        self.tab_retention = RetentionTab(self.notebook)
        self.tab_protection = ProtectionTab(self.notebook)
        self.tab_encryption = EncryptionTab(self.notebook)
        self.tab_schedule = ScheduleTab(self.notebook, scheduler=self.scheduler)
        self.tab_email = EmailTab(self.notebook)
        self.tab_recovery = RecoveryTab(self.notebook)
        self.tab_verify = VerifyTab(self.notebook, events=self.events)
        self.tab_history = HistoryTab(self.notebook)

        # Add tabs to notebook
        tabs = [
            (self.tab_run, "Run"),
            (self.tab_general, "General"),
            (self.tab_storage, "Storage"),
            (self.tab_mirror1, "Mirror 1"),
            (self.tab_mirror2, "Mirror 2"),
            (self.tab_encryption, "Encryption"),
            (self.tab_schedule, "Schedule"),
            (self.tab_retention, "Retention"),
            (self.tab_email, "Email"),
            (self.tab_recovery, "Recovery"),
            (self.tab_verify, "Verify"),
            (self.tab_history, "History"),
        ]
        for tab, label in tabs:
            self.notebook.add(tab, text=f" {label} ")

        # Wire the Retention and Schedule tabs into General so the one-shot
        # autoconfig (first Full->Diff per profile) can set both.
        self.tab_general.set_retention_tab(self.tab_retention)
        self.tab_general.set_schedule_tab(self.tab_schedule)

        # Connect run tab buttons
        self.tab_run.start_btn.config(command=self._run_backup)
        self.tab_run.cancel_btn.config(command=self._cancel_backup)

        # Connect verify tab buttons
        self.tab_verify.start_btn.config(command=self._run_verify)
        self.tab_verify.cancel_btn.config(command=self._cancel_verify)

        # Save button at bottom (hidden on Run and History tabs).
        # Packed before notebook so it reserves space at the bottom.
        self._save_frame = ttk.Frame(parent)
        self._save_frame.pack(fill="x", side="bottom", before=self.notebook)
        # Full-width bottom bar: Save is a critical action, so it must be
        # impossible to miss and cheap to click. Edge-docked buttons make
        # the user hunt for them.
        ttk.Button(
            self._save_frame,
            text="Save",
            style="Accent.TButton",
            command=self._save_profile,
        ).pack(fill="x", padx=Spacing.LARGE, pady=Spacing.MEDIUM)

        # Tabs where Save is not relevant
        self._no_save_tabs = {
            str(self.tab_run),
            str(self.tab_history),
            str(self.tab_recovery),
            str(self.tab_verify),
        }
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

    def _on_mode_changed(self, *_args: object) -> None:
        """Handle mode switch between Classic and Anti-Ransomware.

        Saves the new mode, reloads profiles filtered by mode,
        and launches the wizard if no profiles exist in the new mode.
        """
        new_mode = self.tab_general.mode_var.get()
        if new_mode == self._current_mode:
            return

        old_mode = self._current_mode
        self._current_mode = new_mode

        # Persist mode to app settings
        settings = self.config_manager.load_app_settings()
        settings["mode"] = new_mode
        self.config_manager.save_app_settings(settings)

        # Reload profiles filtered by new mode
        self._load_profiles()

        # If no profiles in this mode, launch the wizard
        if not self._profiles:
            from src.ui.wizard import MODE_PERSONAL, MODE_PROFESSIONAL, SetupWizard

            self.root.withdraw()
            wizard = SetupWizard(self.root, standalone=True)
            if new_mode == "anti-ransomware":
                wizard._select_mode(MODE_PROFESSIONAL)
            else:
                wizard._select_mode(MODE_PERSONAL)
            profile = wizard.run()
            self.root.deiconify()

            if profile:
                self.config_manager.save_profile(profile)
                self._load_profiles()

                # Switch to Run tab and prevent auto-backup
                self.notebook.select(self.tab_run)
                from datetime import datetime

                # Use the public scheduler API rather than poking at
                # the private ``_state`` attribute (SLF001 and fragile).
                self.scheduler.mark_triggered_now(profile.id, datetime.now())
            else:
                # Wizard cancelled — revert to previous mode
                self._current_mode = old_mode
                self.tab_general.mode_var.set(old_mode)
                settings["mode"] = old_mode
                self.config_manager.save_app_settings(settings)
                self._load_profiles()
                return

        # Select the first profile in the new mode
        for i, (_idx, p) in enumerate(self._listbox_profile_map):
            if p is not None:
                self.profile_listbox.selection_clear(0, "end")
                self.profile_listbox.selection_set(i)
                self._on_profile_selected(None)
                break

    def _on_tab_changed(self, event=None):
        """Show or hide the Save button depending on the active tab."""
        current = self.notebook.select()
        if current in self._no_save_tabs:
            self._save_frame.pack_forget()
        else:
            self._save_frame.pack(fill="x", side="bottom")

    # --- Profile management ---

    def _load_profiles(self):
        """Load profiles into the sidebar, filtered by the active mode."""
        self.profile_listbox.delete(0, "end")
        all_profiles = self.config_manager.get_all_profiles()
        self._all_profiles = all_profiles  # Keep unfiltered for mode switching

        # Filter by current mode
        is_anti_ran = self._current_mode == "anti-ransomware"
        self._profiles = [p for p in all_profiles if p.object_lock_enabled == is_anti_ran]
        self._header_indices = set()
        self._listbox_profile_map = []

        active = [p for p in self._profiles if p.active]
        inactive = [p for p in self._profiles if not p.active]

        idx = 0
        # Active header
        self.profile_listbox.insert("end", "ACTIVE PROFILES")
        self.profile_listbox.itemconfig(
            idx,
            fg=Colors.TEXT_DISABLED,
            selectbackground=Colors.SIDEBAR_BG,
            selectforeground=Colors.TEXT_DISABLED,
        )
        self._header_indices.add(idx)
        self._listbox_profile_map.append((idx, None))
        idx += 1

        for p in active:
            self.profile_listbox.insert("end", f"  {p.name}")
            self._listbox_profile_map.append((idx, p))
            idx += 1

        # Spacer + Inactive header
        self.profile_listbox.insert("end", "")
        self.profile_listbox.itemconfig(
            idx, selectbackground=Colors.SIDEBAR_BG, selectforeground=Colors.SIDEBAR_BG
        )
        self._header_indices.add(idx)
        self._listbox_profile_map.append((idx, None))
        idx += 1

        self.profile_listbox.insert("end", "INACTIVE PROFILES")
        self.profile_listbox.itemconfig(
            idx,
            fg=Colors.TEXT_DISABLED,
            selectbackground=Colors.SIDEBAR_BG,
            selectforeground=Colors.TEXT_DISABLED,
        )
        self._header_indices.add(idx)
        self._listbox_profile_map.append((idx, None))
        idx += 1

        for p in inactive:
            self.profile_listbox.insert("end", f"  {p.name}")
            self.profile_listbox.itemconfig(idx, fg="#888888")
            self._listbox_profile_map.append((idx, p))
            idx += 1

        # Select first active profile
        if active:
            first_active_idx = 1  # index 0 is header
            self.profile_listbox.select_set(first_active_idx)
            self._load_profile(active[0])

    def _on_profile_selected(self, event=None):
        sel = self.profile_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        # Skip headers
        if idx in self._header_indices:
            self.profile_listbox.selection_clear(idx)
            return

        # Find the profile for this index
        new_profile = None
        for map_idx, profile in self._listbox_profile_map:
            if map_idx == idx and profile is not None:
                new_profile = profile
                break
        if new_profile is None:
            return

        # Programmatic select_set() calls (e.g. from _load_profiles after a
        # _save_profile) fire <<ListboxSelect>> even when the target profile
        # id matches the current one. Skip the save-before-switch path in
        # that case — otherwise every _save_profile triggers a cascade
        # (save -> load_profiles -> select_set -> ListboxSelect -> save)
        # that can mutate the profile object while a backup is running.
        if self._current_profile and self._current_profile.id == new_profile.id:
            return

        # Save current profile before switching to a different one
        if self._current_profile:
            self._save_profile(silent=True)

        self._load_profile(new_profile)

    def _load_profile(self, profile: BackupProfile):
        """Load a profile into all tabs."""
        previous_id = self._current_profile.id if self._current_profile else None
        self._current_profile = profile

        self.tab_general.load_profile(profile)
        self.tab_storage.load_profile(profile)
        self.tab_mirror1.load_profile(profile)
        self.tab_mirror2.load_profile(profile)
        self.tab_encryption.load_profile(profile)
        self.tab_schedule.load_profile(profile)
        self.tab_email.load_profile(profile)
        self.tab_recovery.load_profile(profile)

        # Swap Retention ↔ Protection tab based on Object Lock mode
        self._update_retention_protection_tab(profile)

        self.tab_retention.load_profile(profile)
        self.tab_protection.load_profile(profile)

        self.tab_run.update_profile_info(
            profile.name,
            profile.backup_type.value,
            profile.last_backup,
            profile.last_full_backup or "",
        )
        # Only clear log when switching to a different profile
        if profile.id != previous_id:
            self.tab_run.clear_log()

        self._update_health_dashboard(profile)

    def _update_retention_protection_tab(self, profile: BackupProfile) -> None:
        """Show Protection tab or Retention tab based on profile mode.

        Object Lock profiles show the Protection tab (read-only).
        Standard profiles show the normal Retention tab with GFS config.

        Args:
            profile: Currently loaded profile.
        """
        try:
            if profile.object_lock_enabled:
                self.notebook.hide(self.tab_retention)
                # Add Protection tab at the position where Retention was
                try:
                    self.notebook.index(self.tab_protection)
                except tk.TclError:
                    # Not yet added — insert after Schedule
                    schedule_idx = self.notebook.index(self.tab_schedule)
                    self.notebook.insert(schedule_idx + 1, self.tab_protection, text=" Protection ")
            else:
                import contextlib

                with contextlib.suppress(tk.TclError):
                    self.notebook.hide(self.tab_protection)
                try:
                    self.notebook.index(self.tab_retention)
                except tk.TclError:
                    schedule_idx = self.notebook.index(self.tab_schedule)
                    self.notebook.insert(schedule_idx + 1, self.tab_retention, text=" Retention ")
        except tk.TclError:
            pass  # Tab manipulation during window setup

    def _update_health_dashboard(self, profile: BackupProfile) -> None:
        """Populate the Run tab health dashboard for a profile.

        Updates the 3 cards: Last backup, Next scheduled, Destinations.
        Destination checks run in background threads; results update
        the UI via after() callbacks.

        Args:
            profile: The currently selected profile.
        """
        # Card 1: Last backup — use journal timestamp (more accurate
        # than profile.last_backup which only updates on success)
        last_run = self.scheduler.journal.get_last_run(profile.id)
        files_count = 0
        success = True
        last_timestamp = profile.last_backup or ""
        if last_run:
            success = last_run.get("status") == "success"
            files_count = last_run.get("files_count", 0)
            last_timestamp = last_run.get("timestamp", last_timestamp)
        is_diff = profile.backup_type == BackupType.DIFFERENTIAL
        self.tab_run.update_last_backup_card(
            last_timestamp,
            files_count=files_count,
            success=success,
            is_differential=is_diff,
            last_full_backup=profile.last_full_backup or "",
            last_full_files_count=profile.last_full_files_count,
        )

        # Card 2: Next scheduled
        next_info = self.scheduler.get_next_run_info(profile)
        self.tab_run.update_next_scheduled_card(next_info)

        # Card 3: Destinations (with async checks)
        destinations = []
        try:
            profile.storage.validate()
            destinations.append(("Storage", profile.storage.storage_type.value))
        except ValueError:
            pass
        for i, mirror in enumerate(profile.mirror_destinations):
            try:
                mirror.validate()
                destinations.append(
                    (f"Mirror {i + 1}", mirror.storage_type.value),
                )
            except ValueError:
                pass

        self.tab_run.update_destinations_card(destinations)

        # Track destination configs for continuous health polling
        self._health_configs: dict[int, tuple[StorageConfig, str]] = {}
        try:
            profile.storage.validate()
            self._health_configs[0] = (profile.storage, "Storage")
        except ValueError:
            pass
        for i, mirror in enumerate(profile.mirror_destinations):
            try:
                mirror.validate()
                self._health_configs[i + 1] = (mirror, f"Mirror {i + 1}")
            except ValueError:
                pass

        # Bump generation to cancel polling from previous profile
        self._health_poll_generation = getattr(self, "_health_poll_generation", 0) + 1

        if destinations:
            check_destinations_async(
                profile.storage,
                profile.mirror_destinations,
                callback=self._on_health_result,
            )
            # Schedule continuous polling every 30s
            self.root.after(
                HEALTH_POLL_INTERVAL_MS,
                self._poll_health,
                self._health_poll_generation,
            )

    def _on_health_result(self, index: int, health) -> None:
        """Callback from health check thread — schedule UI update.

        Args:
            index: Destination index (0=storage, 1+=mirrors).
            health: DestinationHealth result.
        """

        with contextlib.suppress(Exception):
            self.tab_run.after(
                0,
                self.tab_run.update_destination_status,
                index,
                health,
            )

    def _poll_health(self, generation: int) -> None:
        """Re-check all destinations periodically.

        Stops if the profile changed (generation mismatch) or no
        destinations are configured.

        Args:
            generation: Poll generation to detect profile changes.
        """
        if generation != getattr(self, "_health_poll_generation", -1):
            return
        if not getattr(self, "_health_configs", {}):
            return

        for index, (config, label) in self._health_configs.items():
            threading.Thread(
                target=self._check_single_destination,
                args=(index, config, label),
                daemon=True,
                name=f"HealthPoll-{label}",
            ).start()

        # Schedule next poll
        self.root.after(
            HEALTH_POLL_INTERVAL_MS,
            self._poll_health,
            generation,
        )

    def _check_single_destination(self, index: int, config: "StorageConfig", label: str) -> None:
        """Check one destination and report result via callback.

        Args:
            index: Destination index (0=storage, 1+=mirrors).
            config: Storage configuration.
            label: Display label.
        """
        from src.core.health_checker import _check_destination

        result = _check_destination(config, label)
        self._on_health_result(index, result)

    def _save_profile(self, silent: bool = False) -> bool:
        """Collect config from all tabs and save.

        Args:
            silent: If True, suppress the "Saved" confirmation dialog.

        Returns:
            True if the profile was saved successfully, False otherwise.
        """
        if not self._current_profile:
            return False

        # Never mutate the profile while a backup is running — the engine
        # holds the same instance and relies on a stable view of fields
        # like backup_type, retention and profile_hash. Silent saves (auto)
        # are dropped; explicit saves warn the user.
        if self._backup_running:
            if silent:
                return True
            messagebox.showwarning(
                "Backup in progress",
                "A backup is currently running. Profile changes will be "
                "available after it completes.",
            )
            return False

        # Validate encryption
        enc_error = self.tab_encryption.validate()
        if enc_error:
            self.notebook.select(self.tab_encryption)
            messagebox.showwarning("Validation", enc_error)
            return False

        profile = self._current_profile

        # Collect from all tabs
        general = self.tab_general.collect_config()

        # Validate profile name uniqueness
        new_name = general["name"]
        for p in self._profiles:
            if p.id != profile.id and p.name.lower() == new_name.lower():
                self.notebook.select(self.tab_general)
                messagebox.showwarning(
                    "Validation",
                    f"A profile named '{p.name}' already exists. "
                    "Please choose a different name.",
                )
                return False

        profile.name = new_name
        profile.backup_type = general["backup_type"]
        profile.full_backup_every = general["full_backup_every"]
        profile.source_paths = general["source_paths"]
        profile.exclude_patterns = general["exclude_patterns"]
        profile.bandwidth_percent = general["bandwidth_percent"]

        storage = self.tab_storage.collect_config()
        profile.storage = storage["storage"]

        mirrors = []
        for tab in (self.tab_mirror1, self.tab_mirror2):
            m = tab.collect_config()
            if m is not None:
                mirrors.append(m)
        profile.mirror_destinations = mirrors

        # Check for duplicate destinations
        dup_error = self._check_duplicate_destinations(storage["storage"], mirrors)
        if dup_error:
            self.notebook.select(self.tab_storage)
            messagebox.showwarning("Validation", dup_error)
            return False

        retention = self.tab_retention.collect_config()
        profile.retention = retention["retention"]

        # Validate differential full-backup cycle (skip for Object Lock profiles
        # where GFS rotation is disabled — S3 Lifecycle handles cleanup)
        if general["backup_type"] == BackupType.DIFFERENTIAL and profile.retention.gfs_enabled:
            cycle = general["full_backup_every"]
            gfs_d = profile.retention.gfs_daily

            if cycle > gfs_d:
                self.notebook.select(self.tab_general)
                messagebox.showwarning(
                    "Validation",
                    f"Full backup cycle ({cycle}) must not exceed "
                    f"daily retention ({gfs_d}).\n\n"
                    f"Otherwise the daily rotation could delete the "
                    f"full backup before the next one is created.",
                )
                return False

        encryption = self.tab_encryption.collect_config()
        profile.encrypt_primary = encryption["encrypt_primary"]
        profile.encrypt_mirror1 = encryption["encrypt_mirror1"]
        profile.encrypt_mirror2 = encryption["encrypt_mirror2"]
        profile.encryption = encryption["encryption"]

        schedule = self.tab_schedule.collect_config()
        sched_cfg = schedule["schedule"]
        # Retry enabled comes from general tab
        sched_cfg.retry_enabled = general["retry_enabled"]
        profile.schedule = sched_cfg

        email = self.tab_email.collect_config()
        profile.email = email["email"]

        self.config_manager.save_profile(profile)

        # Immediate user feedback BEFORE the secondary work (registry
        # write + profile-list rebuild) — those take 100-300 ms on a
        # laptop and the user reads the popup as "the click did nothing"
        # when they happen first. The data is safely on disk at this
        # point, so the popup is truthful.
        if not silent:
            messagebox.showinfo("Saved", f"Profile '{profile.name}' saved.")

        # Apply auto-start setting
        if general["autostart"]:
            show_window = not general["autostart_minimized"]
            AutoStart.ensure_startup(show_window=show_window)
        else:
            AutoStart.disable()

        self._load_profiles()
        # Saving a profile invalidates any outstanding target-precheck
        # alert (e.g. a mirror that the user just deleted).  Drop the
        # stale alert rather than leaving it visible over the tabs.
        self._hide_target_alert()
        return True

    @staticmethod
    def _check_duplicate_destinations(storage, mirrors) -> str:
        """Check that storage and mirrors don't point to the same destination.

        Args:
            storage: Primary StorageConfig.
            mirrors: List of mirror StorageConfig.

        Returns:
            Error message if duplicates found, empty string if OK.
        """
        from src.core.config import StorageType

        def _destination_key(config) -> str:
            """Build a unique key for a destination."""
            st = config.storage_type
            if st in (StorageType.LOCAL, StorageType.NETWORK):
                return f"{st.value}:{config.destination_path.rstrip('/').rstrip(chr(92)).lower()}"
            if st == StorageType.SFTP:
                return (
                    f"sftp:{config.sftp_host}:{config.sftp_port}"
                    f":{config.sftp_remote_path.rstrip('/')}"
                )
            if st == StorageType.S3:
                return f"s3:{config.s3_bucket}:{config.s3_prefix.strip('/')}"
            return ""

        targets = [("Storage", storage)]
        for i, m in enumerate(mirrors):
            targets.append((f"Mirror {i + 1}", m))

        seen: dict[str, str] = {}
        for name, config in targets:
            key = _destination_key(config)
            if not key:
                continue
            if key in seen:
                return (
                    f"{name} and {seen[key]} point to the same destination. "
                    f"Each destination must be unique."
                )
            seen[key] = name

        return ""

    def _new_profile(self):
        profile = BackupProfile()
        self.config_manager.save_profile(profile)
        self._load_profiles()
        # Select only the new profile
        self.profile_listbox.selection_clear(0, "end")
        for map_idx, p in self._listbox_profile_map:
            if p is not None and p.id == profile.id:
                self.profile_listbox.select_set(map_idx)
                self._load_profile(p)
                break

    def _delete_profile(self):
        if not self._current_profile:
            return
        profile = self._current_profile
        name = profile.name

        if not messagebox.askyesno("Delete", f"Delete profile '{name}'?"):
            return

        if profile.object_lock_enabled:
            # Object Lock profiles: backups cannot be deleted
            messagebox.showinfo(
                "Object Lock",
                f"Profile '{name}' will be removed.\n\n"
                "Backups on Amazon AWS S3 are protected by Object Lock "
                "and cannot be deleted. They will expire automatically "
                "at the end of the retention period.",
            )
            self._finalize_profile_deletion(profile)
        else:
            delete_backups = messagebox.askyesno(
                "Delete backups",
                f"Also delete all backups created by '{name}'?\n\n"
                "This will remove backups from all destinations "
                "(primary storage, mirrors).\n\n"
                "This cannot be undone.",
            )

            if delete_backups:
                self._delete_profile_backups_async(profile)
            else:
                self._finalize_profile_deletion(profile)

    def _finalize_profile_deletion(self, profile: BackupProfile) -> None:
        """Remove profile config and refresh the UI.

        When the deleted profile was the last one on disk, relaunch the
        setup wizard exactly the way the cold start does — an empty app
        with no way to create a profile is a dead end for the user.
        """
        self.config_manager.delete_profile(profile.id)
        self._current_profile = None
        self._load_profiles()
        if self._current_profile is None:
            self._clear_tabs()

        if not self.config_manager.get_all_profiles():
            self._relaunch_wizard_after_delete()

    def _relaunch_wizard_after_delete(self) -> None:
        """Launch the setup wizard modal when no profiles remain.

        Mirrors the cold-start path in ``__main__.py``: hide the main
        window, let the user pick Personal vs Professional, then reload
        the sidebar with whatever came out. On cancel, nothing happens —
        the main window reappears empty and the sidebar ``New profile``
        button stays reachable.
        """
        from src.ui.wizard import SetupWizard

        self.root.withdraw()
        try:
            wizard = SetupWizard(self.root, standalone=True)
            profile = wizard.run()
        finally:
            self.root.deiconify()

        if profile is None:
            return
        self.config_manager.save_profile(profile)
        # Sync the app's mode to the profile we just created so the
        # sidebar and General tab reflect it.
        self._current_mode = "anti-ransomware" if profile.object_lock_enabled else "classic"
        self.tab_general.mode_var.set(self._current_mode)
        self._load_profiles()
        # Prevent the scheduler from auto-firing the brand new profile
        # on the very next tick — the user probably wants a review pass
        # first, exactly like after the first-launch wizard.
        from datetime import datetime

        self.scheduler.mark_triggered_now(profile.id, datetime.now())

    def _delete_profile_backups_async(self, profile: BackupProfile) -> None:
        """Delete all backups for a profile in background, then remove config."""
        from src.core.backup_engine import delete_profile_backups

        configs = [profile.storage] + list(profile.mirror_destinations)
        configs = [c for c in configs if c.destination_path or c.sftp_host or c.s3_bucket]

        result: list = [None]

        def _do_delete():
            result[0] = delete_profile_backups(profile.name, configs)

        def _poll():
            if result[0] is None:
                self.root.after(200, _poll)
                return
            deleted, errors = result[0]
            for err in errors:
                logger.warning("Backup deletion error: %s", err)
            if errors:
                messagebox.showwarning(
                    "Partial cleanup",
                    f"Deleted {deleted} backup(s) but {len(errors)} "
                    f"error(s) occurred.\nCheck logs for details.",
                )
            self._finalize_profile_deletion(profile)

        threading.Thread(target=_do_delete, daemon=True, name="DeleteBackups").start()
        self.root.after(200, _poll)

    def _clear_tabs(self):
        """Reset all tabs to empty/default state after profile deletion."""
        blank = BackupProfile()
        self.tab_general.load_profile(blank)
        self.tab_storage.load_profile(blank)
        self.tab_mirror1.load_profile(blank)
        self.tab_mirror2.load_profile(blank)
        self.tab_retention.load_profile(blank)
        self.tab_encryption.load_profile(blank)
        self.tab_schedule.load_profile(blank)
        self.tab_email.load_profile(blank)
        self.tab_history.load_profile(blank)
        self.tab_recovery.load_profile(blank)

    def _get_selected_profile(self):
        """Get the currently selected profile and its listbox index.

        Tk listboxes with the default ``exportselection=1`` clear their
        selection when focus moves to another widget (tab change, entry
        field click, etc.) so ``curselection()`` can return empty even
        when the user still has a profile loaded. ``self._current_profile``
        is maintained by ``_load_profile`` and stays accurate regardless,
        so it is the real source of truth; the listbox index is only
        needed by the up/down move buttons.
        """
        if self._current_profile is not None:
            for map_idx, profile in self._listbox_profile_map:
                if profile is not None and profile.id == self._current_profile.id:
                    return self._current_profile, map_idx
        # Fallback: no current profile — honour the visible selection.
        sel = self.profile_listbox.curselection()
        if not sel:
            return None, None
        idx = sel[0]
        if idx in self._header_indices:
            return None, None
        for map_idx, profile in self._listbox_profile_map:
            if map_idx == idx and profile is not None:
                return profile, idx
        return None, None

    def _move_profile_up(self):
        """Move selected profile up, or from inactive to active."""
        profile, idx = self._get_selected_profile()
        if profile is None:
            return

        active_profiles = [p for p in self._profiles if p.active]
        inactive_profiles = [p for p in self._profiles if not p.active]

        if profile.active:
            # Already active — move up within active list
            pos = active_profiles.index(profile)
            if pos == 0:
                return  # Already at top
            # Swap sort_order with the profile above
            other = active_profiles[pos - 1]
            profile.sort_order, other.sort_order = other.sort_order, profile.sort_order
            self.config_manager.save_profile(profile)
            self.config_manager.save_profile(other)
        else:
            # Inactive — first position: move to active
            pos = inactive_profiles.index(profile)
            if pos == 0:
                # Move to active (bottom of active list)
                profile.active = True
                if active_profiles:
                    profile.sort_order = max(p.sort_order for p in active_profiles) + 1
                else:
                    profile.sort_order = 0
                self.config_manager.save_profile(profile)
            else:
                # Move up within inactive list
                other = inactive_profiles[pos - 1]
                profile.sort_order, other.sort_order = other.sort_order, profile.sort_order
                self.config_manager.save_profile(profile)
                self.config_manager.save_profile(other)

        self._load_profiles()
        self._reselect_profile(profile)

    def _move_profile_down(self):
        """Move selected profile down, or from active to inactive."""
        profile, idx = self._get_selected_profile()
        if profile is None:
            return

        active_profiles = [p for p in self._profiles if p.active]
        inactive_profiles = [p for p in self._profiles if not p.active]

        if profile.active:
            pos = active_profiles.index(profile)
            if pos >= len(active_profiles) - 1:
                # Last active — move to inactive
                profile.active = False
                if inactive_profiles:
                    profile.sort_order = min(p.sort_order for p in inactive_profiles) - 1
                else:
                    profile.sort_order = 0
                self.config_manager.save_profile(profile)
            else:
                # Move down within active list
                other = active_profiles[pos + 1]
                profile.sort_order, other.sort_order = other.sort_order, profile.sort_order
                self.config_manager.save_profile(profile)
                self.config_manager.save_profile(other)
        else:
            # Inactive — move down within inactive list
            pos = inactive_profiles.index(profile)
            if pos >= len(inactive_profiles) - 1:
                return  # Already at bottom
            other = inactive_profiles[pos + 1]
            profile.sort_order, other.sort_order = other.sort_order, profile.sort_order
            self.config_manager.save_profile(profile)
            self.config_manager.save_profile(other)

        self._load_profiles()
        self._reselect_profile(profile)

    def _reselect_profile(self, profile: BackupProfile):
        """Re-select a profile in the listbox after reload."""
        self.profile_listbox.selection_clear(0, "end")
        for map_idx, p in self._listbox_profile_map:
            if p is not None and p.id == profile.id:
                self.profile_listbox.select_set(map_idx)
                self._load_profile(p)
                return

    # --- Backup execution ---

    def _run_backup(self):
        if not self._current_profile:
            messagebox.showwarning("Backup", "No profile selected.")
            return

        # Save current UI state before running (validates config)
        if not self._save_profile(silent=True):
            return

        profile = self._current_profile

        # Validate config before attempting connectivity check
        try:
            profile.storage.validate()
        except ValueError as e:
            self.notebook.select(self.tab_storage)
            messagebox.showwarning("Backup", f"Invalid configuration: {e}")
            return

        mirror_tabs = [self.tab_mirror1, self.tab_mirror2]
        for i, mirror in enumerate(profile.mirror_destinations):
            try:
                mirror.validate()
            except ValueError as e:
                self.notebook.select(mirror_tabs[i])
                messagebox.showwarning("Backup", f"Invalid configuration: {e}")
                return

        self.engine = BackupEngine(self.config_manager, events=self.events)

        # Pre-check targets in background, then start backup if all OK
        self._precheck_and_run(profile)

    def _precheck_and_run(self, profile: BackupProfile, _retry_attempt: int = 0) -> None:
        """Run target pre-check in background thread, then start backup.

        Shows a "Checking destinations..." message immediately so the user
        knows something is happening (SFTP timeouts can take 15+ seconds).
        Uses polling pattern (root.after) to stay thread-safe with tkinter.

        On failure, retries the precheck ONCE silently before surfacing
        the "Destinations unavailable" popup. Pairs with the extended
        wake-up budget in ``LocalStorage.test_connection`` to give a USB
        drive two full wake-up rounds before telling the user something
        is wrong — the first round may have nudged the drive out of deep
        sleep without fully re-enumerating in time.
        """
        if _retry_attempt == 0:
            # Only show the "Checking..." overlay on the first attempt;
            # the silent retry keeps the UI quiet so a transparent
            # second chance doesn't look like a stutter.
            self._show_checking_message()

        result: list = [None]  # [None] = pending, [list] = done

        def _do_check() -> None:
            result[0] = self.engine.precheck_targets(profile)

        def _poll() -> None:
            if result[0] is None:
                self.root.after(200, _poll)
                return

            failures = [r for r in result[0] if not r[2]]
            if not failures:
                self._hide_target_alert()  # Remove "Checking..." message
                self._start_backup_thread(profile)
                return

            # One silent retry before showing the popup. Half a second
            # of pause lets Windows finish mounting a drive that the
            # first precheck already started waking up.
            if _retry_attempt == 0:
                self.root.after(
                    500,
                    lambda: self._precheck_and_run(profile, _retry_attempt=1),
                )
                return

            self._hide_target_alert()
            # Check if primary storage is OK (only mirrors failed)
            primary_ok = all(r[2] for r in result[0] if r[0] == "Storage")
            # The lambda wraps the whole ternary so on_continue is
            # None when primary is down (no safe fallback).  The
            # previous form wrapped only the true-branch, making
            # on_continue always truthy and rendering a no-op
            # "Continue without mirror" button in every case.
            on_continue = (lambda: self._on_precheck_continue(profile)) if primary_ok else None
            self._show_target_alert(
                failures,
                on_retry=lambda: self._on_precheck_retry(profile),
                on_cancel=lambda: self._on_precheck_cancel(),
                on_continue=on_continue,
            )

        threading.Thread(target=_do_check, daemon=True, name="Precheck").start()
        self.root.after(200, _poll)

    def _show_checking_message(self) -> None:
        """Show a 'Checking destinations...' message while precheck runs."""
        self._hide_target_alert()
        self.notebook.pack_forget()

        frame = tk.Frame(self._main_frame, bg=Colors.CARD_BG)
        frame.pack(fill="both", expand=True)
        self._alert_frame = frame

        content = tk.Frame(frame, bg=Colors.CARD_BG)
        content.pack(expand=True)

        tk.Label(
            content,
            text="Checking destinations...",
            font=(Fonts.FAMILY, Fonts.SIZE_HEADER),
            fg=Colors.ACCENT,
            bg=Colors.CARD_BG,
        ).pack(pady=(0, 10))

        tk.Label(
            content,
            text="Verifying that all backup targets are reachable.",
            font=(Fonts.FAMILY, Fonts.SIZE_NORMAL),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.CARD_BG,
        ).pack()

    def _on_precheck_retry(self, profile: BackupProfile) -> None:
        """User clicked Retry — hide alert and re-run precheck."""
        self._hide_target_alert()
        self._precheck_and_run(profile)

    def _on_precheck_continue(self, profile: BackupProfile) -> None:
        """User clicked Continue without mirror — run backup anyway."""
        self._hide_target_alert()
        self._start_backup_thread(profile)

    def _on_precheck_cancel(self) -> None:
        """User clicked Cancel — hide alert, set tray to error."""
        self._hide_target_alert()
        self.tray.set_state(TrayState.BACKUP_ERROR)

    def _start_backup_thread(self, profile: BackupProfile) -> None:
        """Start the actual backup in a background thread."""
        self.tab_run.clear_log()
        self.tab_run._append_log(f"Backup started — {profile.name}")

        # Raise the running flag BEFORE spawning the thread so any UI save
        # queued between this point and the engine's _maybe_force_full sees
        # the flag and skips. Reset in the thread's finally block.
        self._backup_running = True

        def _backup_thread():
            from datetime import datetime

            from src.core.scheduler import ScheduleLogEntry

            # Log manual backup start in the schedule journal
            with self.scheduler.op_lock:
                self.scheduler.journal.add(
                    ScheduleLogEntry(
                        profile_id=profile.id,
                        profile_name=profile.name,
                        trigger="manual",
                        status="started",
                    )
                )

            try:
                self.tray.set_state(TrayState.BACKUP_RUNNING)
                stats = self.engine.run_backup(profile)
                self.tray.set_state(TrayState.BACKUP_SUCCESS)
                self.tray.notify(
                    "Backup complete",
                    f"{stats.files_processed} files in {stats.duration_seconds:.0f}s",
                )

                completed_at = datetime.now().isoformat()
                with self.scheduler.op_lock:
                    self.scheduler.journal.update_last(
                        status="success",
                        files_count=stats.files_processed,
                        duration_seconds=stats.duration_seconds,
                        timestamp=completed_at,
                    )

                # Save backup log to file
                self._save_backup_log(profile, stats)

                # Update last_backup
                profile.last_backup = completed_at
                self.config_manager.save_profile(profile)

                # Refresh dashboard to show updated last backup info
                self.root.after(0, self._update_health_dashboard, profile)
                # Refresh the Run tab header so an auto-promoted run is
                # annotated as "differential — last run: full (auto-promoted)"
                # rather than leaving the transient "full (auto-promoted)"
                # label from BACKUP_TYPE_DETERMINED.
                self.root.after(0, self._refresh_run_header, profile)

                # Send email notification
                if profile.email.enabled:
                    self._send_backup_email(
                        profile,
                        True,
                        stats,
                        f"{stats.files_processed} files backed up",
                    )

            except CancelledError:
                self.tray.set_state(TrayState.IDLE)
                self.tray.notify(
                    "Backup cancelled",
                    f"[{profile.name}] Cancelled by user",
                )
                result = self.engine._current_result if self.engine else None
                with self.scheduler.op_lock:
                    self.scheduler.journal.update_last(status="cancelled")
                if result:
                    self._save_backup_log(profile, result)
                if profile.email.enabled:
                    self._send_backup_email(
                        profile,
                        False,
                        result,
                        "Backup cancelled by user",
                        cancelled=True,
                    )
            except Exception as e:
                self.tray.set_state(TrayState.BACKUP_ERROR)
                self.tray.notify("Backup failed", str(e))
                result = self.engine._current_result if self.engine else None
                with self.scheduler.op_lock:
                    self.scheduler.journal.update_last(
                        status="failed",
                        detail=f"{type(e).__name__}: {e}",
                    )
                if result:
                    self._save_backup_log(profile, result)
                if profile.email.enabled:
                    self._send_backup_email(
                        profile,
                        False,
                        result,
                        str(e),
                    )
            finally:
                # Lower the flag so the next Save collects UI edits made
                # during the backup (and warnings stop firing).
                self._backup_running = False
                # Reset the Run tab header so any transient
                # "full (auto-promoted)" override from BACKUP_TYPE_DETERMINED
                # is replaced by the canonical profile-derived label
                # (even on cancel/error paths).
                self.root.after(0, self._refresh_run_header, profile)

        threading.Thread(target=_backup_thread, daemon=True, name="Backup").start()

    def _cancel_backup(self):
        if self.engine:
            self.tab_run._append_log("Cancelling backup...")
            self.engine.cancel()

    # --- Integrity Verification ---

    _verifier = None

    def _run_verify(self):
        """Start integrity verification for the current profile."""
        profile, _idx = self._get_selected_profile()
        if profile is None:
            from tkinter import messagebox

            messagebox.showwarning(
                "No profile selected",
                "Please select a profile in the sidebar before verifying.",
            )
            return

        from src.core.integrity_verifier import IntegrityVerifier

        self.tab_verify.clear()
        self.tab_verify.set_running(True)

        self._verifier = IntegrityVerifier(profile, self.config_manager, events=None)

        def _verify_thread():
            try:
                for idx, bvr in enumerate(self._verifier.verify_iter(), start=1):
                    # ``total_backups`` is populated by the verifier as it
                    # finishes listing each destination; by the time we
                    # receive the first result past the listing phase
                    # it's stable and we can compute a percentage.
                    total = self._verifier._result.total_backups or idx
                    self.root.after(0, self._on_verify_result, bvr, idx, total)
                result = self._verifier.get_result()
                self.root.after(0, self._on_verify_done, result)
            except Exception as e:
                logger.exception("Verification failed: %s", e)
                self.root.after(0, self._on_verify_error, str(e))

        import threading

        thread = threading.Thread(target=_verify_thread, daemon=True, name="IntegrityVerifier")
        thread.start()

    def _cancel_verify(self):
        """Cancel a running verification."""
        if self._verifier:
            self._verifier.cancel()
            self.tab_verify.set_running(False)
            self.tab_verify.status_label.config(text="Cancelled", foreground=Colors.DANGER)

    def _on_verify_result(self, bvr, checked: int = 0, total: int = 0):
        """Handle a single verification result on the main thread."""
        self.tab_verify.add_result(
            bvr.destination,
            bvr.backup_name,
            bvr.status,
            bvr.message,
            checked=checked,
            total=total,
        )

    def _on_verify_done(self, result):
        """Handle verification completion on the main thread."""
        from datetime import datetime

        self.tab_verify.set_complete(
            result.ok_count,
            result.error_count,
            result.duration_seconds,
            warning_count=result.warning_count,
        )

        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        self.tab_verify.update_last_verify(ts)

        # Persist last verify timestamp
        settings = self.config_manager.load_app_settings()
        settings["last_verify"] = ts
        self.config_manager.save_app_settings(settings)

        # Send email report if configured
        profile, _idx = self._get_selected_profile()
        if profile and profile.email.enabled:
            self._send_verify_email(profile, result)

        self._verifier = None

    def _on_verify_error(self, error_msg: str):
        """Handle verification error on the main thread."""
        self.tab_verify.set_running(False)
        self.tab_verify.add_result("—", "—", "error", error_msg)
        self._verifier = None

    def _send_backup_email(
        self,
        profile: BackupProfile,
        success: bool,
        result,
        summary: str,
        cancelled: bool = False,
    ) -> None:
        """Send backup report email with enriched metrics.

        Args:
            profile: Backup profile.
            success: Whether backup succeeded.
            result: BackupResult (may be None on early failure).
            summary: Short summary text.
            cancelled: Whether the backup was cancelled.
        """
        try:
            from src.core.integrity_verifier import _build_backend
            from src.notifications.email_notifier import send_backup_report

            details = ""
            free_space = None
            if result:
                details = "\n".join(result.log_lines)
                # Try to get remaining disk space on primary destination
                try:
                    backend = _build_backend(profile.storage)
                    free_space = backend.get_free_space()
                except Exception:
                    logger.debug("Could not get free space for email report", exc_info=True)

            send_backup_report(
                profile.email,
                profile.name,
                success,
                summary,
                details=details,
                cancelled=cancelled,
                result=result,
                backup_type=(
                    result.actual_backup_type
                    if result and result.actual_backup_type
                    else profile.backup_type.value.upper()
                ),
                free_space=free_space,
            )
        except Exception as e:
            logger.warning("Could not send backup report: %s", e)

    def _refresh_run_header(self, profile: BackupProfile) -> None:
        """Re-render the Run tab header from profile state.

        Called after every backup (success/cancel/error) so any transient
        override set by the ``BACKUP_TYPE_DETERMINED`` event is replaced
        with the canonical profile-derived view (including the
        "last run: full (auto-promoted)" annotation when applicable).
        """
        self.tab_run.update_profile_info(
            profile.name,
            profile.backup_type.value,
            profile.last_backup or "",
            profile.last_full_backup or "",
        )

    def _save_backup_log(self, profile: BackupProfile, result) -> None:
        """Write backup log lines to a timestamped file.

        Args:
            profile: Backup profile.
            result: BackupResult with log_lines.
        """
        try:
            log_path = self.config_manager.get_log_path(profile.id)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            header = f"Starting backup '{profile.name}'\n"
            log_path.write_text(
                header + "\n".join(result.log_lines),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning("Could not save backup log: %s", e)

    def _send_verify_email(self, profile, result):
        """Send verification report email if configured."""
        try:
            from src.notifications.email_notifier import send_verify_report

            send_verify_report(profile.email, profile.name, result)
        except Exception as e:
            logger.warning("Could not send verify report: %s", e)

    def _scheduled_backup(self, profile: BackupProfile):
        """Callback for scheduler-triggered backups.

        Runs in the scheduler daemon thread. Pre-checks targets
        and shows alert on the main thread if any are unavailable.

        Raises:
            RuntimeError: If targets are unavailable and user cancels,
                or if the backup itself fails.
        """
        from datetime import datetime

        # Skip unconfigured profiles (default storage has empty destination)
        try:
            profile.storage.validate()
            for mirror in profile.mirror_destinations:
                mirror.validate()
        except ValueError as e:
            logger.warning("Skipping scheduled backup for '%s': %s", profile.name, e)
            return

        self.engine = BackupEngine(self.config_manager, events=self.events)

        # Pre-check targets (blocking — we're in the scheduler thread)
        results = self.engine.precheck_targets(profile)
        failures = [r for r in results if not r[2]]

        if failures:
            # Show alert on main thread and wait for user decision
            user_choice = self._scheduled_precheck_prompt(failures, profile)
            if user_choice == "cancel":
                self.tray.set_state(TrayState.BACKUP_ERROR)
                raise RuntimeError("Backup cancelled: destinations unavailable")

        # Scheduler owns its own profile instance (freshly loaded from disk)
        # so UI saves cannot mutate it. Raise the flag anyway so a concurrent
        # UI save cannot overwrite the JSON on disk while the scheduler is
        # mid-pipeline.
        self._backup_running = True
        try:
            self.tray.set_state(TrayState.BACKUP_RUNNING)
            stats = self.engine.run_backup(profile)
            self.tray.set_state(TrayState.BACKUP_SUCCESS)
            self.tray.notify(
                "Scheduled backup complete",
                f"[{profile.name}] {stats.files_processed} files "
                f"in {stats.duration_seconds:.0f}s",
            )
            completed_at = datetime.now().isoformat()
            with self.scheduler.op_lock:
                self.scheduler.journal.update_last(
                    status="success",
                    files_count=stats.files_processed,
                    duration_seconds=stats.duration_seconds,
                    timestamp=completed_at,
                )

            self._save_backup_log(profile, stats)

            # Update last_backup and refresh dashboard
            profile.last_backup = completed_at
            self.config_manager.save_profile(profile)
            self.root.after(0, self._update_health_dashboard, profile)

            if profile.email.enabled:
                self._send_backup_email(
                    profile,
                    True,
                    stats,
                    f"{stats.files_processed} files backed up",
                )

        except CancelledError:
            self.tray.set_state(TrayState.IDLE)
            self.tray.notify(
                "Scheduled backup cancelled",
                f"[{profile.name}] Cancelled by user",
            )
            if profile.email.enabled:
                result = self.engine._current_result if self.engine else None
                self._send_backup_email(
                    profile,
                    False,
                    result,
                    "Backup cancelled by user",
                    cancelled=True,
                )

        except Exception as e:
            self.tray.set_state(TrayState.BACKUP_ERROR)
            self.tray.notify(
                "Scheduled backup failed",
                f"[{profile.name}] {e}",
            )

            if profile.email.enabled:
                result = self.engine._current_result if self.engine else None
                self._send_backup_email(
                    profile,
                    False,
                    result,
                    str(e),
                )

            # Re-raise so the scheduler can trigger retry logic
            raise
        finally:
            self._backup_running = False

    def _scheduled_precheck_prompt(
        self,
        failures: list[tuple[str, str, bool, str]],
        profile: BackupProfile,
    ) -> str:
        """Show target alert from scheduler thread, wait for user response.

        Loops until the user either cancels or the precheck passes.
        Previously implemented with recursion: every Retry click pushed
        a new frame onto the Python stack, and a user who clicked Retry
        repeatedly on a still-offline NAS could blow the stack.

        Uses root.after() to show UI on the main thread and
        threading.Event to block the scheduler thread until the
        user makes a choice.

        Args:
            failures: Failed targets from precheck_targets().
            profile: Backup profile (for retry precheck).

        Returns:
            "ok" if all targets eventually pass, "cancel" if user cancels.
        """
        # Hard timeout so the scheduler thread cannot block forever if
        # the user ignores or dismisses the alert. Past this point we
        # give up, hide the alert and treat it as cancel — subsequent
        # scheduler ticks get a fresh chance to prompt again.
        _ALERT_TIMEOUT_SECONDS = 30 * 60  # 30 minutes

        current_failures = failures
        deadline = time.monotonic() + _ALERT_TIMEOUT_SECONDS
        while True:
            decision = {"value": None}  # "retry", "cancel", or None
            event = threading.Event()

            # Default-argument binding for ``decision`` and ``event``
            # makes these closures properly capture the per-iteration
            # values rather than the surrounding name (which ruff B023
            # would otherwise flag as fragile to maintenance).
            def _on_choice(choice: str, d=decision, e=event):
                d["value"] = choice
                e.set()

            def _show_alert(f=current_failures, pick=_on_choice):
                self._show_target_alert(
                    f,
                    on_retry=lambda: pick("retry"),
                    on_cancel=lambda: pick("cancel"),
                )

            # Show alert on main thread
            self.root.after(0, _show_alert)
            remaining = max(1.0, deadline - time.monotonic())
            clicked = event.wait(timeout=remaining)

            if not clicked:
                # No response within budget — treat as cancel to release
                # the scheduler thread. The next tick will re-prompt.
                logger.warning(
                    "Precheck prompt timed out after %ds — releasing scheduler thread",
                    _ALERT_TIMEOUT_SECONDS,
                )
                self.root.after(0, self._hide_target_alert)
                return "cancel"

            if decision["value"] == "cancel":
                self.root.after(0, self._hide_target_alert)
                return "cancel"

            # User clicked retry — hide alert and re-check
            self.root.after(0, self._hide_target_alert)
            results = self.engine.precheck_targets(profile)
            new_failures = [r for r in results if not r[2]]

            if not new_failures:
                return "ok"

            # Still failing — loop back to prompt again (bounded by
            # user input OR the deadline).
            current_failures = new_failures
            if time.monotonic() >= deadline:
                logger.warning("Precheck retry budget exhausted — releasing scheduler thread")
                self.root.after(0, self._hide_target_alert)
                return "cancel"

    # --- Target pre-check alert ---

    def _show_target_alert(
        self,
        failures: list[tuple[str, str, bool, str]],
        on_retry: callable,
        on_cancel: callable,
        on_continue=None,
    ) -> None:
        """Replace notebook with an alert frame listing unreachable targets.

        Args:
            failures: List of (role, action, success, detail) with success=False.
            on_retry: Callback when user clicks Retry.
            on_cancel: Callback when user clicks Cancel backup.
        """
        self._hide_target_alert()
        self.notebook.pack_forget()

        # Bring the window to the foreground so the user sees the alert
        self._show_window()

        frame = tk.Frame(self._main_frame, bg=Colors.CARD_BG)
        frame.pack(fill="both", expand=True)
        self._alert_frame = frame

        # Centered content with constrained width
        content = tk.Frame(frame, bg=Colors.CARD_BG)
        content.pack(expand=True, padx=60, pady=40)

        max_width = 700  # Max text width in pixels

        # Warning icon + title
        tk.Label(
            content,
            text="\u26a0  Destinations unavailable",
            font=(Fonts.FAMILY, Fonts.SIZE_HEADER, "bold"),
            fg=Colors.DANGER,
            bg=Colors.CARD_BG,
        ).pack(pady=(0, 20))

        tk.Label(
            content,
            text="The following backup destinations are not reachable:",
            font=(Fonts.FAMILY, Fonts.SIZE_LARGE),
            fg=Colors.TEXT,
            bg=Colors.CARD_BG,
        ).pack(pady=(0, 15))

        # List each failed target
        for role, action, _ok, _detail in failures:
            target_frame = tk.Frame(content, bg=Colors.CARD_BG)
            target_frame.pack(fill="x", pady=8, padx=20)

            tk.Label(
                target_frame,
                text=f"\u25cf  {role}",
                font=(Fonts.FAMILY, Fonts.SIZE_LARGE, "bold"),
                fg=Colors.TEXT,
                bg=Colors.CARD_BG,
                anchor="w",
            ).pack(fill="x")

            tk.Label(
                target_frame,
                text=f"    {action}",
                font=(Fonts.FAMILY, Fonts.SIZE_NORMAL),
                fg=Colors.ACCENT,
                bg=Colors.CARD_BG,
                anchor="w",
                wraplength=max_width,
                justify="left",
            ).pack(fill="x")

        # Footer message
        tk.Label(
            content,
            text="Please connect these destinations and click Retry.",
            font=(Fonts.FAMILY, Fonts.SIZE_NORMAL),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.CARD_BG,
        ).pack(pady=(20, 20))

        # Buttons
        btn_frame = tk.Frame(content, bg=Colors.CARD_BG)
        btn_frame.pack()

        ttk.Button(
            btn_frame,
            text="Retry",
            command=on_retry,
            style="Accent.TButton",
        ).pack(side="left", padx=10)

        ttk.Button(
            btn_frame,
            text="Cancel backup",
            command=on_cancel,
        ).pack(side="left", padx=10)

        if on_continue is not None:
            ttk.Button(
                btn_frame,
                text="Continue without mirror",
                command=on_continue,
            ).pack(side="left", padx=10)

    def _hide_target_alert(self) -> None:
        """Remove the alert frame and restore the notebook."""
        if self._alert_frame is not None:
            self._alert_frame.destroy()
            self._alert_frame = None
        self.notebook.pack(fill="both", expand=True)

    # --- Status ---

    def _on_status_change(self, state="", **kw):
        pass  # RunTab handles status display via events

    # --- Window management ---

    def _setup_single_instance_listener(self):
        """Poll for a signal file that indicates a second launch.

        When a second instance starts, it writes a signal file
        and exits. We poll for that file and show the window.
        """

        appdata = os.environ.get("APPDATA", "")
        signal_file = Path(appdata) / "BackupManager" / ".show_signal"

        def _check_signal():
            try:
                if signal_file.exists():
                    signal_file.unlink()
                    self._show_window()
            except Exception:
                logger.debug("Signal file check failed", exc_info=True)
            self.root.after(500, _check_signal)

        self.root.after(500, _check_signal)

    def _show_window(self):
        """Bring the main window to the foreground."""
        self.root.deiconify()
        self.root.state("normal")
        self.root.lift()
        self.root.attributes("-topmost", True)
        self.root.after(100, lambda: self.root.attributes("-topmost", False))
        self.root.focus_force()

    def _on_close(self):
        """Auto-save current profile and minimize to tray."""
        self._auto_save()
        self.root.withdraw()

    def _quit_app(self):
        """Auto-save current profile and quit the application."""
        self._auto_save()
        # Stop health polling to prevent background checks during shutdown
        self._health_poll_generation = -1
        self._health_configs = {}
        # Hide the window immediately to avoid visual flicker during cleanup
        self.root.withdraw()
        self.root.update_idletasks()
        self.scheduler.stop()
        self.tray.stop()
        self.root.destroy()

    def _auto_save(self):
        """Silently save the current profile if one is loaded."""
        if self._current_profile is not None:
            try:
                self._save_profile(silent=True)
            except Exception as exc:
                logger.warning("Auto-save failed: %s", exc)

    def _show_modules(self):
        from src.installer import check_all

        results = check_all()
        msg = "Feature status:\n\n"
        for feat, info in results.items():
            status = (
                "✅ Available" if info["available"] else f"❌ Missing: {', '.join(info['missing'])}"
            )
            msg += f"  {feat}: {status}\n"
        messagebox.showinfo("Modules", msg)

    def _show_about(self):
        """Show About as a full-screen inline panel replacing the notebook."""
        import webbrowser

        # Already open — do nothing
        if hasattr(self, "_about_frame") and self._about_frame is not None:
            return

        # Hide notebook + save frame
        self.notebook.pack_forget()
        self._save_frame.pack_forget()

        about = ttk.Frame(self._main_frame)
        about.pack(fill="both", expand=True)
        self._about_frame = about

        main = ttk.Frame(about, padding=(Spacing.SECTION, Spacing.LARGE))
        main.pack(fill="both", expand=True)

        ttk.Label(
            main,
            text=f"{APP_TITLE} v{__version__}",
            font=Fonts.title(),
        ).pack(anchor="w")

        ttk.Label(
            main,
            text="\nCopyright (c) 2026 Loic Ader",
            foreground=Colors.TEXT_SECONDARY,
        ).pack(anchor="w")

        link = ttk.Label(
            main,
            text="loicata.com",
            foreground="#1a73e8",
            cursor="hand2",
            font=Fonts.normal(),
        )
        link.pack(anchor="w")
        link.bind(
            "<Button-1>",
            lambda e: webbrowser.open("https://loicata.com"),
        )

        ttk.Label(
            main,
            text="GNU General Public License v3.0",
            foreground=Colors.TEXT_SECONDARY,
        ).pack(anchor="w")

        ttk.Separator(main, orient="horizontal").pack(fill="x", pady=(Spacing.LARGE, 0))

        # --- Buttons ---
        btn_frame = ttk.Frame(main)
        btn_frame.pack(fill="x", pady=(Spacing.LARGE, 0))

        def _close_about():
            about.destroy()
            self._about_frame = None
            self._save_frame.pack(fill="x", side="bottom")
            self.notebook.pack(fill="both", expand=True)

        def _open_bug_report():
            _close_about()
            self._show_bug_report()

        ttk.Button(
            btn_frame,
            text="Report a Bug",
            style="Accent.TButton",
            command=_open_bug_report,
        ).pack(side="left")

        ttk.Button(btn_frame, text="Close", command=_close_about).pack(
            side="left", padx=(Spacing.MEDIUM, 0)
        )

    # ------------------------------------------------------------------
    # Bug report
    # ------------------------------------------------------------------

    def _collect_diagnostic(self) -> str:
        """Collect anonymized diagnostic information.

        Gathers maximum useful data while strictly anonymizing personal
        information (paths, IPs, emails, profile names, bucket names).

        Returns:
            Formatted diagnostic string with system info and anonymized logs.
        """
        lines: list[str] = []

        # --- System info ---
        lines.append(f"- Version: {__version__}")
        lines.append(f"- Git commit: {_get_git_commit()}")
        lines.append(
            f"- OS: {platform.system()} {platform.release()} " f"Build {platform.version()}"
        )
        lines.append(f"- Python: {platform.python_version()}")
        lines.append(f"- Frozen: {_is_packaged_build()}")
        lines.append(f"- Install ID: {self.config_manager.get_install_id()}")
        lines.append(f"- Architecture: {platform.machine()} " f"({platform.architecture()[0]})")

        # Screen resolution + DPI
        try:
            w = self.root.winfo_screenwidth()
            h = self.root.winfo_screenheight()
            dpi = self.root.winfo_fpixels("1i")
            lines.append(f"- Screen: {w}x{h} @ {dpi:.0f} DPI")
        except Exception:
            lines.append("- Screen: unknown")

        # Process memory usage
        try:
            import psutil

            proc = psutil.Process(os.getpid())
            mem_mb = proc.memory_info().rss / (1024 * 1024)
            lines.append(f"- Memory usage: {mem_mb:.1f} MB")
        except Exception:
            pass

        # App uptime
        try:
            import time

            uptime_s = time.time() - self._start_time
            hours, rem = divmod(int(uptime_s), 3600)
            minutes, secs = divmod(rem, 60)
            lines.append(f"- Uptime: {hours}h {minutes}m {secs}s")
        except Exception:
            pass

        # --- App config ---
        app_settings = self.config_manager.load_app_settings()
        mode = app_settings.get("mode", "classic")
        lines.append(f"- Mode: {mode.replace('-', ' ').title()}")

        # Active tab
        try:
            active_tab_id = self.notebook.select()
            active_tab_text = self.notebook.tab(active_tab_id, "text").strip()
            lines.append(f"- Active tab: {active_tab_text}")
        except Exception:
            lines.append("- Active tab: unknown")

        # --- Profile details (anonymized: no names, no paths) ---
        profiles = self.config_manager.get_all_profiles()
        storage_types: set[str] = set()
        mirror_count = 0
        profile_details: list[str] = []
        for i, p in enumerate(profiles):
            stype = p.storage.storage_type.value
            storage_types.add(stype)
            m1 = getattr(p, "mirror1", None)
            m2 = getattr(p, "mirror2", None)
            if m1 and m1.storage_type:
                mirror_count += 1
            if m2 and m2.storage_type:
                mirror_count += 1

            # Per-profile anonymous summary
            btype = getattr(p, "backup_type", "unknown")
            if hasattr(btype, "value"):
                btype = btype.value
            encrypted = bool(getattr(p, "encryption_password", None))
            schedule = getattr(p, "schedule_frequency", "unknown")
            if hasattr(schedule, "value"):
                schedule = schedule.value
            retention = getattr(p, "retention_daily", "?")
            last_status = getattr(p, "last_status", "unknown")
            last_date = getattr(p, "last_backup_date", None)
            last_str = str(last_date) if last_date else "never"

            detail = (
                f"  Profile {i + 1}: storage={stype}, "
                f"type={btype}, encrypted={encrypted}, "
                f"schedule={schedule}, "
                f"retention_daily={retention}, "
                f"last={last_str}, status={last_status}"
            )
            profile_details.append(detail)

        type_str = " + ".join(sorted(storage_types)) if storage_types else "none"
        mirror_str = f", {mirror_count} mirror(s)" if mirror_count else ""
        lines.append(f"- Profiles: {len(profiles)} " f"(storage: {type_str}{mirror_str})")
        lines.extend(profile_details)

        # Installed dependencies versions
        deps = _collect_dependency_versions()
        if deps:
            lines.append(f"- Dependencies: {deps}")

        # Scheduler state
        try:
            running = getattr(self.scheduler, "_running", False)
            lines.append(f"- Scheduler: {'running' if running else 'stopped'}")
        except Exception:
            lines.append("- Scheduler: unknown")

        # Backup history (last 10 runs, anonymized)
        try:
            # Use the public ``journal`` property — the underscore
            # attribute is private and couples this caller to the
            # Scheduler internal naming.
            journal = self.scheduler.journal
            entries = journal.get_entries(limit=10)
            if entries:
                lines.append("- Recent backup runs:")
                for e in entries:
                    ts = e.get("timestamp", "?")
                    status = e.get("status", "?")
                    trigger = e.get("trigger", "?")
                    duration = e.get("duration_s", "?")
                    files = e.get("files_count", "?")
                    size = e.get("total_size_mb", "?")
                    errors = e.get("errors", [])
                    err_count = len(errors) if isinstance(errors, list) else 0
                    raw_line = (
                        f"    {ts} | {status} | trigger={trigger} | "
                        f"duration={duration}s | files={files} | "
                        f"size={size}MB | errors={err_count}"
                    )
                    # Anonymize journal entries (exception messages
                    # could contain injected content)
                    anon = anonymize_log_lines([raw_line])
                    lines.append(anon[0])
        except Exception:
            pass

        diagnostic = "\n".join(lines)

        # Return ONLY trusted system info as diagnostic text.
        # Untrusted data (logs, errors, crash) is collected separately
        # and placed INSIDE the signed machine-readable JSON block
        # by _build_machine_readable() to prevent injection via
        # unsigned text sections.
        return f"DIAGNOSTIC INFO:\n{diagnostic}"

    def _generate_bug_report(
        self,
        description: str,
        diagnostic: str,
        advanced: bool = False,
        id_file_path: str = "",
    ) -> Path:
        """Generate a bug report folder with diagnostic and instructions.

        Two modes:
        - Standard (advanced=False): system info only in the JSON block.
          No logs, no crash data, no traceback. Zero injection risk.
        - Advanced (advanced=True): includes sanitized logs, crash data,
          and structured traceback in the signed JSON block. Requires
          ID verification.

        Args:
            description: User-provided description of the issue.
            diagnostic: Anonymized diagnostic information.
            advanced: Include logs/crash in signed JSON block.
            id_file_path: Path to the ID document (advanced mode only).

        Returns:
            Path to the generated report folder.
        """
        appdata = os.environ.get("APPDATA", "")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        folder = Path(appdata) / "BackupManager" / f"BugReport_{timestamp}"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "screenshots").mkdir(exist_ok=True)

        # Sanitize user description against injection
        safe_description = _sanitize_user_text(description)

        # Build machine-readable block
        machine_data = _build_machine_readable(diagnostic, include_logs=advanced)

        # Compute source file hashes for cross-validation
        source_hashes = _compute_source_hashes()
        machine_data["source_hashes"] = source_hashes

        # Sign the machine-readable block
        machine_json = json.dumps(machine_data, indent=2, ensure_ascii=False)
        signature = _compute_report_hmac(machine_json)

        # Sign the FULL report (description + diagnostic + machine JSON)
        full_content = f"{safe_description}\n{diagnostic}\n{machine_json}"
        full_signature = _compute_report_hmac(full_content)

        mode_label = "Advanced" if advanced else "Standard"
        report_content = (
            f"{'=' * 60}\n"
            f"Backup Manager - Bug Report ({mode_label})\n"
            f"Generated: "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'=' * 60}\n\n"
            f"USER DESCRIPTION:\n{safe_description}\n\n"
            f"{'=' * 60}\n"
            f"{diagnostic}\n\n"
            f"{'=' * 60}\n"
            f"MACHINE READABLE (signed):\n"
            f"{machine_json}\n\n"
            f"HMAC-SHA256: {signature}\n"
            f"HMAC-FULL-SHA256: {full_signature}\n"
        )

        # Ed25519 signature proving report origin (official binary)
        ed_sig = _sign_report_ed25519(f"{safe_description}\n{diagnostic}\n{machine_json}")
        if ed_sig:
            report_content += f"ED25519-SIG: {ed_sig}\n"
        else:
            report_content += "ED25519-SIG: unavailable (dev build)\n"

        (folder / "diagnostic.txt").write_text(report_content, encoding="utf-8")

        # ID verification: hash the file + create decoy of same size
        if id_file_path:
            id_path = Path(id_file_path)
            if id_path.exists():
                try:
                    # Cap at 20 MB: a passport scan is typically 2-5 MB;
                    # anything above likely means the user picked the
                    # wrong file (video, backup archive, etc.). Reading
                    # the whole file into RAM could cause an OOM.
                    file_size = id_path.stat().st_size
                    max_id_size = 20 * 1024 * 1024
                    if file_size > max_id_size:
                        logger.warning(
                            "ID file too large (%d bytes > 20 MB limit), skipping",
                            file_size,
                        )
                    else:
                        # Streaming hash — avoids loading the whole file
                        # into memory.
                        h = hashlib.sha256()
                        chunk_size = 1024 * 1024  # 1 MB
                        with id_path.open("rb") as f:
                            while chunk := f.read(chunk_size):
                                h.update(chunk)
                        id_hash = h.hexdigest()

                        # Write hash to diagnostic (append)
                        with open(folder / "diagnostic.txt", "a", encoding="utf-8") as f:
                            f.write(f"ID-HASH-SHA256: {id_hash}\n")

                        # Decoy file of the same size — streaming write so
                        # we never hold a multi-MB random buffer in RAM.
                        decoy = folder / "id_verification.enc"
                        remaining = file_size
                        with decoy.open("wb") as f:
                            while remaining > 0:
                                n = min(remaining, chunk_size)
                                f.write(os.urandom(n))
                                remaining -= n
                except OSError:
                    logger.warning("Could not process ID file", exc_info=True)

        # INSTRUCTIONS.txt
        if advanced:
            instructions = (
                "HOW TO SEND THIS BUG REPORT\n"
                "============================\n\n"
                "All logs in this report have been anonymized.\n"
                "No personal data, file paths, or server names "
                "are included.\n\n"
                "1. Take screenshots of the problem\n"
                "   Press  Win + Shift + S  to capture your screen\n"
                '   Save them in the "screenshots" folder\n\n'
                "2. Send this entire folder by email to:\n\n"
                f"   {BUG_REPORT_EMAIL}\n\n"
                "   - Attach ALL files from this folder\n"
                '   - Including the "screenshots" folder\n\n'
                "Thank you for helping improve Backup Manager!\n"
            )
        else:
            instructions = (
                "HOW TO SEND THIS BUG REPORT\n"
                "============================\n\n"
                "This report contains only system information.\n"
                "No personal data is included.\n\n"
                "To help us fix the problem, please:\n\n"
                "1. Describe the problem in detail in your email\n"
                "   - What were you doing when it happened?\n"
                "   - What did you expect to happen?\n"
                "   - Does it happen every time?\n\n"
                "2. Take as many screenshots as possible\n"
                "   Press  Win + Shift + S  to capture your screen\n"
                '   Save them in the "screenshots" folder\n\n'
                "3. Send this entire folder by email to:\n\n"
                f"   {BUG_REPORT_EMAIL}\n\n"
                "   - Attach ALL files from this folder\n"
                '   - Including the "screenshots" folder\n\n'
                "The more detail you provide, the faster we can "
                "fix the issue.\n\n"
                "Thank you for helping improve Backup Manager!\n"
            )
        (folder / "INSTRUCTIONS.txt").write_text(instructions, encoding="utf-8")

        return folder

    def _show_bug_report(self):
        """Show the bug report as a full-screen panel replacing the notebook.

        Default mode (safe): system info only, no logs/crash/traceback.
        Advanced mode: full diagnostic with logs + ID verification.
        """
        # Already open — do nothing
        if hasattr(self, "_bug_frame") and self._bug_frame is not None:
            return

        try:
            diagnostic = self._collect_diagnostic()
        except Exception:
            logger.error("Failed to collect diagnostic", exc_info=True)
            diagnostic = "(diagnostic collection failed)"

        # Hide any previously-shown target alert so the bug report is
        # not rendered next to "Destinations unavailable" (both frames
        # live in _main_frame and would stack otherwise).
        self._hide_target_alert()

        # Hide notebook + save frame
        self.notebook.pack_forget()
        self._save_frame.pack_forget()

        # --- Full-screen bug report frame ---
        bug = ttk.Frame(self._main_frame)
        bug.pack(fill="both", expand=True)
        self._bug_frame = bug

        main = ttk.Frame(bug, padding=(Spacing.SECTION, Spacing.LARGE))
        main.pack(fill="both", expand=True)

        # --- Header ---
        ttk.Label(main, text="Report a Bug", font=Fonts.title()).pack(anchor="w")

        ttk.Label(
            main,
            text=(
                "Describe what happened and what you expected.\n"
                "Include as many screenshots as possible."
            ),
            foreground=Colors.TEXT_SECONDARY,
        ).pack(anchor="w", pady=(Spacing.SMALL, Spacing.MEDIUM))

        # --- Description text ---
        desc_text = tk.Text(
            main,
            height=8,
            font=Fonts.normal(),
            wrap="word",
            relief="solid",
            borderwidth=1,
        )
        desc_text.pack(fill="x", pady=(0, Spacing.LARGE))

        # --- Diagnostic (collapsible) ---
        diag_visible = tk.BooleanVar(value=False)
        toggle_btn = ttk.Button(main, text="\u25b6 Show diagnostic info")
        toggle_btn.pack(anchor="w")

        diag_frame = ttk.Frame(main)
        diag_text = tk.Text(
            diag_frame,
            height=14,
            font=Fonts.mono(),
            wrap="word",
            relief="solid",
            borderwidth=1,
            state="disabled",
        )
        diag_text.pack(fill="x")
        diag_text.configure(state="normal")
        diag_text.insert("1.0", diagnostic)
        diag_text.configure(state="disabled")

        def _toggle_diagnostic():
            if diag_visible.get():
                diag_frame.pack_forget()
                toggle_btn.configure(text="\u25b6 Show diagnostic info")
                diag_visible.set(False)
            else:
                diag_frame.pack(fill="x", pady=(Spacing.SMALL, 0), after=toggle_btn)
                toggle_btn.configure(text="\u25bc Hide diagnostic info")
                diag_visible.set(True)

        toggle_btn.configure(command=_toggle_diagnostic)

        # --- Advanced mode toggle ---
        advanced_var = tk.BooleanVar(value=False)
        advanced_frame = ttk.Frame(main)

        # ID picker (inside advanced_frame, shown only in advanced mode)
        id_path_var = tk.StringVar(value="")

        ttk.Label(
            advanced_frame,
            text="Identity verification (required for advanced mode)",
            font=Fonts.bold(),
        ).pack(anchor="w")

        ttk.Label(
            advanced_frame,
            text=(
                "Please provide a photo ID (passport, driver's license). "
                "This enables detailed log analysis for faster resolution."
            ),
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
            wraplength=800,
            justify="left",
        ).pack(anchor="w", pady=(Spacing.SMALL, 0))

        id_row = ttk.Frame(advanced_frame)
        id_row.pack(fill="x", pady=(Spacing.SMALL, 0))

        id_label = ttk.Label(id_row, text="No file selected", foreground=Colors.TEXT_DISABLED)
        id_label.pack(side="left", fill="x", expand=True, anchor="w")

        def _browse_id():
            from tkinter import filedialog

            path = filedialog.askopenfilename(
                title="Select ID document",
                filetypes=[
                    ("Images", "*.jpg *.jpeg *.png *.bmp *.pdf"),
                    ("All files", "*.*"),
                ],
                parent=self.root,
            )
            if path:
                id_path_var.set(path)
                fname = Path(path).name
                id_label.configure(
                    text=f"\u2713 {fname}",
                    foreground=Colors.SUCCESS,
                )

        ttk.Button(id_row, text="Browse...", command=_browse_id).pack(side="right")

        def _toggle_advanced():
            if advanced_var.get():
                advanced_frame.pack(fill="x", pady=(Spacing.LARGE, 0), before=sep)
            else:
                advanced_frame.pack_forget()
                id_path_var.set("")
                id_label.configure(
                    text="No file selected",
                    foreground=Colors.TEXT_DISABLED,
                )

        adv_check = ttk.Checkbutton(
            main,
            text="Advanced report (includes logs and crash data"
            " \u2014 requires ID verification)",
            variable=advanced_var,
            command=_toggle_advanced,
        )
        adv_check.pack(anchor="w", pady=(Spacing.LARGE, 0))

        # --- Footer separator + privacy note ---
        sep = ttk.Separator(main, orient="horizontal")
        sep.pack(fill="x", pady=(Spacing.LARGE, 0))

        ttk.Label(
            main,
            text=(
                "Only system information is included \u2014 no personal "
                "data, file paths, or server names."
            ),
            foreground=Colors.TEXT_SECONDARY,
            font=Fonts.small(),
        ).pack(anchor="w", pady=(Spacing.SMALL, Spacing.MEDIUM))

        # --- Buttons ---
        btn_frame = ttk.Frame(main)
        btn_frame.pack(fill="x")

        def _close_bug_report():
            bug.destroy()
            self._bug_frame = None
            self._save_frame.pack(fill="x", side="bottom")
            self.notebook.pack(fill="both", expand=True)

        def _send_report():
            description = desc_text.get("1.0", "end-1c").strip()
            if not description:
                messagebox.showwarning(
                    "Description required",
                    "Please describe the issue before sending.",
                    parent=self.root,
                )
                return

            is_advanced = advanced_var.get()
            id_file = id_path_var.get()

            if is_advanced and not id_file:
                messagebox.showwarning(
                    "ID required",
                    "Advanced mode requires an identity document.\n"
                    "Uncheck the advanced option to send a standard "
                    "report without ID.",
                    parent=self.root,
                )
                return

            try:
                folder = self._generate_bug_report(
                    description,
                    diagnostic,
                    advanced=is_advanced,
                    id_file_path=id_file if is_advanced else "",
                )
            except Exception:
                logger.error("Failed to generate bug report", exc_info=True)
                messagebox.showerror(
                    "Report generation failed",
                    "Could not create the bug report folder.\n"
                    "Check the log file for details and try again.",
                    parent=self.root,
                )
                return
            try:
                os.startfile(str(folder))
            except OSError:
                logger.warning("Could not open report folder", exc_info=True)
            bug.destroy()
            self._bug_frame = None
            self._show_report_ready()

        ttk.Button(
            btn_frame,
            text="Generate report",
            style="Accent.TButton",
            command=_send_report,
        ).pack(side="left")

        ttk.Button(btn_frame, text="Close", command=_close_bug_report).pack(
            side="left", padx=(Spacing.MEDIUM, 0)
        )

    def _show_report_ready(self):
        """Show report-ready confirmation as a full-screen inline panel."""
        ready = ttk.Frame(self._main_frame)
        ready.pack(fill="both", expand=True)

        main = ttk.Frame(ready, padding=(Spacing.SECTION, Spacing.LARGE))
        main.pack(fill="both", expand=True)

        ttk.Label(main, text="Report Ready", font=Fonts.title()).pack(anchor="w")

        ttk.Label(
            main,
            text="Your report folder has been opened.",
            foreground=Colors.TEXT_SECONDARY,
        ).pack(anchor="w", pady=(Spacing.SMALL, Spacing.LARGE))

        # Instructions
        steps = ttk.Frame(main)
        steps.pack(fill="x", pady=(0, Spacing.LARGE))

        ttk.Label(
            steps,
            text=(
                "1.  Send the folder contents by email to the address below\n"
                "2.  Add screenshots showing the problem \u2014 they help a lot!\n\n"
                "Tip: Press  Win + Shift + S  to capture your screen."
            ),
            justify="left",
            font=Fonts.normal(),
        ).pack(anchor="w")

        # Email address + copy button
        addr_frame = ttk.Frame(main)
        addr_frame.pack(fill="x", pady=(0, Spacing.LARGE))

        ttk.Label(
            addr_frame,
            text=BUG_REPORT_EMAIL,
            font=Fonts.bold(),
            foreground=Colors.ACCENT,
        ).pack(side="left")

        def _copy_address():
            self.root.clipboard_clear()
            self.root.clipboard_append(BUG_REPORT_EMAIL)
            copy_btn.configure(text="Copied!")

        copy_btn = ttk.Button(addr_frame, text="Copy address", command=_copy_address)
        copy_btn.pack(side="left", padx=(Spacing.MEDIUM, 0))

        # Separator + OK button
        ttk.Separator(main, orient="horizontal").pack(fill="x", pady=(Spacing.LARGE, 0))

        def _close_ready():
            ready.destroy()
            self._save_frame.pack(fill="x", side="bottom")
            self.notebook.pack(fill="both", expand=True)

        ttk.Button(
            main,
            text="OK",
            style="Accent.TButton",
            command=_close_ready,
        ).pack(anchor="w", pady=(Spacing.LARGE, 0))
