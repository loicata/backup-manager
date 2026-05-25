"""Backup Manager v3 — Application entry point.

Handles: DPI awareness, single instance enforcement, logging setup,
setup wizard (first launch), integrity check, and app launch.
"""

import contextlib
import ctypes
import logging
import os
import sys
import traceback
from logging.handlers import RotatingFileHandler
from pathlib import Path

logger = logging.getLogger(__name__)


def _setup_dpi_awareness():
    """Enable high-DPI awareness on Windows."""
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except (AttributeError, OSError):
        with contextlib.suppress(AttributeError, OSError):
            ctypes.windll.user32.SetProcessDPIAware()


def _set_app_user_model_id():
    """Set AppUserModelID for proper taskbar icon grouping."""
    with contextlib.suppress(AttributeError, OSError):
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
            "BackupManager.BackupManager.3.0"
        )


def _is_nuitka() -> bool:
    """Detect if running as a Nuitka-compiled standalone binary."""
    return "__compiled__" in globals() or hasattr(sys.modules.get("__main__"), "__compiled__")


def _should_auto_enable_autostart() -> bool:
    return getattr(sys, "frozen", False) or _is_nuitka()


def _get_base_dir() -> "Path":
    """Resolve the application base directory.

    PyInstaller: sys._MEIPASS (temp extraction folder).
    Nuitka standalone: directory containing the .exe.
    Development: project root (parent of src/).

    Returns:
        Path to the base directory containing assets/.
    """
    from pathlib import Path

    if hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)  # noqa: SLF001
    if _is_nuitka():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def _get_icon_path() -> "Path | None":
    """Resolve the path to backup_manager.ico."""
    ico_path = _get_base_dir() / "assets" / "backup_manager.ico"
    return ico_path if ico_path.exists() else None


def _set_window_icon(root):
    """Set the window icon for taskbar and title bar.

    Uses both iconbitmap (title bar) and iconphoto (taskbar)
    to ensure consistent icon display on Windows.
    """
    ico_path = _get_icon_path()
    if ico_path is None:
        return

    try:
        # iconbitmap for title bar
        root.iconbitmap(default=str(ico_path))
        root.iconbitmap(str(ico_path))

        # iconphoto for taskbar — extract from ICO via PIL if available
        try:
            from PIL import Image, ImageTk

            img = Image.open(str(ico_path))
            # Get the largest size available in the ICO
            sizes = img.info.get("sizes", set())
            if sizes:
                largest = max(sizes, key=lambda s: s[0] * s[1])
                img = img.resize(largest, Image.LANCZOS)
            photo = ImageTk.PhotoImage(img)
            root.iconphoto(True, photo)
            # Keep reference to prevent garbage collection
            root._icon_photo = photo  # noqa: SLF001
        except ImportError:
            pass  # PIL not available, iconbitmap alone is fine

    except Exception:
        logger.debug("Could not set window icon", exc_info=True)


_mutex_handle = None


def _show_starting_splash(parent) -> "object":
    """Show a transient "Starting Backup Manager..." splash window.

    Used only on the first-launch wizard path: after the wizard's
    Toplevel is destroyed, ``BackupManagerApp.__init__`` builds 12
    tabs synchronously plus runs an integrity check, which can take
    5-10 seconds on a cold boot. Without a placeholder the screen is
    completely blank during that window — indistinguishable from a
    crash from the user's point of view.

    The splash is a chromeless Tk Toplevel (``overrideredirect``)
    centered on the primary monitor, so it cannot be moved or
    closed and does not appear in the taskbar. It is intentionally
    static (no spinner): the Tk event loop is blocked by the
    BackupManagerApp constructor that runs immediately after, so any
    ``after``-driven animation would freeze on the first frame.

    The caller MUST call ``.destroy()`` once the main window is
    ready to deiconify, otherwise the splash will outlive the app
    transition.

    Args:
        parent: The (currently hidden) Tk root window.

    Returns:
        The Toplevel widget. Stored as ``object`` in the type hint to
        keep this module importable without a Tk display present
        (e.g. in unit tests that mock the root).
    """
    import tkinter as tk
    from tkinter import ttk

    splash = tk.Toplevel(parent)
    splash.overrideredirect(True)
    splash.attributes("-topmost", True)

    width, height = 360, 140
    sw = splash.winfo_screenwidth()
    sh = splash.winfo_screenheight()
    x = (sw - width) // 2
    y = (sh - height) // 2
    splash.geometry(f"{width}x{height}+{x}+{y}")

    # Visible 1px frame border — without window chrome the splash
    # would otherwise float on screen with no boundary against the
    # desktop, which looks broken.
    frame = ttk.Frame(splash, relief="solid", borderwidth=1)
    frame.pack(fill="both", expand=True)

    title = ttk.Label(
        frame,
        text="Backup Manager",
        font=("Segoe UI", 14, "bold"),
        anchor="center",
    )
    title.pack(pady=(28, 4))

    msg = ttk.Label(
        frame,
        text="Starting...",
        font=("Segoe UI", 10),
        anchor="center",
    )
    msg.pack()

    # Force the splash to draw NOW, before the synchronous
    # BackupManagerApp constructor blocks the Tk event loop. Without
    # this call the Toplevel is created but never rendered — the
    # OS sees a window that never paints itself.
    splash.update()
    return splash


def _get_signal_file() -> Path:
    """Return the path to the 'show window' signal file."""
    appdata = os.environ.get("APPDATA", "")
    return Path(appdata) / "BackupManager" / ".show_signal"


def _acquire_single_instance() -> bool:
    """Ensure only one instance of the application is running.

    Returns True if this is the first instance.
    Uses a mutex for detection and a signal file to tell the
    running instance to bring its window to the foreground.
    """
    global _mutex_handle
    try:
        mutex_name = "BackupManager_v3_SingleInstance"
        kernel32 = ctypes.windll.kernel32
        _mutex_handle = kernel32.CreateMutexW(None, True, mutex_name)
        last_error = kernel32.GetLastError()

        if last_error == 183:  # ERROR_ALREADY_EXISTS
            # Write signal file so the running instance shows itself
            signal_file = _get_signal_file()
            signal_file.parent.mkdir(parents=True, exist_ok=True)
            signal_file.write_text("show", encoding="utf-8")
            kernel32.CloseHandle(_mutex_handle)
            _mutex_handle = None
            return False
        return True
    except Exception:
        logger.debug("Mutex acquisition failed, allowing startup", exc_info=True)
        return True


def _release_single_instance() -> None:
    """Release the single-instance mutex before exit."""
    global _mutex_handle
    if _mutex_handle is not None:
        try:
            kernel32 = ctypes.windll.kernel32
            kernel32.ReleaseMutex(_mutex_handle)
            kernel32.CloseHandle(_mutex_handle)
        except Exception:
            logger.debug("Could not release mutex", exc_info=True)
        _mutex_handle = None


def _setup_logging():
    """Configure rotating file logger."""
    appdata = os.environ.get("APPDATA", "")
    log_dir = Path(appdata) / "BackupManager" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "backup_manager.log"

    handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    # Tag every record with the active profile name when one is set
    # in the per-thread context. Two parallel runs (scheduler +
    # manual on a different profile) used to interleave their
    # messages into ``backup_manager.log`` without any way to tell
    # whose line was whose. ``[<profile_name>]`` in front of each
    # message lets ``grep '\[TestLoic\]'`` split the streams.
    from src.core.log_context import ProfilePrefixFilter

    handler.addFilter(ProfilePrefixFilter())

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    return logging.getLogger(__name__)


def _crash_log(error_msg: str):
    """Write crash info to a file."""
    appdata = os.environ.get("APPDATA", "")
    crash_file = Path(appdata) / "BackupManager" / "crash.log"
    crash_file.parent.mkdir(parents=True, exist_ok=True)
    crash_file.write_text(error_msg, encoding="utf-8")


def _format_hmac_regen_message(error) -> str:
    """Build the modal body shown when the HMAC key needs regeneration.

    Surfaces (a) the technical reason returned by ``_get_hmac_key``,
    (b) a plain-English explanation of the data-loss risk, and
    (c) the path to the most recent ``.legacy_*`` archive when
    ``_archive_old_key`` was able to back the old key up.

    Kept in its own helper so the handler can stay short and so the
    text can be exercised by a unit test that imports the function
    without spinning up a Tk display.

    Args:
        error: The :class:`HMACKeyRegeneratedError` instance raised
            by ``_get_hmac_key`` (or, equivalently, by
            ``verify_integrity`` which calls it).

    Returns:
        Multi-line message ready for ``tkinter.messagebox.askyesno``.
    """
    archive_hint = ""
    parent_dir = error.prior_key_path.parent
    if parent_dir.exists():
        pattern = f"{error.prior_key_path.name}.legacy_*"
        legacy_files = sorted(parent_dir.glob(pattern))
        if legacy_files:
            archive_hint = (
                f"\n\nThe original key file has been archived to:\n"
                f"  {legacy_files[-1]}\n"
                f"Keep this file: a future recovery tool may use it to "
                f"re-validate historical backup commit markers."
            )
    return (
        "Backup Manager has detected a change in its installation identity.\n\n"
        f"{error.reason}\n\n"
        "If you have NOT deliberately reinstalled Windows, changed user, "
        "or moved %APPDATA%\\BackupManager between machines:\n"
        "  -> Click 'No' (recommended). Your existing backups stay safe as "
        "long as no new run starts. Investigate the cause before relaunching.\n\n"
        "If this change is expected:\n"
        "  -> Click 'Yes'. Backup Manager will create a fresh key. "
        "ALL EXISTING BACKUPS ON LOCAL DESTINATIONS will be classified as "
        "orphans and removed at the next backup run."
        f"{archive_hint}\n\n"
        "Continue and accept loss of historical backups?"
    )


def _handle_hmac_regen_at_startup(error) -> str:
    """Show modal alert when ``_get_hmac_key`` reports a suspicious regen.

    Two outcomes:
        - ``"abort"``: user clicked No (or the dialog could not be
          displayed). The bootstrap MUST stop without regenerating —
          the offending state stays on disk so the next launch raises
          the same alert (idempotent).
        - ``"continue_destructive"``: user explicitly accepted that
          historical backups on LOCAL destinations will be wiped at
          the next backup. The caller is expected to enable plaintext
          fallback for this session and re-run integrity check so the
          regen actually happens.

    Args:
        error: The :class:`HMACKeyRegeneratedError` instance.

    Returns:
        ``"abort"`` or ``"continue_destructive"``.
    """
    try:
        import tkinter.messagebox as mb

        user_says_yes = mb.askyesno(
            "Backup Manager - Identity change detected",
            _format_hmac_regen_message(error),
            icon="warning",
            default="no",
        )
    except Exception:
        # If we cannot display the dialog (no display, Tk broken),
        # default to ABORT. Better to refuse to start than to delete
        # the user's backups without their consent.
        logger.exception("Could not show HMAC regen dialog — defaulting to abort")
        return "abort"
    return "continue_destructive" if user_says_yes else "abort"


def main():
    """Application main entry point."""
    start_minimized = "--minimized" in sys.argv
    # Opt-in flag that lets the user run with a clear-text HMAC key /
    # machine key when DPAPI is broken on their Windows profile
    # (corrupted user profile, group policy lockdown, antivirus
    # blocking crypt32). Off by default — the strict path refuses to
    # start and surfaces a dialog instead of silently degrading the
    # tamper-detection guarantees.
    allow_plaintext = "--allow-plaintext-keys" in sys.argv

    # Windows-specific setup
    if sys.platform == "win32":
        _setup_dpi_awareness()
        _set_app_user_model_id()

        if not _acquire_single_instance():
            sys.exit(0)

    # Logging
    logger = _setup_logging()
    logger.info(
        "Backup Manager v3 starting%s...",
        " (minimized)" if start_minimized else "",
    )

    # Apply CLI overrides that affect crypto behaviour BEFORE any
    # code path that may trigger key generation (verify_integrity,
    # save_profile, etc.). Done after logging setup so the ERROR line
    # emitted by ``enable_plaintext_fallback`` lands in the log file.
    if allow_plaintext:
        from src.security.integrity_check import enable_plaintext_fallback

        enable_plaintext_fallback()

    try:
        import tkinter as tk

        from src.core.config import ConfigManager
        from src.security.integrity_check import verify_integrity
        from src.ui.app import BackupManagerApp
        from src.ui.theme import setup_theme
        from src.ui.wizard import SetupWizard

        # Create root window (hidden until app is ready)
        logger.info("Creating root window...")
        root = tk.Tk()
        root.withdraw()

        # Apply the theme early — named-font overrides and DPI scaling
        # must be in place BEFORE any Toplevel (including the wizard) is
        # built, otherwise widgets fall back to the OS default font size
        # which looks oversized on HiDPI displays.
        setup_theme(root)

        # Set window icon for taskbar
        _set_window_icon(root)

        # Check if first launch (no profiles)
        logger.info("Loading profiles...")
        config_mgr = ConfigManager()
        profiles = config_mgr.get_all_profiles()
        logger.info("Found %d profiles", len(profiles))

        from_wizard = False

        if not profiles:
            # Show setup wizard — keep root hidden but move it
            # off-screen so the transient wizard Toplevel is visible.
            logger.info("No profiles — launching setup wizard...")
            root.withdraw()  # Keep root hidden during wizard
            wizard = SetupWizard(root, standalone=True)
            profile = wizard.run()
            if profile:
                config_mgr.save_profile(profile)
                logger.info("Wizard completed — profile saved")
                from_wizard = True
            else:
                logger.info("Wizard cancelled — exiting")
                root.destroy()
                return

        # Bridge the visual gap between wizard close and main UI
        # reveal. BackupManagerApp.__init__ takes 5-10 s after a fresh
        # install and Tk shows nothing during that interval; the
        # splash makes the wait obviously "starting", not "crashed".
        # Only shown on the wizard path because returning users see
        # the same delay but already trust the app — adding a splash
        # there would just feel like an extra flash on every launch.
        splash = _show_starting_splash(root) if from_wizard else None

        if _should_auto_enable_autostart():
            from src.core.scheduler import AutoStart

            # Re-create the Run-key entry whenever it is missing.  This
            # is NOT necessarily "first launch": MSI uninstall removes
            # the key, so an upgrade install path will see it gone and
            # legitimately rewrite it.  The old "(first launch)" label
            # was misleading in that case — say what we actually did.
            if not AutoStart.is_enabled():
                AutoStart.ensure_startup(show_window=False)
                logger.info("Auto-start registry entry created (was missing)")

        # Integrity check (non-blocking) — but the call also resolves
        # the per-install HMAC key via ``_get_hmac_key``. A suspicious
        # regen (DPAPI unwrap fail, key file disappeared while sentinel
        # present, etc.) raises ``HMACKeyRegeneratedError`` so we can
        # warn the user BEFORE the next backup classifies every
        # historical ``.wbcommit`` as an orphan and deletes the
        # corresponding LOCAL-destination backups.
        from src.core.exceptions import HMACKeyRegeneratedError

        logger.info("Running integrity check...")
        try:
            ok, msg = verify_integrity()
            if not ok:
                logger.warning("Integrity check: %s", msg)
        except HMACKeyRegeneratedError as regen_error:
            decision = _handle_hmac_regen_at_startup(regen_error)
            if decision == "abort":
                logger.warning(
                    "User aborted launch after HMAC regeneration alert. "
                    "State on disk left unchanged so the next launch re-prompts."
                )
                _release_single_instance()
                logger.info("Backup Manager exiting")
                os._exit(0)
            # User accepted: enable the plaintext fallback so the
            # regen actually proceeds on the retry below. The flag is
            # process-local, so a subsequent relaunch returns to the
            # strict posture automatically.
            from src.security.integrity_check import enable_plaintext_fallback

            enable_plaintext_fallback()
            logger.critical(
                "User confirmed HMAC regeneration despite warning. "
                "Historical .wbcommit markers will be classified as orphans "
                "and deleted at the next backup run on LOCAL destinations."
            )
            ok, msg = verify_integrity()
            if not ok:
                logger.warning("Integrity check (post-regen): %s", msg)

        # Launch main app — reset geometry and prepare window
        logger.info("Launching main app...")
        root.withdraw()  # Ensure hidden while resetting
        root.geometry("")  # Clear off-screen geometry from wizard
        root.update_idletasks()  # Process geometry reset
        # Center on screen with reasonable default size
        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()
        win_w, win_h = 1700, 1000
        x = (screen_w - win_w) // 2
        y = (screen_h - win_h) // 2
        root.geometry(f"{win_w}x{win_h}+{x}+{y}")

        # Build UI while window is still hidden to avoid flicker
        _app = BackupManagerApp(root, from_wizard=from_wizard)
        root.update_idletasks()

        # Tear down the wizard splash now that the main UI is built.
        # Done BEFORE deiconify so the visual handover is splash →
        # full window with no perceptible blank frame in between.
        if splash is not None:
            with contextlib.suppress(Exception):
                splash.destroy()

        # Now reveal the fully-built window
        root.attributes("-alpha", 1)
        if not start_minimized:
            root.deiconify()
            root.lift()
            root.attributes("-topmost", True)
            root.after(100, lambda: root.attributes("-topmost", False))
            root.focus_force()
        else:
            logger.info("Started minimized to tray")

        root.mainloop()

    except Exception as e:
        # Dedicated branch for DPAPI-unavailable so the user sees a
        # recovery-oriented message instead of the generic "Fatal Error"
        # panel that gives no actionable guidance. Imported inside the
        # handler so a corrupt ``src.core.exceptions`` cannot itself
        # break the safety-net dialog.
        try:
            from src.core.exceptions import DPAPIUnavailableError
        except Exception:
            DPAPIUnavailableError = ()  # type: ignore[assignment, misc]

        if isinstance(e, DPAPIUnavailableError):
            logger.critical("DPAPI failure at startup: %s", e)
            _crash_log(str(e))
            try:
                import tkinter.messagebox as mb

                mb.showerror(
                    "Backup Manager — DPAPI Unavailable",
                    "Backup Manager cannot start because Windows DPAPI is "
                    "unavailable on this user profile.\n\n"
                    f"{e}\n\n"
                    "Try one of the following:\n"
                    "  - Repair your Windows user profile and relaunch.\n"
                    "  - Wipe %APPDATA%\\BackupManager\\ to start fresh.\n"
                    "  - Relaunch with --allow-plaintext-keys to accept "
                    "the degraded security posture.",
                )
            except Exception:
                logger.debug("Could not show DPAPI error dialog", exc_info=True)
        else:
            error_msg = traceback.format_exc()
            logger.critical("Fatal error: %s", error_msg)
            _crash_log(error_msg)

            try:
                import tkinter.messagebox as mb

                mb.showerror(
                    "Backup Manager — Fatal Error",
                    f"An unexpected error occurred:\n\n{e}\n\n"
                    f"Details saved to crash.log",
                )
            except Exception:
                logger.debug("Could not show error dialog", exc_info=True)

    finally:
        # Release mutex and force-kill any lingering daemon threads
        if sys.platform == "win32":
            _release_single_instance()
        logger.info("Backup Manager exiting")
        os._exit(0)


if __name__ == "__main__":
    main()
