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


def _get_icon_path() -> "Path | None":
    """Resolve the path to backup_manager.ico."""
    from pathlib import Path

    if getattr(sys, "frozen", False):
        base = Path(sys._MEIPASS)  # noqa: SLF001
    else:
        base = Path(__file__).resolve().parent.parent

    ico_path = base / "assets" / "backup_manager.ico"
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
        pass


_mutex_handle = None


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
        return True  # If mutex fails, allow running


def _release_single_instance() -> None:
    """Release the single-instance mutex before exit."""
    global _mutex_handle
    if _mutex_handle is not None:
        try:
            kernel32 = ctypes.windll.kernel32
            kernel32.ReleaseMutex(_mutex_handle)
            kernel32.CloseHandle(_mutex_handle)
        except Exception:
            pass
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


def main():
    """Application main entry point."""
    start_minimized = "--minimized" in sys.argv

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

    try:
        import tkinter as tk

        from src.core.config import ConfigManager
        from src.security.integrity_check import verify_integrity
        from src.ui.app import BackupManagerApp
        from src.ui.wizard import SetupWizard

        # Create root window (hidden until app is ready)
        logger.info("Creating root window...")
        root = tk.Tk()
        root.withdraw()

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
            root.geometry("1x1+-10000+-10000")
            root.deiconify()
            root.attributes("-alpha", 0)  # Fully transparent root
            wizard = SetupWizard(root)
            profile = wizard.run()
            root.attributes("-alpha", 1)  # Restore opacity
            root.withdraw()
            if profile:
                config_mgr.save_profile(profile)
                logger.info("Wizard completed — profile saved")
                from_wizard = True
            else:
                logger.info("Wizard cancelled — launching app without profile")

        # Enable auto-start on first frozen launch if not already configured
        if getattr(sys, "frozen", False):
            from src.core.scheduler import AutoStart

            if not AutoStart.is_enabled():
                AutoStart.ensure_startup(show_window=False)
                logger.info("Auto-start enabled (first launch)")

        # Integrity check (non-blocking)
        logger.info("Running integrity check...")
        ok, msg = verify_integrity()
        if not ok:
            logger.warning("Integrity check: %s", msg)

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
        error_msg = traceback.format_exc()
        logger.critical("Fatal error: %s", error_msg)
        _crash_log(error_msg)

        try:
            import tkinter.messagebox as mb

            mb.showerror(
                "Backup Manager — Fatal Error",
                f"An unexpected error occurred:\n\n{e}\n\n" f"Details saved to crash.log",
            )
        except Exception:
            pass

    finally:
        # Release mutex and force-kill any lingering daemon threads
        if sys.platform == "win32":
            _release_single_instance()
        logger.info("Backup Manager exiting")
        os._exit(0)


if __name__ == "__main__":
    main()
