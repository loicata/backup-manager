"""
Backup Manager — Entry Point
==============================
Run with: python -m src

Startup sequence:
  1. DPI awareness (Windows)
  2. Create Tk root with splash "Starting..."
  3. auto_install_all() — install missing pip packages (Toplevel dialog)
  4. SetupWizard — first-launch 11-step wizard (Toplevel dialog)
  5. verify_integrity() — SHA-256 checksums of app files
  6. BackupManagerApp(root=_root) — takes over the root window
"""

import logging
import os
import sys
import tkinter as tk
from tkinter import messagebox
import traceback
from logging.handlers import RotatingFileHandler
from pathlib import Path

# 0. Enable DPI awareness FIRST — before any Tk window is created.
if sys.platform == "win32":
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
    except (AttributeError, OSError):
        try:
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except (AttributeError, OSError):
            try:
                import ctypes
                ctypes.windll.user32.SetProcessDPIAware()
            except (AttributeError, OSError):
                pass


def _crash_log(msg: str):
    """Write crash info to a log file next to the exe (or in APPDATA)."""
    import os
    from pathlib import Path
    try:
        if getattr(sys, 'frozen', False):
            log_path = Path(sys.executable).parent / "crash.log"
        else:
            log_path = Path(__file__).parent.parent / "crash.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(msg)
    except Exception:
        try:
            appdata = Path(os.environ.get("APPDATA", ".")) / "BackupManager"
            appdata.mkdir(parents=True, exist_ok=True)
            with open(appdata / "crash.log", "w", encoding="utf-8") as f:
                f.write(msg)
        except Exception:
            pass


def _setup_logging():
    """Configure root logger with RotatingFileHandler."""
    log_dir = Path(os.environ.get("APPDATA", ".")) / "BackupManager" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "backup_manager.log"

    handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    ))

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


def main():
    _setup_logging()

    try:
        # 1. Create root window — keep visible throughout startup
        _root = tk.Tk()
        _root.title("Backup Manager — Starting...")
        _root.geometry("400x100")
        _root.resizable(False, False)

        # Set shield icon immediately
        try:
            from PIL import Image, ImageDraw, ImageFont, ImageTk
            _sz = 64
            _ico = Image.new("RGBA", (_sz, _sz), (0, 0, 0, 0))
            _drw = ImageDraw.Draw(_ico)
            _cx, _cy = _sz // 2, _sz // 2
            _drw.polygon([(_cx, 2), (_sz-4, _cy-12), (_sz-6, _cy+12),
                          (_cx, _sz-2), (6, _cy+12), (4, _cy-12)],
                         fill="#3498db", outline="white")
            try:
                _fnt = ImageFont.truetype("arial.ttf", _sz // 3)
            except (OSError, IOError):
                _fnt = ImageFont.load_default()
            _bb = _drw.textbbox((0, 0), "B", font=_fnt)
            _drw.text((_cx - (_bb[2]-_bb[0])//2, _cy - (_bb[3]-_bb[1])//2 - 1),
                      "B", fill="white", font=_fnt)
            _root._startup_icon = ImageTk.PhotoImage(_ico)
            _root.iconphoto(True, _root._startup_icon)
        except Exception:
            pass

        # Center on screen
        _root.update_idletasks()
        x = (_root.winfo_screenwidth() - 400) // 2
        y = (_root.winfo_screenheight() - 100) // 2
        _root.geometry(f"400x100+{x}+{y}")
        _startup_label = tk.Label(_root, text="⏳ Starting Backup Manager...",
                                   font=("Segoe UI", 11))
        _startup_label.pack(expand=True)
        _root.update()

        # 2. Install all missing dependencies automatically
        from src.installer import auto_install_all
        auto_install_all(parent=_root)

        # 3. Show setup wizard on first launch (no profiles yet)
        from src.ui.wizard import SetupWizard, should_show_wizard
        from src.core.config import ConfigManager
        config_check = ConfigManager()
        auto_run_backup = False
        if should_show_wizard(config_check):
            wizard = SetupWizard(config_check, parent=_root)
            wizard.run()
            if wizard.result_profile:
                auto_run_backup = True

        # 4. Application integrity check (non-blocking)
        try:
            from src.security.integrity_check import verify_integrity, reset_checksums
            passed, msg = verify_integrity()
            if not passed:
                reset_checksums()
        except Exception:
            pass

        # 5. Launch main application — clear startup splash first
        _startup_label.destroy()
        from src.ui.app import BackupManagerApp
        app = BackupManagerApp(root=_root)
        if auto_run_backup:
            app.root.after(500, app._auto_start_first_backup)
        app.run()

    except Exception as e:
        error_text = f"Backup Manager failed to start:\n\n{e}\n\n{traceback.format_exc()}"
        _crash_log(error_text)

        try:
            err_root = tk.Tk()
            err_root.withdraw()
            messagebox.showerror(
                "Backup Manager — Startup Error",
                f"The application failed to start.\n\n"
                f"Error: {e}\n\n"
                f"A crash log has been saved.\n"
                f"Please check crash.log for details.",
                parent=err_root,
            )
            err_root.destroy()
        except Exception:
            pass

        sys.exit(1)


if __name__ == "__main__":
    main()
