"""
Backup Manager - System Tray Icon
===================================
Windows notification area icon with dynamic status and toast notifications.

Dependencies (all optional — graceful degradation):
  pystray  → system tray icon management
  Pillow   → icon generation (shield shape, color-coded)
  plyer    → Windows toast notifications (fallback: pystray balloon)

Icon states (color-coded shield):
  IDLE            → blue    — no backup running
  BACKUP_RUNNING  → orange  — backup in progress
  BACKUP_SUCCESS  → green   — last backup succeeded
  BACKUP_ERROR    → red     — last backup failed

Right-click menu: Show window, Run backup now, Status line, Quit
Double-click: restore the main window

All imports are protected with try/except. If any dependency is missing,
is_tray_available() returns False and the app runs without tray support.
The BackupTray.start() spawns a daemon thread for the icon event loop.
"""

from __future__ import annotations

import threading
import sys
from enum import Enum
from typing import Optional, Callable

try:
    from PIL import Image, ImageDraw, ImageFont
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

try:
    from pystray import Icon, Menu, MenuItem
    HAS_PYSTRAY = True
except ImportError:
    HAS_PYSTRAY = False

try:
    from plyer import notification as plyer_notification
    HAS_PLYER = True
except ImportError:
    HAS_PLYER = False


# ══════════════════════════════════════════════
#  Tray States
# ══════════════════════════════════════════════
class TrayState(Enum):
    IDLE = "idle"
    BACKUP_RUNNING = "running"
    BACKUP_SUCCESS = "success"
    BACKUP_ERROR = "error"


# ══════════════════════════════════════════════
#  Icon Generation (shield style)
# ══════════════════════════════════════════════
def _create_icon(color: str, badge_text: str = "B", size: int = 64) -> Image.Image:
    """
    Generate a shield-shaped icon with the given colour.
    Works at any DPI because it is drawn programmatically.
    """
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Shield polygon
    cx, cy = size // 2, size // 2
    points = [
        (cx, 2),
        (size - 4, cy - 12),
        (size - 6, cy + 12),
        (cx, size - 2),
        (6, cy + 12),
        (4, cy - 12),
    ]
    draw.polygon(points, fill=color, outline="white")

    # Badge letter
    try:
        font = ImageFont.truetype("arial.ttf", size // 3)
    except (OSError, IOError):
        font = ImageFont.load_default()

    bbox = draw.textbbox((0, 0), badge_text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text((cx - tw // 2, cy - th // 2 - 1), badge_text,
              fill="white", font=font)
    return img


# Pre-rendered icons for each state (only if PIL is available)
if HAS_PIL:
    _ICONS = {
        TrayState.IDLE:           _create_icon("#3498db", "B"),   # Blue — idle
        TrayState.BACKUP_RUNNING: _create_icon("#f39c12", "▶"),   # Orange — running
        TrayState.BACKUP_SUCCESS: _create_icon("#27ae60", "✓"),   # Green  — success
        TrayState.BACKUP_ERROR:   _create_icon("#e74c3c", "!"),   # Red    — error
    }
else:
    _ICONS = {}

_TOOLTIPS = {
    TrayState.IDLE:           "Backup Manager — Idle",
    TrayState.BACKUP_RUNNING: "Backup Manager — Backup in progress…",
    TrayState.BACKUP_SUCCESS: "Backup Manager — Last backup OK",
    TrayState.BACKUP_ERROR:   "Backup Manager — Last backup failed",
}


# ══════════════════════════════════════════════
#  BackupTray — main class
# ══════════════════════════════════════════════
class BackupTray:
    """
    Manages the system-tray icon for Backup Manager.

    Usage from gui.py
    -----------------
    >>> self.tray = BackupTray(
    ...     on_show=self._show_from_tray,
    ...     on_run_backup=self._run_backup,
    ...     on_quit=self._quit_app,
    ... )
    >>> self.tray.start()                       # spawns the icon
    >>> self.tray.set_state(TrayState.BACKUP_RUNNING)
    >>> self.tray.notify("Backup started", "Profile: My Backup")
    >>> self.tray.stop()
    """

    def __init__(
        self,
        on_show: Optional[Callable] = None,
        on_run_backup: Optional[Callable] = None,
        on_quit: Optional[Callable] = None,
        app_version: str = "",
    ):
        if not HAS_PYSTRAY:
            raise ImportError(
                "pystray is required for the system tray icon.\n"
                "Install it with: pip install pystray Pillow"
            )

        self._on_show = on_show
        self._on_run_backup = on_run_backup
        self._on_quit = on_quit
        self._app_version = app_version

        self._state = TrayState.IDLE
        self._icon: Optional[Icon] = None
        self._thread: Optional[threading.Thread] = None

    # ── public API ────────────────────────────

    @property
    def available(self) -> bool:
        return HAS_PYSTRAY

    @property
    def state(self) -> TrayState:
        return self._state

    def start(self):
        """Create and show the tray icon (non-blocking)."""
        self._icon = Icon(
            name="Backup Manager",
            icon=_ICONS[self._state],
            title=_TOOLTIPS[self._state],
            menu=self._build_menu(),
        )
        self._thread = threading.Thread(target=self._icon.run, daemon=True)
        self._thread.start()

    def stop(self):
        """Remove the icon from the tray."""
        if self._icon:
            try:
                self._icon.stop()
            except Exception:
                pass

    def set_state(self, state: TrayState):
        """Update icon + tooltip to reflect the new state."""
        self._state = state
        if self._icon:
            self._icon.icon = _ICONS[state]
            self._icon.title = _TOOLTIPS[state]

    # ── Toast notifications ──
    # Priority: plyer (native Windows toast) > pystray balloon > print fallback
    def notify(self, title: str, message: str):
        """
        Send a Windows toast notification.
        Falls back to pystray.notify, then console print.
        """
        if HAS_PLYER:
            try:
                plyer_notification.notify(
                    title=title,
                    message=message,
                    app_name="Backup Manager",
                    timeout=6,
                )
                return
            except Exception:
                pass

        # Fallback: pystray built-in (balloon tip on Windows)
        if self._icon:
            try:
                self._icon.notify(message, title)
                return
            except Exception:
                pass

        # Last resort
        print(f"[TRAY] {title} — {message}")

    # ── menu ──────────────────────────────────

    def _build_menu(self) -> "Menu":
        title_text = f"Backup Manager v{self._app_version}" if self._app_version else "Backup Manager"
        return Menu(
            MenuItem(title_text, None, enabled=False),
            Menu.SEPARATOR,
            MenuItem("🖥  Show window", self._action_show, default=True),
            MenuItem("▶  Run backup now", self._action_run_backup),
            Menu.SEPARATOR,
            MenuItem(
                "Status",
                Menu(
                    MenuItem(lambda item: f"  {_TOOLTIPS[self._state]}", None, enabled=False),
                ),
            ),
            Menu.SEPARATOR,
            MenuItem("❌  Quit", self._action_quit),
        )

    # ── callbacks (run on pystray thread → delegate to tkinter) ──

    def _action_show(self, icon=None, item=None):
        if self._on_show:
            self._on_show()

    def _action_run_backup(self, icon=None, item=None):
        if self._on_run_backup:
            self._on_run_backup()

    def _action_quit(self, icon=None, item=None):
        if self._on_quit:
            self._on_quit()


# ══════════════════════════════════════════════
#  Availability helper (for import guards)
# ══════════════════════════════════════════════
def is_tray_available() -> bool:
    """Return True if pystray + Pillow are importable."""
    return HAS_PYSTRAY and HAS_PIL
