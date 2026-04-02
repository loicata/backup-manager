"""System tray icon with status indicators.

Uses pystray + Pillow for icon generation.
Status states: idle, running, success, error.
"""

import contextlib
import logging
import threading
from collections.abc import Callable
from enum import Enum

logger = logging.getLogger(__name__)


class TrayState(Enum):
    IDLE = "idle"
    BACKUP_RUNNING = "running"
    BACKUP_SUCCESS = "success"
    BACKUP_ERROR = "error"


# Shield icon colors per state
_STATE_COLORS = {
    TrayState.IDLE: ("#3498db", "B"),  # Blue
    TrayState.BACKUP_RUNNING: ("#f39c12", "▶"),  # Orange
    TrayState.BACKUP_SUCCESS: ("#27ae60", "✓"),  # Green
    TrayState.BACKUP_ERROR: ("#e74c3c", "!"),  # Red
}


def _create_icon_image(state: TrayState, size: int = 64):
    """Generate a shield icon with status badge."""
    try:
        from PIL import Image, ImageDraw, ImageFont

        img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        color, letter = _STATE_COLORS.get(state, ("#3498db", "B"))

        # Draw shield shape
        cx, cy = size // 2, size // 2
        r = size // 2 - 2
        points = [
            (cx, cy - r),  # Top
            (cx + r, cy - r // 2),  # Top right
            (cx + r, cy + r // 4),  # Right
            (cx, cy + r),  # Bottom
            (cx - r, cy + r // 4),  # Left
            (cx - r, cy - r // 2),  # Top left
        ]
        draw.polygon(points, fill=color, outline=color)

        # Draw letter
        try:
            font = ImageFont.truetype("segoeui.ttf", size // 2)
        except Exception:
            font = ImageFont.load_default()

        bbox = draw.textbbox((0, 0), letter, font=font)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        draw.text(
            (cx - tw // 2, cy - th // 2 - 2),
            letter,
            fill="white",
            font=font,
        )

        return img
    except ImportError:
        return None


class BackupTray:
    """System tray icon manager."""

    def __init__(
        self,
        show_callback: Callable,
        run_backup_callback: Callable,
        quit_callback: Callable,
    ):
        self._show_cb = show_callback
        self._run_cb = run_backup_callback
        self._quit_cb = quit_callback
        self._icon = None
        self._state = TrayState.IDLE
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start tray icon in a daemon thread."""
        try:
            import pystray

            icon_image = _create_icon_image(self._state)
            if icon_image is None:
                logger.warning("Cannot create tray icon (Pillow not available)")
                return

            menu = pystray.Menu(
                pystray.MenuItem("Show window", self._on_show, default=True),
                pystray.MenuItem("Run backup now", self._on_run),
                pystray.Menu.SEPARATOR,
                pystray.MenuItem("Exit", self._on_quit),
            )

            self._icon = pystray.Icon(
                "BackupManager",
                icon_image,
                "Backup Manager",
                menu,
            )

            self._thread = threading.Thread(target=self._icon.run, daemon=True, name="TrayIcon")
            self._thread.start()
            logger.info("Tray icon started")

        except ImportError:
            logger.info("pystray not available — tray icon disabled")
        except Exception:
            logger.exception("Failed to start tray icon")

    def stop(self) -> None:
        """Stop and remove the tray icon."""
        if self._icon:
            with contextlib.suppress(Exception):
                self._icon.stop()
            self._icon = None
        logger.info("Tray icon stopped")

    def set_state(self, state: TrayState) -> None:
        """Update tray icon to reflect backup state."""
        self._state = state
        if self._icon:
            try:
                img = _create_icon_image(state)
                if img:
                    self._icon.icon = img
            except Exception:
                logger.debug("Could not update tray icon", exc_info=True)

    def notify(self, title: str, message: str) -> None:
        """Show a system notification."""
        if self._icon:
            with contextlib.suppress(Exception):
                self._icon.notify(message, title)

    def _on_show(self, icon=None, item=None):
        self._show_cb()

    def _on_run(self, icon=None, item=None):
        self._run_cb()

    def _on_quit(self, icon=None, item=None):
        self._quit_cb()
