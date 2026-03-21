r"""Network storage backend for UNC paths (\\server\share).

Extends LocalStorage with connection timeout handling.
"""

import logging
import threading
from pathlib import Path

from src.storage.local import LocalStorage

logger = logging.getLogger(__name__)

CONNECTION_TIMEOUT = 15  # seconds


class NetworkStorage(LocalStorage):
    """Storage backend for network UNC paths."""

    def test_connection(self) -> tuple[bool, str]:
        """Test network path with timeout."""
        result = [False, "Connection timeout"]

        def _test():
            try:
                dest = Path(self._dest)
                if not dest.exists():
                    result[0] = False
                    result[1] = f"Network path not found: {self._dest}"
                    return

                test_file = dest / ".backup_manager_test"
                test_file.write_text("test", encoding="utf-8")
                test_file.unlink()

                free = self.get_free_space()
                if free is not None:
                    free_gb = free / (1024**3)
                    result[0] = True
                    result[1] = f"Connected — {free_gb:.1f} GB free"
                else:
                    result[0] = True
                    result[1] = "Connected"
            except PermissionError:
                result[0] = False
                result[1] = f"Permission denied: {self._dest}"
            except Exception as e:
                result[0] = False
                result[1] = f"Error: {e}"

        thread = threading.Thread(target=_test, daemon=True)
        thread.start()
        thread.join(timeout=CONNECTION_TIMEOUT)

        if thread.is_alive():
            return False, f"Connection timeout after {CONNECTION_TIMEOUT}s"

        return result[0], result[1]
