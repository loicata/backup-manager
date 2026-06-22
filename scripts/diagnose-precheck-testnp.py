"""Reproduce LocalStorage.test_connection() against G:/Backup Manager.

Used to diagnose the TestNP precheck failure observed at 12:52:32 on
2026-05-18 where the scheduled backup was rejected with "Destinations
unavailable" 12 seconds after TestLoic finished its GFS rotation.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

# Make src importable
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from src.core.config import StorageConfig, StorageType  # noqa: E402
from src.storage.local import LocalStorage  # noqa: E402


def main() -> int:
    backend = LocalStorage("G:/Backup Manager")
    print(f"Backend dest: {backend._dest}")
    print(f"Path exists (raw): {backend._dest.exists()}")
    print()

    print("Running test_connection() 5 times to catch flakiness...")
    for i in range(1, 6):
        t0 = time.monotonic()
        ok, msg = backend.test_connection()
        dt = time.monotonic() - t0
        marker = "OK" if ok else "FAIL"
        print(f"  [{i}] {marker} ({dt:5.2f}s) — {msg}")
        time.sleep(0.5)

    free = backend.get_free_space()
    if free is not None:
        print(f"\nFree space: {free / (1024**3):.2f} GB")

    return 0


if __name__ == "__main__":
    sys.exit(main())
