"""USB/external drive detection by hardware serial number.

Resolves drive letter changes for local storage destinations on Windows.
Uses PowerShell to query the hardware serial from the disk controller,
which is unique and immutable (survives reformats and port changes).

Non-Windows platforms: all functions return None / passthrough.
"""

import logging
import subprocess
import sys
from pathlib import Path

# Hide the console window when spawning PowerShell on Windows
_SUBPROCESS_FLAGS = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0

logger = logging.getLogger(__name__)

_PS_TIMEOUT = 5  # seconds


def get_hardware_serial(drive_letter: str) -> str | None:
    """Get the hardware serial number for a drive letter.

    Queries the physical disk serial via PowerShell, which is the
    manufacturer-assigned serial (not the volume serial).

    Args:
        drive_letter: Single letter like "G" (without colon or backslash).

    Returns:
        Serial string, or None if unavailable.
    """
    if sys.platform != "win32":
        return None

    if not drive_letter or not drive_letter[0].isalpha():
        return None

    letter = drive_letter[0].upper()

    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"Get-Partition -DriveLetter {letter} "
                "| Get-Disk "
                "| Select-Object -ExpandProperty SerialNumber",
            ],
            capture_output=True,
            text=True,
            timeout=_PS_TIMEOUT,
            creationflags=_SUBPROCESS_FLAGS,
        )
        serial = result.stdout.strip()
        if result.returncode == 0 and serial:
            logger.debug("Drive %s: serial=%s", letter, serial)
            return serial
    except subprocess.TimeoutExpired:
        logger.warning("PowerShell timeout getting serial for drive %s:", letter)
    except FileNotFoundError:
        logger.debug("PowerShell not available")
    except Exception as e:
        logger.debug("Could not get serial for drive %s: %s", letter, e)

    return None


def find_drive_by_serial(serial: str) -> str | None:
    """Find a drive letter matching the given hardware serial.

    Runs a single PowerShell query returning every (DriveLetter,
    SerialNumber) pair on the system, then searches the result. This
    replaces the previous per-letter loop that spawned up to 24
    PowerShell processes sequentially (each with a 5 s timeout) —
    freezing the UI for several seconds on laptops with many phantom
    volumes.

    Args:
        serial: Hardware serial to search for.

    Returns:
        Drive letter (e.g. "H") if found, or None.
    """
    if not serial or sys.platform != "win32":
        return None

    target = serial.strip().upper()

    mapping = _enumerate_drive_serials()
    for letter, found in mapping.items():
        if found and found.strip().upper() == target:
            return letter

    return None


def _enumerate_drive_serials() -> dict[str, str]:
    """Return {drive_letter: serial_number} for every mounted disk.

    Uses a single PowerShell invocation (Get-Partition | Get-Disk)
    so UI threads see one subprocess round-trip instead of 24.

    Returns:
        Dict mapping single-letter drive letter (uppercase) to its
        hardware serial. Empty dict on non-Windows, PowerShell
        failure, or timeout.
    """
    if sys.platform != "win32":
        return {}

    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                # Format-style join: "<letter>\t<serial>" per line.
                # Drives with no letter (unmounted) are skipped by the
                # Where-Object filter.
                "Get-Partition "
                "| Where-Object { $_.DriveLetter } "
                "| ForEach-Object { "
                "    $d = $_ | Get-Disk; "
                '    "$($_.DriveLetter)`t$($d.SerialNumber)" '
                "  }",
            ],
            capture_output=True,
            text=True,
            timeout=_PS_TIMEOUT,
            creationflags=_SUBPROCESS_FLAGS,
        )
    except subprocess.TimeoutExpired:
        logger.warning("PowerShell timeout enumerating drive serials")
        return {}
    except FileNotFoundError:
        logger.debug("PowerShell not available")
        return {}
    except Exception as e:
        logger.debug("Could not enumerate drive serials: %s", e)
        return {}

    if result.returncode != 0:
        return {}

    mapping: dict[str, str] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or "\t" not in line:
            continue
        letter, _, serial = line.partition("\t")
        letter = letter.strip()
        serial = serial.strip()
        if letter and serial:
            mapping[letter.upper()] = serial
    return mapping


def _probe_path_with_wake(path_str: str) -> bool:
    """Return True if ``path_str`` becomes readable within ~5 seconds.

    USB drives in power-save can return False on the first
    ``Path.exists()`` probe because Windows hasn't finished spinning
    them up. A handful of short retries reliably catch the common
    case (drive woke up during the first or second probe) and
    avoid a spurious ``Destinations unavailable`` alert on a drive
    that is fully functional.

    Wake sequence tries, with exponential back-off:
        0.0s  immediate check
        0.3s  quick retry
        0.8s  second retry
        1.8s  after I/O-poke the drive root
        3.8s  last chance
    Total worst case ≈ 4 seconds, which is invisible in the UI and
    far shorter than a real "not mounted" timeout.
    """
    import os as _os
    import time as _time

    p = Path(path_str)
    if p.exists():
        return True

    # Try to force Windows to bring the volume online by reading
    # its drive root (e.g. ``G:\``). This is a cheap filesystem
    # touch that typically wakes a sleeping USB drive.
    def _poke_root():
        if len(path_str) >= 2 and path_str[1] == ":":
            root = f"{path_str[0]}:\\"
            try:
                _os.listdir(root)
            except OSError:
                pass  # Ignore — we only wanted the side effect

    for attempt, delay in enumerate((0.3, 0.5, 1.0, 2.0)):
        _time.sleep(delay)
        if attempt == 2:
            # After two naive retries, poke the drive root to wake
            # a stubborn device before the last attempts.
            _poke_root()
        if p.exists():
            return True
    return False


def resolve_local_path(destination_path: str, device_serial: str) -> str:
    """Resolve a local storage path using the drive's hardware serial.

    If the original path is inaccessible but the drive can be found
    on a different letter via its serial, the path is rewritten.

    Args:
        destination_path: Original configured path (e.g. "G:\\Backups").
        device_serial: Hardware serial saved when the path was configured.

    Returns:
        Resolved path (may be unchanged if no resolution needed).
    """
    if not device_serial:
        # No serial → can't resolve. Still give the drive a chance
        # to wake up so the caller's ``test_connection`` doesn't get
        # a premature False from a sleeping USB.
        _probe_path_with_wake(destination_path)
        return destination_path

    # Try the original path first — with wake-up retries — before
    # spending time on a PowerShell enumeration. On a healthy setup
    # this returns immediately.
    if _probe_path_with_wake(destination_path):
        return destination_path

    if len(destination_path) < 2 or destination_path[1] != ":":
        return destination_path

    old_letter = destination_path[0].upper()
    relative = destination_path[2:]  # Everything after "X:" (includes leading \)

    new_letter = find_drive_by_serial(device_serial)
    if new_letter is None:
        logger.warning(
            "Drive not found for serial %s (was %s:)",
            device_serial,
            old_letter,
        )
        return destination_path

    if new_letter.upper() == old_letter:
        # Same letter but path doesn't exist — subfolder issue, not a letter change
        return destination_path

    resolved = f"{new_letter}:{relative}"
    logger.info(
        "Drive letter changed: %s: -> %s: (serial %s) — resolved to %s",
        old_letter,
        new_letter,
        device_serial,
        resolved,
    )
    return resolved
