"""USB/external drive detection by hardware serial number.

Resolves drive letter changes for local storage destinations on Windows.
Uses PowerShell to query the hardware serial from the disk controller,
which is unique and immutable (survives reformats and port changes).

Non-Windows platforms: all functions return None / passthrough.
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

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

    Scans all drive letters C-Z and compares their hardware serial.

    Args:
        serial: Hardware serial to search for.

    Returns:
        Drive letter (e.g. "H") if found, or None.
    """
    if not serial or sys.platform != "win32":
        return None

    target = serial.strip().upper()

    for code in range(ord("C"), ord("Z") + 1):
        letter = chr(code)
        if not os.path.isdir(f"{letter}:\\"):
            continue

        found = get_hardware_serial(letter)
        if found and found.strip().upper() == target:
            return letter

    return None


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
        return destination_path

    if Path(destination_path).exists():
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
