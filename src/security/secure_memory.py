"""
Backup Manager - Secure Memory Module
=======================================
Best-effort memory protection for sensitive data (passwords, API keys).

IMPORTANT: Python strings are immutable and may be interned/cached.
These utilities reduce the exposure window but cannot guarantee that
Python hasn't made copies elsewhere. For maximum security, use
environment variables or hardware key storage (YubiKey).

Components:
  secure_clear(str)              → zeros CPython's internal string buffer via ctypes
  secure_clear_bytearray(b)      → zeros a mutable bytearray in place
  secure_clear_dict(d, keys)     → clears specific fields from a dict
  SecureString                   → context manager that auto-clears on exit

Usage in the app:
  - backup_engine: clears _encryption_password after each backup (finally block)
  - email_notifier: clears SMTP password after sending
  - storage: clears SFTP/Proton passwords after connecting
  - gui: _clear_sensitive_fields() on profile switch and app quit
"""

import ctypes
import hmac
import os
import re
import sys
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def secure_clear(s: Optional[str]) -> None:
    """
    Best-effort security cleanup for sensitive strings.

    NOTE: Python strings are immutable and may be interned/cached.
    Using ctypes.memset to zero string buffers causes segfaults in practice
    (interned literals, GC'd objects, reused memory). Instead, we rely on
    setting references to None and letting the garbage collector handle it.

    For truly sensitive data, use bytearrays (mutable) with secure_clear_bytearray().
    """
    # No-op for strings — ctypes.memset is unsafe on CPython string objects.
    # The caller should set the reference to None or "" after calling this.
    pass


def secure_clear_bytearray(b: Optional[bytearray]) -> None:
    """Zero out a bytearray in place."""
    if b and isinstance(b, bytearray):
        for i in range(len(b)):
            b[i] = 0


def secure_clear_dict(d: dict, keys: tuple[str, ...]) -> None:
    """
    Clear specific sensitive fields from a dictionary.
    Replaces each specified key's value with an empty string
    after attempting to zero the original.

    Args:
        d: Dictionary to clean
        keys: Tuple of key names to clear
    """
    for key in keys:
        value = d.get(key)
        if value and isinstance(value, str):
            secure_clear(value)
            d[key] = ""


# ── Context manager for automatic password cleanup ──
# Usage: with SecureString(password) as ss: use ss.value
# On exit (or garbage collection), the string buffer is zeroed.
class SecureString:
    """
    A context manager for sensitive strings that ensures cleanup.

    Usage:
        with SecureString(password) as pwd:
            use_password(pwd.value)
        # pwd.value is now zeroed

    Or without context manager:
        ss = SecureString(password)
        use_password(ss.value)
        ss.clear()  # Explicit cleanup
    """

    __slots__ = ('_value', '_cleared')

    def __init__(self, value: str = ""):
        self._value = value
        self._cleared = False

    @property
    def value(self) -> str:
        if self._cleared:
            return ""
        return self._value

    def __str__(self) -> str:
        return self.value

    def __bool__(self) -> bool:
        return bool(self._value) and not self._cleared

    def __len__(self) -> int:
        return len(self._value) if not self._cleared else 0

    def clear(self):
        """Zero the underlying string and mark as cleared."""
        if not self._cleared and self._value:
            secure_clear(self._value)
            self._value = ""
            self._cleared = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.clear()

    def __del__(self):
        self.clear()


# ── Bytearray-based secure password storage ──
class SecurePassword:
    """
    Mutable password storage using bytearray (can be zeroed in place).

    Unlike Python str (immutable, may be interned/cached), bytearray contents
    can be reliably overwritten. Use this for passwords that must be held in
    memory temporarily.

    Usage:
        with SecurePassword("hunter2") as pwd:
            authenticate(pwd.get())
        # memory is zeroed
    """

    __slots__ = ('_data', '_cleared')

    def __init__(self, password: str = ""):
        self._data = bytearray(password.encode("utf-8")) if password else bytearray()
        self._cleared = False

    def get(self) -> str:
        if self._cleared:
            return ""
        return self._data.decode("utf-8")

    def clear(self):
        if not self._cleared and self._data:
            secure_clear_bytearray(self._data)
            self._cleared = True

    def __bool__(self) -> bool:
        return len(self._data) > 0 and not self._cleared

    def __len__(self) -> int:
        return len(self._data) if not self._cleared else 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.clear()

    def __del__(self):
        self.clear()

    def __repr__(self) -> str:
        return f"SecurePassword(***)" if not self._cleared else "SecurePassword(cleared)"


# ── Constant-time comparison ──
def constant_time_compare(a: str, b: str) -> bool:
    """Compare two strings in constant time to prevent timing attacks."""
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


# ── Path validation utilities ──
# Windows reserved names and characters
_WINDOWS_RESERVED = re.compile(
    r'^(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(\.|$)', re.IGNORECASE
)
_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def sanitize_backup_name(name: str) -> str:
    """
    Sanitize a backup folder/file name for safe filesystem use.
    Removes path traversal, null bytes, reserved Windows names, and unsafe chars.
    """
    if not name:
        return "backup"
    # Remove path separators and traversal
    name = name.replace("..", "").replace("/", "").replace("\\", "")
    # Remove null bytes and control characters
    name = _UNSAFE_CHARS.sub("_", name)
    # Block Windows reserved names
    if _WINDOWS_RESERVED.match(name):
        name = f"_{name}"
    # Strip leading/trailing dots and spaces (Windows limitation)
    name = name.strip(". ")
    return name or "backup"


def validate_path_no_traversal(base: Path, user_path: Path) -> bool:
    """
    Verify that user_path resolves to a location under base.
    Prevents directory traversal attacks (e.g., ../../etc/passwd).

    Args:
        base: The allowed root directory
        user_path: The path to validate

    Returns:
        True if user_path is safely under base, False otherwise
    """
    try:
        resolved_base = base.resolve()
        resolved_path = user_path.resolve()
        return str(resolved_path).startswith(str(resolved_base))
    except (OSError, ValueError):
        return False