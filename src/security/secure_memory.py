"""Secure memory clearing for sensitive data.

Overwrites mutable byte buffers that may contain sensitive data
(passwords, keys) so they do not linger in memory after use.

Note on immutable types
-----------------------
Python's ``str`` and ``bytes`` are immutable and may be interned.
There is no portable, safe way to overwrite their buffers: ``ctypes``
tricks targeting ``id(obj) + offset`` depend on undocumented CPython
internals, may corrupt neighbouring objects, and can mutate
interpreter-wide interned literals.  ``secure_clear`` therefore only
operates on ``bytearray``; callers that need guaranteed erasure must
transit sensitive material via ``SecurePassword`` (bytearray-backed).
"""

import logging

logger = logging.getLogger(__name__)


def secure_clear(data) -> None:
    """Overwrite the bytes of a ``bytearray`` with zeros in place.

    Args:
        data: Sensitive data to clear.  A ``bytearray`` is zeroed in
            place.  ``None``, ``str`` and ``bytes`` are accepted for
            API compatibility but cannot be overwritten safely, so they
            are silently ignored — the caller should not rely on
            erasure for these types.
    """
    if data is None:
        return
    if isinstance(data, bytearray):
        for i in range(len(data)):
            data[i] = 0
        return
    if isinstance(data, (str, bytes)):
        # Immutable: cannot be overwritten safely.  Emit a debug log so
        # developers notice they are passing the wrong type; production
        # behaviour is a silent no-op to preserve existing call sites.
        logger.debug(
            "secure_clear: %s is immutable and cannot be zeroed; "
            "use SecurePassword for guaranteed erasure",
            type(data).__name__,
        )
        return


class SecurePassword:
    """Context manager that holds a password as a mutable bytearray.

    The password is stored internally as a bytearray so it can be
    deterministically zeroed when no longer needed, unlike Python
    str objects which are immutable.

    Usage:
        with SecurePassword(plain_str) as pw:
            do_encryption(pw.get())
        # password buffer is now zeroed
    """

    def __init__(self, password: str) -> None:
        self._buf = bytearray()
        self._cleared = True
        if not isinstance(password, str):
            raise TypeError(f"Expected str, got {type(password).__name__}")
        self._buf = bytearray(password.encode("utf-8"))
        self._cleared = False

    def get(self) -> str:
        """Return the password as a str.

        Returns:
            The password string.

        Raises:
            RuntimeError: If the password has been cleared.
        """
        if self._cleared:
            raise RuntimeError("SecurePassword has been cleared")
        return self._buf.decode("utf-8")

    def clear(self) -> None:
        """Zero the internal buffer. Safe to call multiple times."""
        if not self._cleared:
            for i in range(len(self._buf)):
                self._buf[i] = 0
            self._cleared = True

    def __enter__(self) -> "SecurePassword":
        return self

    def __exit__(self, *exc) -> bool:
        self.clear()
        return False

    def __del__(self) -> None:
        self.clear()

    def __bool__(self) -> bool:
        """True if password is non-empty and not yet cleared."""
        return not self._cleared and len(self._buf) > 0

    def __repr__(self) -> str:
        if self._cleared:
            return "SecurePassword(cleared)"
        return "SecurePassword(***)"
