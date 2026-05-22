"""Centralized exception definitions for Backup Manager."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.storage._fs_utils import Residual


class CancelledError(Exception):
    """Raised when a backup is cancelled by the user."""

    pass


class WriteError(Exception):
    """Raised when a file write or upload fails during backup.

    Args:
        file_path: The file that failed to write/upload.
        original: The underlying exception that caused the failure.
    """

    def __init__(self, file_path: str, original: Exception):
        self.file_path = file_path
        self.original = original
        super().__init__(f"Failed to write {file_path}: {original}")


class SecretsProtectionError(Exception):
    """Raised when a profile secret cannot be encrypted before persistence.

    Refusing to save is the only safe response: silently logging a
    warning and writing the plaintext to disk (the previous behaviour)
    would leak the password into a JSON profile file that the user
    reasonably assumes is encrypted.

    Args:
        field: Dotted path identifying the field that failed, e.g.
            ``storage.password`` or ``email.smtp_password``.
        original: The underlying exception raised by ``store_password``.
    """

    def __init__(self, field: str, original: Exception):
        self.field = field
        self.original = original
        super().__init__(
            f"Failed to encrypt secret field {field!r}: {original}. "
            f"Profile NOT saved to disk to avoid leaking plaintext."
        )


class PrecheckUserTimeoutError(Exception):
    """Raised when the scheduler's "destinations unavailable" modal
    is left unanswered until the hard timeout fires.

    The condition is a user-absence event, not a backup failure: the
    backup pipeline never started because the user could not confirm
    that the targets were back online. Conflating this with a real
    crash incremented ``crash_recovery_attempts`` on every overnight
    drift, eventually tripping the circuit breaker for profiles that
    had no actual integrity problem (18/05/2026 incident).

    The scheduler classifies this exception as ``skipped`` in the
    journal (same bucket as the concurrent-run case) and explicitly
    bypasses the retry budget — re-prompting in 2 minutes when the
    user is asleep or away from the desk is pure noise.

    Args:
        profile_name: Human-readable name shown in the journal /
            email subject / tray notification.
        timeout_seconds: How long the prompt was offered before the
            scheduler thread reclaimed itself. Used by surface UX
            to compose a meaningful message.
    """

    def __init__(self, profile_name: str, timeout_seconds: int):
        self.profile_name = profile_name
        self.timeout_seconds = timeout_seconds
        minutes = timeout_seconds // 60
        super().__init__(
            f"Precheck prompt for '{profile_name}' timed out after "
            f"{timeout_seconds}s ({minutes} min) — no user response. "
            f"Backup skipped, not retried."
        )

    def __reduce__(self):
        # Default Exception.__reduce__ pickles the message only — the
        # ``profile_name`` and ``timeout_seconds`` attributes would be
        # lost on a thread-pool or email queue round-trip. Explicit
        # __reduce__ preserves the structured payload.
        return (self.__class__, (self.profile_name, self.timeout_seconds))


class DPAPIUnavailableError(RuntimeError):
    """Raised on Windows when DPAPI is required but unusable.

    The HMAC key (``.integrity_key``) and the per-machine key
    (``machine_key.bin``) are wrapped with DPAPI on Windows so that
    a malware process running as the user still has to issue
    ``CryptUnprotectData`` to read them. If DPAPI is unavailable
    (CryptoNG disabled by group policy, profile corrupted, crypt32.dll
    unreachable) the previous behaviour was to silently write the key
    in clear with a ``logger.warning`` line — which leaves every signed
    artefact on disk readable + forgeable by any process running as the
    user, without any visible signal.

    Refusing to start is the correct response: the user must either
    repair DPAPI, wipe ``%APPDATA%/BackupManager/`` to regenerate
    fresh state, or run the app with ``--allow-plaintext-keys`` to
    accept the degraded posture explicitly.

    Args:
        phase: Where DPAPI failed — ``"wrap"`` (encryption side),
            ``"unwrap"`` (decryption side), or ``"absent"``
            (``_has_dpapi`` returned False on Windows).
        original: Underlying exception raised by ``ctypes``, or ``None``
            when the failure is ``"absent"``.
    """

    def __init__(self, phase: str, original: Exception | None = None):
        self.phase = phase
        self.original = original
        detail = f": {original}" if original is not None else ""
        super().__init__(
            f"DPAPI is required on Windows but unavailable ({phase}){detail}. "
            f"The app refuses to start with plaintext keys. Repair your "
            f"Windows user profile, wipe %APPDATA%/BackupManager/ to start "
            f"fresh, or relaunch with --allow-plaintext-keys to accept the "
            f"degraded security posture."
        )


class StorageDeleteError(Exception):
    """Raised when a storage backend cannot fully delete a backup.

    Carries the list of paths that survived the delete attempt so the
    caller (rotator, cleanup script) can surface actionable diagnostic
    instead of treating a partial delete as success.

    Args:
        target: Backup name (e.g. ``Profile_FULL_2026-04-20_100017``)
            whose deletion left residuals.
        residuals: List of ``Residual`` entries describing each path
            that could not be removed.
    """

    def __init__(self, target: str, residuals: list[Residual]):
        self.target = target
        self.residuals = residuals
        sample = ", ".join(r.path for r in residuals[:3])
        suffix = f" (+{len(residuals) - 3} more)" if len(residuals) > 3 else ""
        super().__init__(
            f"Could not fully delete {target}: {len(residuals)} residual(s). "
            f"First: {sample}{suffix}"
        )
