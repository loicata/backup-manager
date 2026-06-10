"""Centralized exception definitions for Backup Manager."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

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


class PrecheckUserCancelledError(Exception):
    """Raised when the user explicitly clicks "Cancel backup" on the
    scheduler's destinations-unavailable prompt.

    An explicit user decision, not a backup failure: the scheduler
    journals it as ``cancelled`` and bypasses the retry ladder.
    Pre-fix this path raised a plain ``RuntimeError``, which the
    retry classification treated as a transient failure — the ladder
    then re-prompted the user up to five more times over ~6 hours
    after they had already declined (audit 2026-06-10).

    Args:
        profile_name: Human-readable profile name for the journal /
            tray / email surfaces.
        details: Short summary of the failed targets shown in the
            prompt, carried into the journal ``detail`` field so the
            cancellation is diagnosable from the History tab.
    """

    def __init__(self, profile_name: str, details: str = ""):
        self.profile_name = profile_name
        self.details = details
        suffix = f" — {details}" if details else ""
        super().__init__(
            f"Backup cancelled by user: destinations unavailable for '{profile_name}'{suffix}"
        )

    def __reduce__(self):
        # Preserve the structured payload across pickling (thread pools,
        # email queue), mirroring PrecheckUserTimeoutError.
        return (self.__class__, (self.profile_name, self.details))


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


class HMACKeyRegeneratedError(RuntimeError):
    """Raised when the per-install HMAC key had to be regenerated under
    suspicious circumstances.

    Three trigger conditions, all symptoms of "the install identity
    changed unexpectedly" rather than "this is the first run":

    1. An existing key file is present on disk but cannot be decrypted
       by DPAPI (Windows reinstall, profile change, AppData copied
       from another machine).
    2. An existing key file is present but cannot be read at all
       (permission denied, AV quarantine).
    3. The key file is absent but an install sentinel
       (``.integrity_key.installed``) marks the install as having
       previously carried a key (accidental delete, cleanup tool,
       AV quarantine of the key file only).

    Letting ``_get_hmac_key`` silently regenerate in any of those
    cases invalidates every ``.wbcommit`` previously signed with the
    old key. On local destinations those backups are then classified
    as orphans by ``LocalStorage.list_orphan_backups`` and DELETED
    at the next ``_phase_orphan_scan`` — silently from the UI's
    point of view, with only ``Orphan removed`` INFO lines in
    ``backup_manager.log``. Surfacing this exception at the bootstrap
    so the user can abort BEFORE any pipeline runs is the only way
    to avoid that data-loss path.

    Genuine first runs (no key, no sentinel) do NOT raise — they are
    indistinguishable from a fresh install and the regeneration is
    expected. The sentinel is the only way to tell the two scenarios
    apart on a system that previously ran Backup Manager at least once.

    Suppressed entirely when ``_ALLOW_PLAINTEXT_FALLBACK`` is True
    (the user explicitly accepted degraded crypto posture via
    ``--allow-plaintext-keys``): regeneration proceeds without raising
    so the existing CLI escape hatch keeps working.

    Args:
        reason: Human-readable explanation suitable for a modal
            dialog message body.
        prior_key_existed: True when there is evidence (file or
            sentinel) that a key was previously installed. Always
            True at raise time — kept as a structured field for
            handlers that may want to differentiate further.
        prior_key_path: Path where the existing/expected key file
            lives. The handler uses ``prior_key_path.parent`` to
            point the user at any ``.legacy_*`` archive created by
            ``_archive_old_key``.
        cause: Underlying exception when the failure stems from a
            specific OSError (DPAPI unwrap, read denied). ``None``
            for the "sentinel says key existed but file is gone" case.
    """

    def __init__(
        self,
        reason: str,
        prior_key_existed: bool,
        prior_key_path: Path,
        cause: Exception | None = None,
    ) -> None:
        self.reason = reason
        self.prior_key_existed = prior_key_existed
        self.prior_key_path = prior_key_path
        self.cause = cause
        super().__init__(reason)


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
