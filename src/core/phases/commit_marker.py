"""Phase 7: Commit marker — destination-side proof of completeness.

After a backup has been written, manifested, and verified on a given
destination, the writer drops a small ``.wbcommit`` file alongside the
backup. The presence of a valid ``.wbcommit`` is the **sole authority**
for whether a backup on a destination is complete and restorable.

Without a valid marker, the backup is treated as orphaned (interrupted
write, failed verify, foreign artefact) and is a candidate for deletion
at the start of the next run.

Format
------
JSON, UTF-8, ~250 bytes:

    {
      "version": 1,
      "completed_at": "2026-05-08T10:46:17.589Z",
      "manifest_sha256": "29396e9366a9ba42...",
      "files_count": 262615,
      "destination_label": "storage",
      "writer_version": "3.3.14",
      "hmac_sha256": "..."
    }

Anti-tamper
-----------
- ``hmac_sha256`` covers all other fields, signed with the per-install
  key from :mod:`src.security.integrity_check`. The key is wrapped by
  Windows DPAPI so a forged marker requires the same DPAPI access as a
  forged ``app_checksums.json``.
- ``manifest_sha256`` binds the marker to the **specific** ``.wbverify``
  next to it. Lifting a valid ``.wbcommit`` from backup A onto backup B
  fails the manifest-binding check.
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from src.security.integrity_check import get_app_hmac_key

logger = logging.getLogger(__name__)

COMMIT_MARKER_SUFFIX: str = ".wbcommit"
COMMIT_MARKER_VERSION: int = 1

# Stable destination labels — persisted on disk, do not rename.
DESTINATION_STORAGE: str = "storage"
DESTINATION_MIRROR_PREFIX: str = "mirror_"  # e.g. "mirror_1", "mirror_2"

# Length of a SHA-256 hex digest. Manifest checksums and HMACs are
# both this size; rejecting anything else avoids parsing a malformed
# marker into a confident-looking dict.
_HEX_DIGEST_LEN: int = 64


def _default_writer_version() -> str:
    """Return the current application version string.

    Imported lazily so tests that patch ``src.__version__`` see the
    patched value, and so this module does not pull the whole package
    init at import time.
    """
    from src import __version__

    return __version__


def commit_marker_path(backup_path: Path) -> Path:
    """Return the ``.wbcommit`` sibling path for a backup.

    Works for both flat-directory backups and ``.tar.wbenc`` archives:

    - ``E:\\…\\BLoic_FULL_2026-05-08_102226``
        → ``E:\\…\\BLoic_FULL_2026-05-08_102226.wbcommit``
    - ``E:\\…\\BLoic_FULL_2026-05-08_102226.tar.wbenc``
        → ``E:\\…\\BLoic_FULL_2026-05-08_102226.tar.wbenc.wbcommit``

    Args:
        backup_path: The backup directory or archive file.

    Returns:
        Sibling path with the ``.wbcommit`` suffix appended.

    Raises:
        TypeError: If ``backup_path`` is not a ``Path`` instance.
    """
    if not isinstance(backup_path, Path):
        raise TypeError(f"backup_path must be a Path, got {type(backup_path).__name__}")
    return backup_path.parent / (backup_path.name + COMMIT_MARKER_SUFFIX)


def _payload_to_sign(payload: dict) -> bytes:
    """Serialise the payload deterministically for HMAC computation.

    The ``hmac_sha256`` field itself is excluded (chicken-and-egg) and
    keys are sorted so the byte representation is stable across Python
    versions and dict insertion orders.
    """
    signable = {k: v for k, v in payload.items() if k != "hmac_sha256"}
    return json.dumps(
        signable,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _compute_marker_hmac(payload: dict) -> str:
    """Compute HMAC-SHA256 over ``payload`` (excluding the HMAC field).

    Returns:
        Lowercase hex digest (64 chars).

    Raises:
        OSError: if the HMAC key is unavailable.
    """
    key = get_app_hmac_key()
    msg = _payload_to_sign(payload)
    return hmac.new(key, msg, hashlib.sha256).hexdigest()


def build_commit_marker(
    manifest_sha256: str,
    files_count: int,
    destination_label: str,
    writer_version: str | None = None,
) -> dict:
    """Assemble a signed commit-marker payload (in-memory).

    Performs strict input validation up front so we never produce a
    marker that will later be rejected by ``read_commit_marker``.

    Args:
        manifest_sha256: 64-char hex digest — the ``total_checksum`` of
            the ``.wbverify`` this commit attests to.
        files_count: Number of files in the manifest.  Must be ≥ 0.
        destination_label: Stable label identifying the destination
            (e.g. ``"storage"``, ``"mirror_1"``).
        writer_version: Application version string. Defaults to the
            running app's ``__version__`` when ``None``.

    Returns:
        Dict containing all fields plus ``hmac_sha256``.

    Raises:
        TypeError: If types are wrong.
        ValueError: If any field fails validation.
    """
    if not isinstance(manifest_sha256, str):
        raise TypeError(f"manifest_sha256 must be str, got {type(manifest_sha256).__name__}")
    if len(manifest_sha256) != _HEX_DIGEST_LEN or any(
        c not in "0123456789abcdef" for c in manifest_sha256.lower()
    ):
        raise ValueError(
            f"manifest_sha256 must be a {_HEX_DIGEST_LEN}-char hex digest, "
            f"got {manifest_sha256!r}"
        )
    if not isinstance(files_count, int) or isinstance(files_count, bool):
        raise TypeError(f"files_count must be int, got {type(files_count).__name__}")
    if files_count < 0:
        raise ValueError(f"files_count must be >= 0, got {files_count}")
    if not isinstance(destination_label, str) or not destination_label:
        raise ValueError(f"destination_label must be a non-empty string, got {destination_label!r}")

    if writer_version is None:
        writer_version = _default_writer_version()
    if not isinstance(writer_version, str) or not writer_version:
        raise ValueError(f"writer_version must be a non-empty string, got {writer_version!r}")

    payload: dict = {
        "version": COMMIT_MARKER_VERSION,
        "completed_at": (datetime.now(UTC).isoformat().replace("+00:00", "Z")),
        "manifest_sha256": manifest_sha256.lower(),
        "files_count": files_count,
        "destination_label": destination_label,
        "writer_version": writer_version,
    }
    payload["hmac_sha256"] = _compute_marker_hmac(payload)
    return payload


def serialise_commit_marker(payload: dict) -> bytes:
    """Encode a marker payload as UTF-8 JSON bytes for upload to a backend.

    Used when writing to a remote backend (S3, SFTP) where atomic local
    rename is not applicable. The local writer uses ``write_commit_marker``
    instead.

    Args:
        payload: Dict from :func:`build_commit_marker`.

    Returns:
        UTF-8 encoded JSON bytes (pretty-printed for human inspection).

    Raises:
        ValueError: If ``payload`` lacks the ``hmac_sha256`` field.
    """
    if "hmac_sha256" not in payload:
        raise ValueError("payload is not signed: hmac_sha256 missing")
    return json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True).encode("utf-8")


def write_commit_marker(
    backup_path: Path,
    manifest_sha256: str,
    files_count: int,
    destination_label: str,
    writer_version: str | None = None,
) -> Path:
    """Atomically write a ``.wbcommit`` next to a local backup.

    Uses the same crash-safe pattern as ``ConfigManager._atomic_write``:
    write to a sibling ``.tmp`` with ``fsync``, then ``os.replace``. A
    crash before the rename leaves no marker (so the backup is
    correctly seen as orphaned at the next scan).

    For remote backends (S3, SFTP), use :func:`serialise_commit_marker`
    and upload the bytes through the backend's ``upload_file``.

    Args:
        backup_path: The backup directory or ``.tar.wbenc`` file.
            Its parent directory must already exist.
        manifest_sha256: ``total_checksum`` of the corresponding
            ``.wbverify``.
        files_count: Number of files in the manifest.
        destination_label: Where this commit lives
            (e.g. ``"storage"``, ``"mirror_1"``).
        writer_version: Optional version override (defaults to running
            app's ``__version__``).

    Returns:
        Path to the written ``.wbcommit``.

    Raises:
        TypeError, ValueError: From :func:`build_commit_marker`.
        FileNotFoundError: If the backup's parent directory does not
            exist.
        OSError: On I/O failure during write/rename.
    """
    if not isinstance(backup_path, Path):
        raise TypeError(f"backup_path must be a Path, got {type(backup_path).__name__}")
    parent = backup_path.parent
    if not parent.exists():
        raise FileNotFoundError(f"Backup parent directory does not exist: {parent}")

    payload = build_commit_marker(
        manifest_sha256=manifest_sha256,
        files_count=files_count,
        destination_label=destination_label,
        writer_version=writer_version,
    )

    marker_path = commit_marker_path(backup_path)
    tmp_path = marker_path.with_name(marker_path.name + ".tmp")

    data = serialise_commit_marker(payload)

    # Crash-safe write: O_TRUNC to clobber any leftover .tmp from a
    # previous failed run, fsync to push bytes onto physical media,
    # then os.replace for atomic publish on NTFS/POSIX (same volume).
    fd = os.open(
        str(tmp_path),
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)

    try:
        os.replace(tmp_path, marker_path)
    except OSError:
        # Best-effort cleanup of the .tmp so a failed rename doesn't
        # leave a stray file on the destination. Swallow secondary
        # errors so the original OSError surfaces.
        with contextlib.suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise

    logger.info(
        "Commit marker written: %s (%s, %d files)",
        marker_path.name,
        destination_label,
        files_count,
    )
    return marker_path


def read_commit_marker(marker_path: Path) -> dict | None:
    """Load and HMAC-verify a ``.wbcommit`` file.

    Returns ``None`` (not an exception) for any "not a valid marker"
    case so callers can treat orphans uniformly: missing file, malformed
    JSON, wrong version, HMAC mismatch all collapse to "no commit".
    Logs at WARNING level for malformed cases so an attacker cannot
    silently feed bogus markers without leaving a trace.

    Args:
        marker_path: Path to the ``.wbcommit`` file.

    Returns:
        Validated payload dict on success, ``None`` otherwise.
    """
    if not isinstance(marker_path, Path):
        raise TypeError(f"marker_path must be a Path, got {type(marker_path).__name__}")
    if not marker_path.exists():
        return None

    try:
        raw = marker_path.read_text(encoding="utf-8")
    except OSError as e:
        logger.warning("Could not read commit marker %s: %s", marker_path, e)
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("Commit marker %s is not valid JSON: %s", marker_path, e)
        return None

    if not isinstance(payload, dict):
        logger.warning("Commit marker %s is not a JSON object", marker_path)
        return None

    expected_hmac = payload.get("hmac_sha256")
    if not isinstance(expected_hmac, str) or len(expected_hmac) != _HEX_DIGEST_LEN:
        logger.warning("Commit marker %s has missing or malformed HMAC", marker_path)
        return None

    try:
        actual_hmac = _compute_marker_hmac(payload)
    except OSError as e:
        # HMAC key unavailable — treat as untrusted rather than crash.
        logger.error(
            "Cannot verify commit marker %s: HMAC key unavailable (%s)",
            marker_path,
            e,
        )
        return None

    if not hmac.compare_digest(expected_hmac, actual_hmac):
        logger.warning(
            "Commit marker %s HMAC mismatch — refusing to trust it",
            marker_path,
        )
        return None

    if payload.get("version") != COMMIT_MARKER_VERSION:
        logger.warning(
            "Commit marker %s has version %r (expected %d) — refusing",
            marker_path,
            payload.get("version"),
            COMMIT_MARKER_VERSION,
        )
        return None

    # Sanity-check the structural fields now that we know the marker
    # is authentic. Any malformed field means the marker was signed
    # with our key but produced by a faulty writer; refuse it rather
    # than risk a downstream crash on wrong types.
    manifest = payload.get("manifest_sha256")
    if not isinstance(manifest, str) or len(manifest) != _HEX_DIGEST_LEN:
        logger.warning("Commit marker %s has malformed manifest_sha256", marker_path)
        return None
    files_count = payload.get("files_count")
    if not isinstance(files_count, int) or isinstance(files_count, bool):
        logger.warning("Commit marker %s has malformed files_count", marker_path)
        return None
    if files_count < 0:
        logger.warning("Commit marker %s has negative files_count", marker_path)
        return None
    if not isinstance(payload.get("destination_label"), str):
        logger.warning("Commit marker %s has malformed destination_label", marker_path)
        return None

    return payload


def is_backup_committed(backup_path: Path) -> bool:
    """Return True iff a valid ``.wbcommit`` sits next to the backup.

    Args:
        backup_path: Backup directory or ``.tar.wbenc`` file.

    Returns:
        True only if the marker exists, parses, HMAC-verifies, and has
        the expected version. Anything else returns False.
    """
    return read_commit_marker(commit_marker_path(backup_path)) is not None


def verify_commit_marker_against_manifest(
    backup_path: Path,
    manifest_sha256: str,
) -> tuple[bool, str]:
    """Check that a marker's ``manifest_sha256`` matches a known value.

    Defends against marker transposition: an attacker copying a valid
    ``.wbcommit`` from backup A onto backup B would have a valid HMAC
    but a manifest checksum that doesn't match B's actual ``.wbverify``.

    Args:
        backup_path: Backup directory or archive file.
        manifest_sha256: Expected ``total_checksum`` from the
            ``.wbverify`` belonging to ``backup_path``.

    Returns:
        ``(ok, reason)``. ``ok`` is True only if the marker exists,
        HMAC-verifies, and binds to ``manifest_sha256``.
    """
    marker = read_commit_marker(commit_marker_path(backup_path))
    if marker is None:
        return False, "no valid commit marker"
    bound = marker.get("manifest_sha256", "")
    if bound != manifest_sha256.lower():
        return False, (
            f"marker manifest_sha256 {bound[:16]}... "
            f"does not match expected {manifest_sha256[:16].lower()}..."
        )
    return True, "ok"
