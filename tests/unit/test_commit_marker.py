"""Tests for the commit-marker module.

The commit marker (``.wbcommit``) is the destination-side proof that a
backup is complete. Its security properties — HMAC binding to the
local install, manifest-checksum binding to defeat transposition,
strict round-trip equality — must be verified exhaustively. A bug here
would either hide failed backups (false-positive complete) or reject
valid ones (false-negative — restore refusal).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.phases.commit_marker import (
    COMMIT_MARKER_SUFFIX,
    COMMIT_MARKER_VERSION,
    DESTINATION_STORAGE,
    _compute_marker_hmac,
    _payload_to_sign,
    build_commit_marker,
    commit_marker_path,
    is_backup_committed,
    read_commit_marker,
    serialise_commit_marker,
    verify_commit_marker_against_manifest,
    write_commit_marker,
)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


# A fixed 32-byte key used by the tests. Patched into
# ``get_app_hmac_key`` so we never touch the real DPAPI-wrapped key
# (which would (a) require Windows + DPAPI online, (b) leak test
# artefacts into the user's real ``%APPDATA%``).
_FAKE_KEY = b"\x42" * 32

# Valid 64-char hex digest — used wherever a manifest_sha256 is needed.
_VALID_DIGEST = "a" * 64


@pytest.fixture(autouse=True)
def _patch_hmac_key():
    """Replace the per-install HMAC key with a fixed test value.

    Patches both the source location AND the import alias inside
    ``commit_marker``: the latter is captured at module-load time so
    patching the original symbol alone would not propagate.
    """
    with patch(
        "src.core.phases.commit_marker.get_app_hmac_key",
        return_value=_FAKE_KEY,
    ):
        yield


@pytest.fixture
def backup_dir(tmp_path: Path) -> Path:
    """Create an empty backup directory and return its path."""
    p = tmp_path / "BLoic_FULL_2026-05-08_102226"
    p.mkdir()
    return p


@pytest.fixture
def encrypted_archive(tmp_path: Path) -> Path:
    """Create a fake .tar.wbenc archive file."""
    p = tmp_path / "BLoic_FULL_2026-05-08_102226.tar.wbenc"
    p.write_bytes(b"WBEC\x01" + b"\x00" * 32)
    return p


# ---------------------------------------------------------------------------
# commit_marker_path
# ---------------------------------------------------------------------------


class TestCommitMarkerPath:
    """Path helper handles directory and archive backups equally."""

    def test_directory_backup_yields_sibling_marker(self, backup_dir: Path) -> None:
        marker = commit_marker_path(backup_dir)
        assert marker.name == backup_dir.name + COMMIT_MARKER_SUFFIX
        assert marker.parent == backup_dir.parent

    def test_encrypted_archive_yields_sibling_marker(self, encrypted_archive: Path) -> None:
        marker = commit_marker_path(encrypted_archive)
        # The full archive name (including ``.tar.wbenc``) is preserved
        # so the marker is unambiguous about which file it certifies.
        assert marker.name == encrypted_archive.name + COMMIT_MARKER_SUFFIX
        assert marker.parent == encrypted_archive.parent

    def test_rejects_non_path_argument(self) -> None:
        with pytest.raises(TypeError, match="must be a Path"):
            commit_marker_path("E:/Backup Manager/foo")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# build_commit_marker — input validation
# ---------------------------------------------------------------------------


class TestBuildCommitMarkerValidation:
    """Strict validation: a marker that would fail to read is never built."""

    def test_valid_inputs_produce_signed_payload(self) -> None:
        payload = build_commit_marker(
            manifest_sha256=_VALID_DIGEST,
            files_count=42,
            destination_label=DESTINATION_STORAGE,
            writer_version="3.3.14",
        )
        assert payload["version"] == COMMIT_MARKER_VERSION
        assert payload["manifest_sha256"] == _VALID_DIGEST
        assert payload["files_count"] == 42
        assert payload["destination_label"] == DESTINATION_STORAGE
        assert payload["writer_version"] == "3.3.14"
        assert "completed_at" in payload
        assert payload["completed_at"].endswith("Z")
        assert len(payload["hmac_sha256"]) == 64

    def test_default_writer_version_uses_app_version(self) -> None:
        with patch(
            "src.core.phases.commit_marker._default_writer_version",
            return_value="9.9.9-test",
        ):
            payload = build_commit_marker(
                manifest_sha256=_VALID_DIGEST,
                files_count=1,
                destination_label=DESTINATION_STORAGE,
            )
        assert payload["writer_version"] == "9.9.9-test"

    @pytest.mark.parametrize(
        "bad_digest",
        [
            "",
            "tooshort",
            "z" * 64,  # right length, non-hex
            "A" * 64 + "0",  # too long
        ],
    )
    def test_rejects_malformed_manifest_sha256(self, bad_digest: str) -> None:
        with pytest.raises(ValueError, match="manifest_sha256"):
            build_commit_marker(
                manifest_sha256=bad_digest,
                files_count=1,
                destination_label=DESTINATION_STORAGE,
            )

    def test_rejects_non_string_manifest_sha256(self) -> None:
        with pytest.raises(TypeError, match="manifest_sha256"):
            build_commit_marker(
                manifest_sha256=12345,  # type: ignore[arg-type]
                files_count=1,
                destination_label=DESTINATION_STORAGE,
            )

    def test_rejects_negative_files_count(self) -> None:
        with pytest.raises(ValueError, match="files_count"):
            build_commit_marker(
                manifest_sha256=_VALID_DIGEST,
                files_count=-1,
                destination_label=DESTINATION_STORAGE,
            )

    def test_rejects_bool_files_count(self) -> None:
        # Python's bool is a subclass of int; without explicit check
        # ``files_count=True`` would silently become ``1``.
        with pytest.raises(TypeError, match="files_count"):
            build_commit_marker(
                manifest_sha256=_VALID_DIGEST,
                files_count=True,  # type: ignore[arg-type]
                destination_label=DESTINATION_STORAGE,
            )

    def test_rejects_empty_destination_label(self) -> None:
        with pytest.raises(ValueError, match="destination_label"):
            build_commit_marker(
                manifest_sha256=_VALID_DIGEST,
                files_count=1,
                destination_label="",
            )

    def test_rejects_empty_writer_version(self) -> None:
        with pytest.raises(ValueError, match="writer_version"):
            build_commit_marker(
                manifest_sha256=_VALID_DIGEST,
                files_count=1,
                destination_label=DESTINATION_STORAGE,
                writer_version="",
            )

    def test_normalises_uppercase_digest_to_lowercase(self) -> None:
        payload = build_commit_marker(
            manifest_sha256="A" * 64,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        assert payload["manifest_sha256"] == "a" * 64


# ---------------------------------------------------------------------------
# Round-trip: write → read
# ---------------------------------------------------------------------------


class TestWriteReadRoundTrip:
    """A freshly-written marker reads back identically and verifies."""

    def test_round_trip_preserves_all_fields(self, backup_dir: Path) -> None:
        marker_path = write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=262615,
            destination_label=DESTINATION_STORAGE,
            writer_version="3.3.14",
        )
        assert marker_path.exists()
        loaded = read_commit_marker(marker_path)
        assert loaded is not None
        assert loaded["manifest_sha256"] == _VALID_DIGEST
        assert loaded["files_count"] == 262615
        assert loaded["destination_label"] == DESTINATION_STORAGE
        assert loaded["writer_version"] == "3.3.14"
        assert loaded["version"] == COMMIT_MARKER_VERSION

    def test_is_backup_committed_returns_true_after_write(self, backup_dir: Path) -> None:
        write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        assert is_backup_committed(backup_dir) is True

    def test_is_backup_committed_false_when_marker_absent(self, backup_dir: Path) -> None:
        assert is_backup_committed(backup_dir) is False

    def test_marker_filename_is_correct(self, backup_dir: Path) -> None:
        marker_path = write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        assert marker_path.name == backup_dir.name + COMMIT_MARKER_SUFFIX

    def test_round_trip_for_encrypted_archive(self, encrypted_archive: Path) -> None:
        marker_path = write_commit_marker(
            backup_path=encrypted_archive,
            manifest_sha256=_VALID_DIGEST,
            files_count=42,
            destination_label="mirror_1",
        )
        assert marker_path.exists()
        assert marker_path.name == encrypted_archive.name + COMMIT_MARKER_SUFFIX
        assert is_backup_committed(encrypted_archive) is True


# ---------------------------------------------------------------------------
# HMAC defeats forgery
# ---------------------------------------------------------------------------


class TestHmacAntiTamper:
    """Modifying any signed field after write must invalidate the marker."""

    def _write_and_load_raw(self, backup_dir: Path) -> tuple[Path, dict]:
        marker_path = write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=10,
            destination_label=DESTINATION_STORAGE,
        )
        return marker_path, json.loads(marker_path.read_text(encoding="utf-8"))

    @pytest.mark.parametrize(
        "field,new_value",
        [
            ("manifest_sha256", "b" * 64),
            ("files_count", 9999),
            ("destination_label", "mirror_1"),
            ("writer_version", "3.3.13"),
            ("completed_at", "2030-01-01T00:00:00Z"),
            ("version", 2),
        ],
    )
    def test_field_tamper_invalidates_marker(
        self,
        backup_dir: Path,
        field: str,
        new_value: object,
    ) -> None:
        marker_path, payload = self._write_and_load_raw(backup_dir)
        payload[field] = new_value
        # Re-serialise WITHOUT recomputing the HMAC — that's the
        # attack: the attacker doesn't have the key.
        marker_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        assert read_commit_marker(marker_path) is None

    def test_hmac_field_replaced_with_garbage_invalidates(self, backup_dir: Path) -> None:
        marker_path, payload = self._write_and_load_raw(backup_dir)
        payload["hmac_sha256"] = "0" * 64
        marker_path.write_text(json.dumps(payload), encoding="utf-8")
        assert read_commit_marker(marker_path) is None

    def test_hmac_field_truncated_invalidates(self, backup_dir: Path) -> None:
        marker_path, payload = self._write_and_load_raw(backup_dir)
        payload["hmac_sha256"] = "abc"  # not 64 chars
        marker_path.write_text(json.dumps(payload), encoding="utf-8")
        assert read_commit_marker(marker_path) is None

    def test_marker_signed_with_different_key_rejected(self, backup_dir: Path) -> None:
        # Write with the fixture key.
        marker_path = write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        # Read back under a DIFFERENT key (simulating a marker copied
        # from another machine / install).
        with patch(
            "src.core.phases.commit_marker.get_app_hmac_key",
            return_value=b"\xff" * 32,
        ):
            assert read_commit_marker(marker_path) is None


# ---------------------------------------------------------------------------
# Manifest binding defeats marker transposition
# ---------------------------------------------------------------------------


class TestManifestBinding:
    """A valid marker for backup A must be invalid when placed beside B."""

    def test_marker_binds_to_specific_manifest_sha256(self, backup_dir: Path) -> None:
        write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        ok, reason = verify_commit_marker_against_manifest(backup_dir, _VALID_DIGEST)
        assert ok, reason

    def test_marker_for_different_manifest_rejected(self, backup_dir: Path) -> None:
        write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        ok, reason = verify_commit_marker_against_manifest(backup_dir, "b" * 64)
        assert not ok
        assert "does not match" in reason

    def test_no_marker_yields_clear_reason(self, backup_dir: Path) -> None:
        ok, reason = verify_commit_marker_against_manifest(backup_dir, _VALID_DIGEST)
        assert not ok
        assert "no valid commit marker" in reason


# ---------------------------------------------------------------------------
# Read robustness
# ---------------------------------------------------------------------------


class TestReadRobustness:
    """Every malformed-input path must return None, not crash."""

    def test_missing_file_returns_none(self, tmp_path: Path) -> None:
        assert read_commit_marker(tmp_path / "nonexistent.wbcommit") is None

    def test_invalid_json_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "bogus.wbcommit"
        path.write_text("not json at all", encoding="utf-8")
        assert read_commit_marker(path) is None

    def test_json_array_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "bogus.wbcommit"
        path.write_text("[1, 2, 3]", encoding="utf-8")
        assert read_commit_marker(path) is None

    def test_missing_hmac_field_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "bogus.wbcommit"
        path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "completed_at": "2026-05-08T00:00:00Z",
                    "manifest_sha256": _VALID_DIGEST,
                    "files_count": 1,
                    "destination_label": DESTINATION_STORAGE,
                    "writer_version": "3.3.14",
                }
            ),
            encoding="utf-8",
        )
        assert read_commit_marker(path) is None

    def test_wrong_version_returns_none(self, backup_dir: Path) -> None:
        # Sign a payload with version=2 using the fixture key, then
        # read back: the HMAC matches but version is unsupported.
        marker_path, payload = (
            commit_marker_path(backup_dir),
            {
                "version": 2,
                "completed_at": "2026-05-08T00:00:00Z",
                "manifest_sha256": _VALID_DIGEST,
                "files_count": 1,
                "destination_label": DESTINATION_STORAGE,
                "writer_version": "3.3.14",
            },
        )
        payload["hmac_sha256"] = _compute_marker_hmac(payload)
        marker_path.write_text(json.dumps(payload), encoding="utf-8")
        assert read_commit_marker(marker_path) is None

    def test_negative_files_count_in_signed_payload_returns_none(self, backup_dir: Path) -> None:
        marker_path = commit_marker_path(backup_dir)
        payload = {
            "version": 1,
            "completed_at": "2026-05-08T00:00:00Z",
            "manifest_sha256": _VALID_DIGEST,
            "files_count": -5,
            "destination_label": DESTINATION_STORAGE,
            "writer_version": "3.3.14",
        }
        payload["hmac_sha256"] = _compute_marker_hmac(payload)
        marker_path.write_text(json.dumps(payload), encoding="utf-8")
        assert read_commit_marker(marker_path) is None

    def test_malformed_manifest_sha256_in_signed_payload_returns_none(
        self, backup_dir: Path
    ) -> None:
        marker_path = commit_marker_path(backup_dir)
        payload = {
            "version": 1,
            "completed_at": "2026-05-08T00:00:00Z",
            "manifest_sha256": "tooshort",
            "files_count": 1,
            "destination_label": DESTINATION_STORAGE,
            "writer_version": "3.3.14",
        }
        payload["hmac_sha256"] = _compute_marker_hmac(payload)
        marker_path.write_text(json.dumps(payload), encoding="utf-8")
        assert read_commit_marker(marker_path) is None

    def test_rejects_non_path_argument(self) -> None:
        with pytest.raises(TypeError, match="must be a Path"):
            read_commit_marker("foo.wbcommit")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Atomicity of write_commit_marker
# ---------------------------------------------------------------------------


class TestWriteAtomicity:
    """No leftover .tmp file after a successful write."""

    def test_no_tmp_file_remains_after_success(self, backup_dir: Path) -> None:
        marker_path = write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        tmp = marker_path.with_name(marker_path.name + ".tmp")
        assert not tmp.exists()
        assert marker_path.exists()

    def test_overwrites_existing_marker(self, backup_dir: Path) -> None:
        # Write a first marker, then a second one with different
        # files_count: the second must replace the first cleanly.
        write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=99,
            destination_label=DESTINATION_STORAGE,
        )
        loaded = read_commit_marker(commit_marker_path(backup_dir))
        assert loaded is not None
        assert loaded["files_count"] == 99

    def test_missing_parent_directory_raises(self, tmp_path: Path) -> None:
        ghost = tmp_path / "does_not_exist" / "BLoic_FULL_xxx"
        with pytest.raises(FileNotFoundError, match="parent directory"):
            write_commit_marker(
                backup_path=ghost,
                manifest_sha256=_VALID_DIGEST,
                files_count=1,
                destination_label=DESTINATION_STORAGE,
            )

    def test_rejects_non_path_argument(self) -> None:
        with pytest.raises(TypeError, match="must be a Path"):
            write_commit_marker(
                backup_path="foo",  # type: ignore[arg-type]
                manifest_sha256=_VALID_DIGEST,
                files_count=1,
                destination_label=DESTINATION_STORAGE,
            )

    def test_leaves_no_tmp_file_when_rename_fails(self, backup_dir: Path, monkeypatch) -> None:
        """A simulated rename failure must clean up the .tmp."""

        def _boom(*_a, **_kw):
            raise OSError("simulated rename failure")

        monkeypatch.setattr("src.core.phases.commit_marker.os.replace", _boom)
        with pytest.raises(OSError, match="simulated rename"):
            write_commit_marker(
                backup_path=backup_dir,
                manifest_sha256=_VALID_DIGEST,
                files_count=1,
                destination_label=DESTINATION_STORAGE,
            )
        marker = commit_marker_path(backup_dir)
        tmp = marker.with_name(marker.name + ".tmp")
        # Final marker must NOT exist (rename never succeeded).
        assert not marker.exists()
        # .tmp must NOT linger on disk.
        assert not tmp.exists()


# ---------------------------------------------------------------------------
# Serialise (for remote backends)
# ---------------------------------------------------------------------------


class TestSerialise:
    """Bytes produced by ``serialise_commit_marker`` are valid JSON."""

    def test_serialised_bytes_are_valid_json(self) -> None:
        payload = build_commit_marker(
            manifest_sha256=_VALID_DIGEST,
            files_count=42,
            destination_label=DESTINATION_STORAGE,
        )
        data = serialise_commit_marker(payload)
        decoded = json.loads(data.decode("utf-8"))
        assert decoded["manifest_sha256"] == _VALID_DIGEST
        assert decoded["hmac_sha256"] == payload["hmac_sha256"]

    def test_serialise_unsigned_payload_raises(self) -> None:
        payload = {
            "version": 1,
            "manifest_sha256": _VALID_DIGEST,
            "files_count": 1,
            "destination_label": DESTINATION_STORAGE,
        }
        with pytest.raises(ValueError, match="hmac_sha256 missing"):
            serialise_commit_marker(payload)


# ---------------------------------------------------------------------------
# HMAC computation determinism
# ---------------------------------------------------------------------------


class TestHmacDeterminism:
    """Same payload + same key always produces same HMAC."""

    def test_payload_to_sign_excludes_hmac_field(self) -> None:
        payload = {
            "version": 1,
            "files_count": 5,
            "hmac_sha256": "ignored",
        }
        signed = _payload_to_sign(payload)
        # The HMAC field MUST not appear in what we sign — otherwise
        # we'd have a chicken-and-egg dependency.
        assert b"hmac_sha256" not in signed
        assert b"ignored" not in signed

    def test_payload_to_sign_is_key_order_invariant(self) -> None:
        a = {"version": 1, "files_count": 5}
        b = {"files_count": 5, "version": 1}
        assert _payload_to_sign(a) == _payload_to_sign(b)

    def test_hmac_matches_manual_computation(self) -> None:
        payload = {
            "version": 1,
            "files_count": 5,
            "manifest_sha256": _VALID_DIGEST,
            "destination_label": DESTINATION_STORAGE,
            "writer_version": "3.3.14",
            "completed_at": "2026-05-08T00:00:00Z",
        }
        actual = _compute_marker_hmac(payload)
        expected = hmac.new(
            _FAKE_KEY,
            _payload_to_sign(payload),
            hashlib.sha256,
        ).hexdigest()
        assert actual == expected

    def test_hmac_changes_when_any_signed_field_changes(self) -> None:
        base = {
            "version": 1,
            "files_count": 5,
            "manifest_sha256": _VALID_DIGEST,
            "destination_label": DESTINATION_STORAGE,
            "writer_version": "3.3.14",
            "completed_at": "2026-05-08T00:00:00Z",
        }
        h_base = _compute_marker_hmac(base)
        for field in (
            "version",
            "files_count",
            "manifest_sha256",
            "destination_label",
            "writer_version",
            "completed_at",
        ):
            tweaked = dict(base)
            # Tweak the field with a different but type-compatible value.
            current = tweaked[field]
            if isinstance(current, int):
                tweaked[field] = current + 1
            else:
                tweaked[field] = str(current) + "x"
            assert _compute_marker_hmac(tweaked) != h_base, field


# ---------------------------------------------------------------------------
# Encoding edge cases
# ---------------------------------------------------------------------------


class TestEncoding:
    """JSON output must round-trip with non-ASCII destination labels."""

    def test_unicode_destination_label_round_trips(self, backup_dir: Path) -> None:
        write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label="storage_éàü_漢",
        )
        loaded = read_commit_marker(commit_marker_path(backup_dir))
        assert loaded is not None
        assert loaded["destination_label"] == "storage_éàü_漢"

    def test_marker_file_uses_utf8_encoding(self, backup_dir: Path) -> None:
        write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label="ünïcødé",
        )
        # If we wrote in cp1252 by accident, this read in utf-8 would
        # fail or mis-decode. Round-trip via raw bytes proves utf-8.
        raw = commit_marker_path(backup_dir).read_bytes()
        decoded = raw.decode("utf-8")
        assert "ünïcødé" in decoded


# ---------------------------------------------------------------------------
# File-mode safety
# ---------------------------------------------------------------------------


class TestFilePermissions:
    """The .wbcommit is created with restrictive mode (best-effort).

    Windows ignores POSIX modes silently — the assertion is informational
    and only meaningful on POSIX, but ``os.open(...mode=0o600)`` is the
    same call we use everywhere else for sensitive artefacts and that
    consistency is the point of the test.
    """

    def test_file_exists_after_write(self, backup_dir: Path) -> None:
        marker_path = write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        assert marker_path.is_file()

    @pytest.mark.skipif(os.name == "nt", reason="POSIX-only mode bits")
    def test_marker_mode_is_owner_only_on_posix(self, backup_dir: Path) -> None:
        marker_path = write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256=_VALID_DIGEST,
            files_count=1,
            destination_label=DESTINATION_STORAGE,
        )
        mode = marker_path.stat().st_mode & 0o777
        # Group / world bits must be clear.
        assert mode & 0o077 == 0
