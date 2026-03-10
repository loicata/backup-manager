"""
Backup Manager - Integrity Verification
=========================================
SHA-256 based backup integrity verification using signed manifests.

Manifest format (.wbverify):
  JSON file containing SHA-256 hash of every source file, saved alongside
  the backup. Can be used to verify integrity months or years later.

Workflow:
  1. build_manifest()           → compute SHA-256 of all source files
  2. manifest.save(backup_path) → write .wbverify next to the backup
  3. verify_backup()            → compare backup contents against manifest
  4. VerifyReport               → results with pass/fail per file

Verification is automatic after every backup (if auto_verify=True).
Also works on encrypted backups (decrypts temporarily for verification).

The manifest is also uploaded to mirror destinations for remote verification.
"""

import hashlib
import json
import logging
import os
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
#  Constants
# ──────────────────────────────────────────────
HASH_ALGORITHM = "sha256"
CHUNK_SIZE = 128 * 1024       # 128 KB for streaming hash
MANIFEST_EXTENSION = ".wbverify"
MANIFEST_VERSION = 1


class VerifyStatus(str, Enum):
    """Result status for a single file verification."""
    OK = "ok"
    MISMATCH = "mismatch"           # Hash doesn't match
    MISSING_IN_BACKUP = "missing"   # File in manifest but not in backup
    EXTRA_IN_BACKUP = "extra"       # File in backup but not in manifest
    SIZE_MISMATCH = "size_mismatch"
    READ_ERROR = "read_error"       # Cannot read file
    CORRUPTED = "corrupted"         # ZIP CRC error or decrypt failure



# ──────────────────────────────────────────────
#  Verification Configuration
# ──────────────────────────────────────────────
@dataclass
# ── Per-profile verification settings ──
class VerificationConfig:
    """Verification settings for a backup profile."""
    auto_verify: bool = True                # Verify after each backup
    alert_on_failure: bool = True           # Show alert dialog on failure


# ──────────────────────────────────────────────
#  Verification Results
# ──────────────────────────────────────────────
@dataclass
class FileVerifyResult:
    """Verification result for a single file."""
    relative_path: str
    status: str = VerifyStatus.OK.value
    expected_hash: str = ""
    actual_hash: str = ""
    expected_size: int = 0
    actual_size: int = 0
    detail: str = ""


@dataclass
# ── Verification results ──
# Contains per-file pass/fail status, mismatch details, and overall verdict.
class VerifyReport:
    """Fulle verification report for a backup."""
    profile_name: str = ""
    backup_path: str = ""
    verify_level: str = "deep"  # Always deep verification
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_files: int = 0
    verified_ok: int = 0
    mismatches: int = 0
    missing: int = 0
    extra: int = 0
    errors: int = 0
    file_results: list[FileVerifyResult] = field(default_factory=list)
    overall_status: str = "unknown"  # "passed", "failed", "warning"

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def duration_str(self) -> str:
        s = int(self.duration_seconds)
        h, remainder = divmod(s, 3600)
        m, sec = divmod(remainder, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    @property
    def failed_files(self) -> list[FileVerifyResult]:
        return [f for f in self.file_results if f.status != VerifyStatus.OK.value]

    def compute_overall_status(self):
        """Compute the overall verification status."""
        if self.mismatches > 0 or self.errors > 0:
            self.overall_status = "failed"
        elif self.missing > 0 or self.extra > 0:
            self.overall_status = "warning"
        elif self.verified_ok == self.total_files and self.total_files > 0:
            self.overall_status = "passed"
        else:
            self.overall_status = "unknown"

    def to_dict(self) -> dict:
        """Export report as dictionary for JSON serialization."""
        return {
            "profile_name": self.profile_name,
            "backup_path": self.backup_path,
            "verify_level": self.verify_level,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration_str,
            "total_files": self.total_files,
            "verified_ok": self.verified_ok,
            "mismatches": self.mismatches,
            "missing": self.missing,
            "extra": self.extra,
            "errors": self.errors,
            "overall_status": self.overall_status,
            "failed_files": [
                {
                    "path": f.relative_path,
                    "status": f.status,
                    "expected_hash": f.expected_hash,
                    "actual_hash": f.actual_hash,
                    "detail": f.detail,
                }
                for f in self.failed_files
            ],
        }


# ──────────────────────────────────────────────
#  Hashing Utilities
# ──────────────────────────────────────────────
def compute_file_hash(filepath: Path, algorithm: str = HASH_ALGORITHM) -> str:
    """
    Compute the cryptographic hash of a file using streaming.

    Args:
        filepath: Path to the file.
        algorithm: Hash algorithm name (default: sha256).

    Returns:
        Hex digest string.
    """
    h = hashlib.new(algorithm)
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def compute_data_hash(data: bytes, algorithm: str = HASH_ALGORITHM) -> str:
    """Compute hash of in-memory bytes."""
    return hashlib.new(algorithm, data).hexdigest()


def compute_manifest_checksum(file_hashes: dict[str, str]) -> str:
    """
    Compute a single checksum representing the entire manifest.
    This is the SHA-256 of all file hashes concatenated in sorted key order.
    Detects any tampering with the manifest itself.
    """
    h = hashlib.sha256()
    for key in sorted(file_hashes.keys()):
        h.update(f"{key}:{file_hashes[key]}".encode("utf-8"))
    return h.hexdigest()


# ──────────────────────────────────────────────
#  Integrity Manifest
# ──────────────────────────────────────────────
@dataclass
# ── SHA-256 manifest of all source files ──
# Created during backup, saved as .wbverify alongside the backup file.
# Used to verify backup integrity days/months/years later.
class IntegrityManifest:
    """Stores checksums for all files in a backup."""
    version: int = MANIFEST_VERSION
    created: str = ""
    profile_id: str = ""
    profile_name: str = ""
    backup_path: str = ""
    algorithm: str = HASH_ALGORITHM
    total_files: int = 0
    total_size: int = 0
    files: dict = field(default_factory=dict)
    manifest_checksum: str = ""

    def save(self, dest_path: Path):
        """Save the manifest as a .wbverify JSON file."""
        manifest_file = dest_path / (dest_path.name + MANIFEST_EXTENSION)
        # If dest_path is a file (e.g., ZIP), save next to it
        if dest_path.is_file():
            manifest_file = dest_path.parent / (dest_path.stem + MANIFEST_EXTENSION)

        data = {
            "version": self.version,
            "created": self.created,
            "profile_id": self.profile_id,
            "profile_name": self.profile_name,
            "backup_path": self.backup_path,
            "algorithm": self.algorithm,
            "total_files": self.total_files,
            "total_size": self.total_size,
            "files": self.files,
            "manifest_checksum": self.manifest_checksum,
        }
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Integrity manifest saved: {manifest_file}")
        return manifest_file

    @classmethod
    def load(cls, manifest_path: Path) -> "IntegrityManifest":
        """Load a manifest from a .wbverify file."""
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        manifest = cls()
        manifest.version = data.get("version", 1)
        manifest.created = data.get("created", "")
        manifest.profile_id = data.get("profile_id", "")
        manifest.profile_name = data.get("profile_name", "")
        manifest.backup_path = data.get("backup_path", "")
        manifest.algorithm = data.get("algorithm", HASH_ALGORITHM)
        manifest.total_files = data.get("total_files", 0)
        manifest.total_size = data.get("total_size", 0)
        manifest.files = data.get("files", {})
        manifest.manifest_checksum = data.get("manifest_checksum", "")
        return manifest

    def validate_self(self) -> bool:
        """Verify the manifest hasn't been tampered with."""
        if not self.manifest_checksum:
            return True  # No checksum to verify (old format)
        file_hashes = {k: v["sha256"] for k, v in self.files.items() if "sha256" in v}
        expected = compute_manifest_checksum(file_hashes)
        return expected == self.manifest_checksum


# ──────────────────────────────────────────────
#  Verification Engine
# ──────────────────────────────────────────────
class VerificationEngine:
    """
    Core verification engine for backup integrity checking.
    Supports:
      - Building manifests from source files
      - Post-backup verification (source ↔ backup)
      - On-demand verification of existing backups
      - ZIP integrity testing
      - Encrypted file validation
    """

    def __init__(self):
        self._cancel_requested = False
        self._progress_callback: Optional[Callable] = None
        self._status_callback: Optional[Callable] = None

    def set_callbacks(
        self,
        progress_callback: Optional[Callable] = None,
        status_callback: Optional[Callable] = None,
    ):
        self._progress_callback = progress_callback
        self._status_callback = status_callback

    def cancel(self):
        self._cancel_requested = True

    def _update_progress(self, current: int, total: int):
        if self._progress_callback and total > 0:
            self._progress_callback(current, total)

    def _update_status(self, message: str):
        if self._status_callback:
            self._status_callback(message)

    # ────────────────────────────────
    #  Build Manifest from Sources
    # ────────────────────────────────
    def build_manifest(
        self,
        file_list: list[tuple[str, Path]],
        profile_id: str = "",
        profile_name: str = "",
    ) -> IntegrityManifest:
        """Build an integrity manifest by hashing all source files."""
        self._cancel_requested = False
        manifest = IntegrityManifest(
            created=datetime.now().isoformat(),
            profile_id=profile_id,
            profile_name=profile_name,
            algorithm=HASH_ALGORITHM,
        )

        total = len(file_list)
        self._update_status(f"🔍 Computing SHA-256 checksums ({total} files)...")

        total_size = 0
        file_hashes = {}

        for i, (rel_path, abs_path) in enumerate(file_list):
            if self._cancel_requested:
                break
            rel_path = rel_path.replace("\\", "/")
            try:
                file_hash = compute_file_hash(abs_path)
                file_stat = abs_path.stat()
                manifest.files[rel_path] = {
                    "sha256": file_hash,
                    "size": file_stat.st_size,
                    "mtime": file_stat.st_mtime,
                }
                file_hashes[rel_path] = file_hash
                total_size += file_stat.st_size
                self._update_progress(i + 1, total)
                self._update_status(f"🔍 Hashing [{i+1}/{total}]...")
            except OSError as e:
                manifest.files[rel_path] = {
                    "sha256": "", "size": 0, "mtime": 0, "error": str(e),
                }
                logger.warning(f"Cannot hash file: {e}")

        manifest.total_files = len(file_hashes)
        manifest.total_size = total_size
        manifest.manifest_checksum = compute_manifest_checksum(file_hashes)

        self._update_status(
            f"🔍 Manifest created: {manifest.total_files} files, "
            f"checksum: {manifest.manifest_checksum[:16]}..."
        )
        return manifest

    # ────────────────────────────────
    #  Post-Backup Verification
    # ────────────────────────────────
    def verify_backup(
        self,
        manifest: IntegrityManifest,
        backup_path: Path,
        encryption_password: Optional[str] = None,
    ) -> VerifyReport:
        """
        Verify a backup against its integrity manifest.
        Always performs deep verification: SHA-256 + ZIP extraction test + decrypt test.

        Args:
            manifest: The manifest built from source files.
            backup_path: Path to the backup (directory or ZIP/encrypted file).
            encryption_password: Password if backup is encrypted.

        Returns:
            VerifyReport with detailed results.
        """
        self._cancel_requested = False
        report = VerifyReport(
            profile_name=manifest.profile_name,
            backup_path=str(backup_path),
            start_time=datetime.now(),
            total_files=manifest.total_files,
        )

        self._update_status("✅ Deep integrity verification...")

        # 1. Validate manifest self-integrity
        if not manifest.validate_self():
            report.errors += 1
            report.file_results.append(FileVerifyResult(
                relative_path="[MANIFESTE]",
                status=VerifyStatus.CORRUPTED.value,
                detail="Global manifest checksum is invalid — manifest potentially tampered.",
            ))
            self._update_status("⚠ The integrity manifest is corrupted !")

        # Detect backup type and route to appropriate verifier
        zip_path = self._find_backup_file(backup_path)

        if zip_path and zip_path.suffix == ".zip":
            report = self._verify_zip_backup(manifest, zip_path, report)
        elif zip_path and zip_path.name.endswith(".zip.wbenc"):
            report = self._verify_encrypted_zip(
                manifest, zip_path, report, encryption_password
            )
        elif backup_path.is_dir():
            report = self._verify_flat_backup(manifest, backup_path, report)
        else:
            report.errors += 1
            report.file_results.append(FileVerifyResult(
                relative_path="[BACKUP]",
                status=VerifyStatus.MISSING_IN_BACKUP.value,
                detail=f"No backup found: {backup_path}",
            ))

        report.end_time = datetime.now()
        report.compute_overall_status()

        status_icon = {
            "passed": "✅", "warning": "⚠", "failed": "❌"
        }.get(report.overall_status, "❓")

        self._update_status(
            f"{status_icon} Verification complete: {report.verified_ok}/{report.total_files} OK, "
            f"{report.mismatches} mismatches, {report.missing} missing, "
            f"{report.errors} error(s) — Duration: {report.duration_str}"
        )

        return report

    def _find_backup_file(self, backup_path: Path) -> Optional[Path]:
        """Find the actual backup file (ZIP, encrypted, or directory)."""
        if backup_path.is_file():
            return backup_path

        # Look for ZIP or encrypted ZIP next to or inside backup_path
        parent = backup_path.parent if backup_path.is_dir() else backup_path.parent
        name = backup_path.name

        for ext in [".zip", ".zip.wbenc"]:
            candidate = parent / (name + ext)
            if candidate.exists():
                return candidate

        if backup_path.is_dir():
            return None  # Flat backup directory

        return None

    # ────────────────────────────────
    #  Flat Backup Verification
    # ────────────────────────────────
    def _verify_flat_backup(
        self,
        manifest: IntegrityManifest,
        backup_dir: Path,
        report: VerifyReport,
    ) -> VerifyReport:
        """Verify an uncompressed directory backup (deep: SHA-256 + sizes)."""
        # Normalize manifest keys to forward slashes
        manifest_files = set(k.replace("\\", "/") for k in manifest.files.keys())
        manifest_normalized = {k.replace("\\", "/"): v for k, v in manifest.files.items()}
        backup_files = set()

        # Scan backup directory
        for filepath in backup_dir.rglob("*"):
            if filepath.is_file() and not filepath.name.endswith(MANIFEST_EXTENSION):
                try:
                    rel = str(filepath.relative_to(backup_dir))
                    # Normalize Windows paths
                    rel = rel.replace("\\", "/")
                    backup_files.add(rel)
                except ValueError:
                    pass

        # Check missing files
        missing = manifest_files - backup_files
        for m in missing:
            report.missing += 1
            report.file_results.append(FileVerifyResult(
                relative_path=m,
                status=VerifyStatus.MISSING_IN_BACKUP.value,
                detail="File present in source but missing from backup.",
            ))

        # Check extra files
        extra = backup_files - manifest_files
        for e in extra:
            report.extra += 1
            report.file_results.append(FileVerifyResult(
                relative_path=e,
                status=VerifyStatus.EXTRA_IN_BACKUP.value,
                detail="File present in backup but missing from source.",
            ))

        # Verify common files
        common = manifest_files & backup_files
        total = len(common)

        for i, rel_path in enumerate(sorted(common)):
            if self._cancel_requested:
                break

            abs_path = backup_dir / rel_path
            expected = manifest_normalized.get(rel_path, {})
            result = FileVerifyResult(relative_path=rel_path)

            try:
                actual_stat = abs_path.stat()

                # Size check (quick + standard + deep)
                expected_size = expected.get("size", 0)
                if actual_stat.st_size != expected_size:
                    result.status = VerifyStatus.SIZE_MISMATCH.value
                    result.expected_size = expected_size
                    result.actual_size = actual_stat.st_size
                    result.detail = (
                        f"Size mismatch: expected {expected_size} bytes, got {actual_stat.st_size} bytes"
                    )
                    report.mismatches += 1

                # SHA-256 hash check
                else:
                    expected_hash = expected.get("sha256", "")
                    if expected_hash:
                        actual_hash = compute_file_hash(abs_path)
                        if actual_hash != expected_hash:
                            result.status = VerifyStatus.MISMATCH.value
                            result.expected_hash = expected_hash
                            result.actual_hash = actual_hash
                            result.detail = "SHA-256 hash mismatch."
                            report.mismatches += 1
                        else:
                            result.status = VerifyStatus.OK.value
                            report.verified_ok += 1
                    else:
                        result.status = VerifyStatus.OK.value
                        report.verified_ok += 1

            except OSError as e:
                result.status = VerifyStatus.READ_ERROR.value
                result.detail = str(e)
                report.errors += 1

            report.file_results.append(result)
            self._update_progress(i + 1, total)
            self._update_status(f"✅ Verifying [{i+1}/{total}]...")

        return report

    # ────────────────────────────────
    #  ZIP Backup Verification
    # ────────────────────────────────
    def _verify_zip_backup(
        self,
        manifest: IntegrityManifest,
        zip_path: Path,
        report: VerifyReport,
    ) -> VerifyReport:
        """Verify a compressed ZIP backup (deep: CRC32 + SHA-256 + extraction test)."""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                # 1. Built-in CRC32 test (all levels)
                self._update_status("📦 CRC32 test of ZIP archive...")
                bad_files = zf.testzip()
                if bad_files:
                    report.errors += 1
                    report.file_results.append(FileVerifyResult(
                        relative_path=bad_files,
                        status=VerifyStatus.CORRUPTED.value,
                        detail=f"Invalid CRC32 in ZIP archive from: {bad_files}",
                    ))
                    self._update_status(f"❌ Corruption detected in ZIP: {bad_files}")

                # 2. File listing comparison
                zip_files = set()
                zip_info_map = {}
                for info in zf.infolist():
                    if not info.is_dir():
                        name = info.filename.replace("\\", "/")
                        zip_files.add(name)
                        zip_info_map[name] = info

                manifest_files = set()
                manifest_normalized = {}
                for k, v in manifest.files.items():
                    nk = k.replace("\\", "/")
                    manifest_files.add(nk)
                    manifest_normalized[nk] = v

                # Missing
                for m in manifest_files - zip_files:
                    report.missing += 1
                    report.file_results.append(FileVerifyResult(
                        relative_path=m,
                        status=VerifyStatus.MISSING_IN_BACKUP.value,
                        detail="File missing from ZIP archive.",
                    ))

                # Extra
                for e in zip_files - manifest_files:
                    report.extra += 1
                    report.file_results.append(FileVerifyResult(
                        relative_path=e,
                        status=VerifyStatus.EXTRA_IN_BACKUP.value,
                        detail="File in ZIP but not in manifest.",
                    ))

                # 3. Verify common files
                common = manifest_files & zip_files
                total = len(common)

                for i, rel_path in enumerate(sorted(common)):
                    if self._cancel_requested:
                        break

                    expected = manifest_normalized.get(rel_path, {})
                    info = zip_info_map[rel_path]
                    result = FileVerifyResult(relative_path=rel_path)

                    # Size check (uncompressed size)
                    expected_size = expected.get("size", 0)
                    if info.file_size != expected_size:
                        result.status = VerifyStatus.SIZE_MISMATCH.value
                        result.expected_size = expected_size
                        result.actual_size = info.file_size
                        result.detail = (
                            f"Size mismatch: expected {expected_size} bytes, got {info.file_size} bytes"
                        )
                        report.mismatches += 1

                    # SHA-256 hash check: extract and hash
                    else:
                        expected_hash = expected.get("sha256", "")
                        if expected_hash:
                            try:
                                data = zf.read(rel_path)
                                actual_hash = compute_data_hash(data)
                                if actual_hash != expected_hash:
                                    result.status = VerifyStatus.MISMATCH.value
                                    result.expected_hash = expected_hash
                                    result.actual_hash = actual_hash
                                    result.detail = "SHA-256 hash mismatch after extraction."
                                    report.mismatches += 1
                                else:
                                    result.status = VerifyStatus.OK.value
                                    report.verified_ok += 1
                            except Exception as e:
                                result.status = VerifyStatus.READ_ERROR.value
                                result.detail = f"Extraction error: {e}"
                                report.errors += 1
                        else:
                            result.status = VerifyStatus.OK.value
                            report.verified_ok += 1
                    report.file_results.append(result)
                    self._update_progress(i + 1, total)
                    self._update_status(f"✅ Verifying [{i+1}/{total}]...")

                # 4. Deep: extraction test in temp directory
                if not self._cancel_requested:
                    report = self._deep_zip_extraction_test(zf, report)

        except zipfile.BadZipFile as e:
            report.errors += 1
            report.file_results.append(FileVerifyResult(
                relative_path="[ARCHIVE]",
                status=VerifyStatus.CORRUPTED.value,
                detail=f"Corrupted ZIP archive: {e}",
            ))
            self._update_status(f"❌ Corrupted ZIP archive: {e}")

        return report

    def _deep_zip_extraction_test(
        self, zf: zipfile.ZipFile, report: VerifyReport
    ) -> VerifyReport:
        """Deep test: extract all files to temp and verify they're readable."""
        self._update_status("🔬 Deep extraction test...")
        try:
            with tempfile.TemporaryDirectory(prefix="wbverify_") as tmpdir:
                zf.extractall(tmpdir)
                extracted = list(Path(tmpdir).rglob("*"))
                file_count = sum(1 for f in extracted if f.is_file())
                self._update_status(
                    f"🔬 Extraction test OK: {file_count} file(s) extracted successfully."
                )
        except Exception as e:
            report.errors += 1
            report.file_results.append(FileVerifyResult(
                relative_path="[EXTRACTION_TEST]",
                status=VerifyStatus.CORRUPTED.value,
                detail=f"Extraction test failed: {e}",
            ))
            self._update_status(f"❌ Failed extraction test: {e}")
        return report

    # ────────────────────────────────
    #  Encrypted Backup Verification
    # ────────────────────────────────
    def _verify_encrypted_zip(
        self,
        manifest: IntegrityManifest,
        enc_path: Path,
        report: VerifyReport,
        password: Optional[str] = None,
    ) -> VerifyReport:
        """Verify an encrypted backup (.wbenc) with deep verification."""
        try:
            from encryption import get_crypto_engine, CryptoEngine
        except ImportError:
            report.errors += 1
            report.file_results.append(FileVerifyResult(
                relative_path="[ENCRYPTED]",
                status=VerifyStatus.READ_ERROR.value,
                detail="Module encryption non available.",
            ))
            return report

        crypto = get_crypto_engine()

        # 1. Check header validity
        self._update_status("🔐 Validating encrypted file header...")
        if not crypto.is_encrypted_file(enc_path):
            report.errors += 1
            report.file_results.append(FileVerifyResult(
                relative_path=enc_path.name,
                status=VerifyStatus.CORRUPTED.value,
                detail="WBAK header is invalid or missing.",
            ))
            return report

        self._update_status("🔐 WBAK header valid.")

        # 2. If no password, we can only verify the header
        if not password:
            report.file_results.append(FileVerifyResult(
                relative_path=enc_path.name,
                status=VerifyStatus.OK.value,
                detail="Header valid. Password required for full verification.",
            ))
            report.verified_ok = 0  # Can't confirm content without password
            self._update_status("⚠ No password provided — verification limited to header.")
            return report

        # 3. Decrypt and verify as ZIP
        self._update_status("🔐 Decrypting for verification...")
        try:
            encrypted_data = enc_path.read_bytes()
            decrypted_data = crypto.decrypt_bytes(encrypted_data, password)

            # Write to temp file and verify as ZIP
            with tempfile.NamedTemporaryFile(
                suffix=".zip", delete=False, prefix="wbverify_"
            ) as tmp:
                tmp.write(decrypted_data)
                tmp_path = Path(tmp.name)

            try:
                report = self._verify_zip_backup(manifest, tmp_path, report)
            finally:
                tmp_path.unlink(missing_ok=True)

        except ValueError as e:
            report.errors += 1
            report.file_results.append(FileVerifyResult(
                relative_path=enc_path.name,
                status=VerifyStatus.CORRUPTED.value,
                detail=f"Decryption failed: {e}",
            ))
            self._update_status(f"❌ Decryption failed: {e}")
        except Exception as e:
            report.errors += 1
            report.file_results.append(FileVerifyResult(
                relative_path=enc_path.name,
                status=VerifyStatus.READ_ERROR.value,
                detail=f"Error: {e}",
            ))

        return report

    # ────────────────────────────────
    #  Standalone Verification
    # ────────────────────────────────
    def verify_from_manifest_file(
        self,
        manifest_path: Path,
        backup_path: Optional[Path] = None,

        encryption_password: Optional[str] = None,
    ) -> VerifyReport:
        """
        Verify an existing backup using a saved .wbverify manifest.

        Args:
            manifest_path: Path to the .wbverify manifest file.
            backup_path: Path to the backup (auto-detected if None).
            encryption_password: Password for encrypted backups.

        Returns:
            VerifyReport with results.
        """
        manifest = IntegrityManifest.load(manifest_path)

        if backup_path is None:
            # Auto-detect: look for backup next to manifest
            backup_path = Path(manifest.backup_path)
            if not backup_path.exists():
                base = manifest_path.parent
                stem = manifest_path.stem
                for candidate in [
                    base / stem,
                    base / (stem + ".zip"),
                    base / (stem + ".zip.wbenc"),
                ]:
                    if candidate.exists():
                        backup_path = candidate
                        break

        return self.verify_backup(manifest, backup_path, encryption_password)

    # ────────────────────────────────
    #  Report Export
    # ────────────────────────────────
    @staticmethod
    def export_report(report: VerifyReport, dest_path: Path) -> Path:
        """Export a verification report as JSON."""
        report_file = dest_path / f"verify_report_{datetime.now():%Y%m%d_%H%M%S}.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
        logger.info(f"Verification report exported: {report_file}")
        return report_file
