"""Backup verification using SHA-256 integrity manifests (.wbverify).

Creates manifests during backup and verifies backup contents
against them during restore or post-backup checks.
"""

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path

logger = logging.getLogger(__name__)

HASH_ALGORITHM = "sha256"
CHUNK_SIZE = 128 * 1024  # 128 KB
MANIFEST_VERSION = 1
MAX_HASH_WORKERS = 4
MANIFEST_EXTENSION = ".wbverify"


class FileStatus(StrEnum):
    OK = "ok"
    MISMATCH = "mismatch"
    MISSING_IN_BACKUP = "missing_in_backup"
    EXTRA_IN_BACKUP = "extra_in_backup"
    SIZE_MISMATCH = "size_mismatch"
    READ_ERROR = "read_error"
    CORRUPTED = "corrupted"


@dataclass
class FileVerifyResult:
    relative_path: str
    status: FileStatus
    expected_hash: str = ""
    actual_hash: str = ""
    detail: str = ""


@dataclass
class VerifyReport:
    profile_name: str = ""
    backup_path: str = ""
    verify_level: str = "deep"
    start_time: datetime | None = None
    end_time: datetime | None = None
    total_files: int = 0
    verified_ok: int = 0
    mismatches: int = 0
    missing: int = 0
    extra: int = 0
    errors: int = 0
    file_results: list[FileVerifyResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.mismatches == 0 and self.missing == 0 and self.errors == 0


@dataclass
class IntegrityManifest:
    """Stores file hashes for a backup."""

    version: int = MANIFEST_VERSION
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    algorithm: str = HASH_ALGORITHM
    files: dict[str, dict] = field(default_factory=dict)
    total_checksum: str = ""

    def save(self, backup_path: Path) -> Path:
        """Save manifest alongside the backup."""
        manifest_path = backup_path.parent / (backup_path.stem + MANIFEST_EXTENSION)
        data = {
            "version": self.version,
            "created_at": self.created_at,
            "algorithm": self.algorithm,
            "files": self.files,
            "total_checksum": self.total_checksum,
        }
        manifest_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Manifest saved: %s (%d files)", manifest_path, len(self.files))
        return manifest_path

    @classmethod
    def load(cls, manifest_path: Path) -> "IntegrityManifest":
        """Load a manifest from file."""
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        return cls(
            version=data.get("version", MANIFEST_VERSION),
            created_at=data.get("created_at", ""),
            algorithm=data.get("algorithm", HASH_ALGORITHM),
            files=data.get("files", {}),
            total_checksum=data.get("total_checksum", ""),
        )


def compute_file_hash(filepath: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def build_manifest(
    source_paths: list[str],
    exclude_patterns: list[str] | None = None,
) -> IntegrityManifest:
    """Build integrity manifest from source files.

    Args:
        source_paths: List of source file paths (absolute).
        exclude_patterns: Glob patterns to exclude.

    Returns:
        IntegrityManifest with hashes of all files.
    """
    manifest = IntegrityManifest()
    all_hashes = []

    def _hash_file(filepath: Path) -> tuple[str, str, int]:
        h = compute_file_hash(filepath)
        return str(filepath), h, filepath.stat().st_size

    with ThreadPoolExecutor(max_workers=MAX_HASH_WORKERS) as executor:
        futures = []
        for path_str in source_paths:
            path = Path(path_str)
            if path.is_file():
                futures.append(executor.submit(_hash_file, path))
            elif path.is_dir():
                for fp in path.rglob("*"):
                    if fp.is_file() and not _is_excluded(fp, exclude_patterns):
                        futures.append(executor.submit(_hash_file, fp))

        for future in futures:
            try:
                filepath, file_hash, size = future.result()
                manifest.files[filepath] = {
                    "hash": file_hash,
                    "size": size,
                }
                all_hashes.append(file_hash)
            except Exception as e:
                logger.warning("Failed to hash file: %s", e)

    # Total checksum: hash of all sorted file hashes
    if all_hashes:
        combined = "\n".join(sorted(all_hashes))
        manifest.total_checksum = hashlib.sha256(combined.encode("utf-8")).hexdigest()

    return manifest


def verify_backup(
    manifest: IntegrityManifest,
    backup_path: Path,
) -> VerifyReport:
    """Verify backup contents against a manifest.

    Args:
        manifest: Expected file hashes.
        backup_path: Path to backup directory or ZIP.

    Returns:
        VerifyReport with detailed results.
    """
    report = VerifyReport(
        backup_path=str(backup_path),
        start_time=datetime.now(),
        total_files=len(manifest.files),
    )

    backup_files = set()
    if backup_path.is_dir():
        for fp in backup_path.rglob("*"):
            if fp.is_file():
                backup_files.add(str(fp))

    for filepath, info in manifest.files.items():
        expected_hash = info.get("hash", "")
        backup_file = Path(filepath)

        # Check if file exists in backup
        if str(backup_file) not in backup_files and not backup_file.exists():
            report.missing += 1
            report.file_results.append(
                FileVerifyResult(
                    relative_path=filepath,
                    status=FileStatus.MISSING_IN_BACKUP,
                    expected_hash=expected_hash,
                )
            )
            continue

        try:
            actual_hash = compute_file_hash(backup_file)
            if actual_hash == expected_hash:
                report.verified_ok += 1
                report.file_results.append(
                    FileVerifyResult(
                        relative_path=filepath,
                        status=FileStatus.OK,
                        expected_hash=expected_hash,
                        actual_hash=actual_hash,
                    )
                )
            else:
                report.mismatches += 1
                report.file_results.append(
                    FileVerifyResult(
                        relative_path=filepath,
                        status=FileStatus.MISMATCH,
                        expected_hash=expected_hash,
                        actual_hash=actual_hash,
                    )
                )
        except Exception as e:
            report.errors += 1
            report.file_results.append(
                FileVerifyResult(
                    relative_path=filepath,
                    status=FileStatus.READ_ERROR,
                    expected_hash=expected_hash,
                    detail=str(e),
                )
            )

    report.end_time = datetime.now()
    return report


def _is_excluded(filepath: Path, patterns: list[str] | None) -> bool:
    """Check if a file matches any exclusion pattern."""
    if not patterns:
        return False
    import fnmatch

    name = filepath.name
    rel = str(filepath)
    for pattern in patterns:
        if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(rel, pattern):
            return True
    return False
