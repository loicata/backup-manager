"""Backup result with error accumulation.

Replaces BackupStats with richer error tracking. Each phase can
record individual file errors via add_error(), and the final result
provides a summary for the user and email notifications.

The 'errors' property returns an int for backward compatibility
with code that used BackupStats.errors as an integer count.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class PhaseError:
    """A single error from a pipeline phase.

    Args:
        phase: Phase that encountered the error (e.g. "writer").
        file_path: File that caused the error, or "" for phase-level errors.
        message: Human-readable description.
        exception: Original exception, if available.
    """

    phase: str
    file_path: str
    message: str
    exception: Optional[Exception] = None


@dataclass
class BackupResult:
    """Aggregated result of a backup run.

    All fields from the former BackupStats are preserved for
    backward compatibility. The new phase_errors list provides
    detailed error tracking with per-file granularity.

    Args:
        files_found: Total files found by the collector.
        files_processed: Files actually written to the destination.
        files_skipped: Files skipped (unchanged in incremental mode).
        bytes_source: Total bytes of source files.
        duration_seconds: Wall-clock time for the backup run.
        backup_path: Path or remote name of the created backup.
        mirror_results: List of (mirror_name, success, message) tuples.
        rotated_count: Number of old backups deleted by rotation.
        phase_errors: Accumulated errors from all pipeline phases.
    """

    files_found: int = 0
    files_processed: int = 0
    files_skipped: int = 0
    bytes_source: int = 0
    duration_seconds: float = 0.0
    backup_path: str = ""
    mirror_results: list[tuple[str, bool, str]] = field(default_factory=list)
    rotated_count: int = 0
    phase_errors: list[PhaseError] = field(default_factory=list)

    @property
    def errors(self) -> int:
        """Total error count.

        Returns an int for backward compatibility with code that
        used BackupStats.errors as an integer counter.
        """
        return len(self.phase_errors)

    @property
    def success(self) -> bool:
        """True if the backup completed without any errors."""
        return len(self.phase_errors) == 0

    def add_error(
        self,
        phase: str,
        file_path: str,
        message: str,
        exception: Optional[Exception] = None,
    ) -> None:
        """Record an error from a pipeline phase.

        Args:
            phase: Phase identifier (e.g. "collector", "writer").
            file_path: File that caused the error, or "" for phase-level.
            message: Human-readable error description.
            exception: Original exception, if available.
        """
        error = PhaseError(
            phase=phase,
            file_path=file_path,
            message=message,
            exception=exception,
        )
        self.phase_errors.append(error)
        logger.error("[%s] %s: %s", phase, file_path or "(phase)", message)

    def error_summary(self) -> str:
        """Human-readable error summary for the user.

        Returns:
            A string describing the backup outcome.
        """
        if self.success:
            return f"Backup successful: {self.files_processed} files, no errors"

        error_count = len(self.phase_errors)
        ok_count = self.files_processed - error_count
        lines = [
            f"Backup completed with {error_count} error(s) "
            f"({ok_count}/{self.files_processed} files OK):"
        ]

        # Show up to 10 errors
        display_limit = 10
        for error in self.phase_errors[:display_limit]:
            if error.file_path:
                lines.append(f"  [{error.phase}] {error.file_path}: {error.message}")
            else:
                lines.append(f"  [{error.phase}] {error.message}")

        remaining = error_count - display_limit
        if remaining > 0:
            lines.append(f"  ...and {remaining} more error(s)")

        return "\n".join(lines)
