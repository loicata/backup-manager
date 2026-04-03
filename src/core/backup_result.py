"""Backup result with error accumulation.

Each phase can record individual file errors via add_error(),
and the final result provides a summary for the user and email
notifications.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ErrorSeverity(Enum):
    """Severity level for pipeline phase errors.

    WARNING: Non-critical issue that does not fail the backup.
    ERROR: File-level or phase-level failure.
    FATAL: Unrecoverable error that should stop the pipeline.
    """

    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"


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
    exception: Exception | None = None
    severity: ErrorSeverity = ErrorSeverity.ERROR


@dataclass
class BackupResult:
    """Aggregated result of a backup run.

    Args:
        files_found: Total files found by the collector.
        files_processed: Files actually written to the destination.
        files_skipped: Files skipped (unchanged in differential mode).
        bytes_source: Total bytes of source files.
        duration_seconds: Wall-clock time for the backup run.
        backup_path: Path or remote name of the created backup.
        mirror_results: List of (mirror_name, success, message, description) tuples.
        rotated_count: Number of old backups deleted by rotation.
        backups_available: Number of backups on primary after rotation.
        phase_errors: Accumulated errors from all pipeline phases.
    """

    files_found: int = 0
    files_processed: int = 0
    files_skipped: int = 0
    bytes_source: int = 0
    duration_seconds: float = 0.0
    backup_path: str = ""
    mirror_results: list[tuple[str, bool, str, str]] = field(default_factory=list)
    rotated_count: int = 0
    backups_available: int = 0
    phase_errors: list[PhaseError] = field(default_factory=list)
    log_lines: list[str] = field(default_factory=list)

    @property
    def errors(self) -> int:
        """Count of ERROR and FATAL severity errors."""
        return sum(
            1 for e in self.phase_errors if e.severity in (ErrorSeverity.ERROR, ErrorSeverity.FATAL)
        )

    @property
    def warnings(self) -> int:
        """Count of WARNING severity errors."""
        return sum(1 for e in self.phase_errors if e.severity == ErrorSeverity.WARNING)

    @property
    def success(self) -> bool:
        """True if the backup completed without ERROR or FATAL errors.

        Warnings do not cause the backup to be considered failed.
        """
        return not any(
            e.severity in (ErrorSeverity.ERROR, ErrorSeverity.FATAL) for e in self.phase_errors
        )

    @property
    def has_fatal_errors(self) -> bool:
        """True if any FATAL error was recorded."""
        return any(e.severity == ErrorSeverity.FATAL for e in self.phase_errors)

    def add_error(
        self,
        phase: str,
        file_path: str,
        message: str,
        exception: Exception | None = None,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
    ) -> None:
        """Record an error from a pipeline phase.

        Args:
            phase: Phase identifier (e.g. "collector", "writer").
            file_path: File that caused the error, or "" for phase-level.
            message: Human-readable error description.
            exception: Original exception, if available.
            severity: Error severity level (default ERROR).
        """
        error = PhaseError(
            phase=phase,
            file_path=file_path,
            message=message,
            exception=exception,
            severity=severity,
        )
        self.phase_errors.append(error)
        log_fn = logger.warning if severity == ErrorSeverity.WARNING else logger.error
        log_fn("[%s] %s: %s", phase, file_path or "(phase)", message)

    def add_warning(
        self,
        phase: str,
        file_path: str,
        message: str,
        exception: Exception | None = None,
    ) -> None:
        """Record a warning from a pipeline phase.

        Warnings do not cause the backup to be considered failed.

        Args:
            phase: Phase identifier (e.g. "collector", "writer").
            file_path: File that caused the warning, or "" for phase-level.
            message: Human-readable warning description.
            exception: Original exception, if available.
        """
        self.add_error(phase, file_path, message, exception, severity=ErrorSeverity.WARNING)

    def error_summary(self) -> str:
        """Human-readable error summary for the user.

        Returns:
            A string describing the backup outcome.
        """
        warn_count = self.warnings
        err_count = self.errors

        if self.success and warn_count == 0:
            return f"Backup successful: {self.files_processed} files, no errors"

        if self.success and warn_count > 0:
            return (
                f"Backup successful with {warn_count} warning(s): " f"{self.files_processed} files"
            )

        ok_count = self.files_processed - err_count
        lines = [
            f"Backup completed with {err_count} error(s) "
            f"({ok_count}/{self.files_processed} files OK):"
        ]

        # Show up to 10 non-warning errors first
        display_limit = 10
        shown = 0
        for error in self.phase_errors:
            if error.severity == ErrorSeverity.WARNING:
                continue
            if shown >= display_limit:
                break
            tag = error.severity.value.upper()
            if error.file_path:
                lines.append(f"  [{tag}][{error.phase}] {error.file_path}: {error.message}")
            else:
                lines.append(f"  [{tag}][{error.phase}] {error.message}")
            shown += 1

        remaining = err_count - shown
        if remaining > 0:
            lines.append(f"  ...and {remaining} more error(s)")

        if warn_count > 0:
            lines.append(f"  ({warn_count} warning(s) not shown)")

        return "\n".join(lines)
