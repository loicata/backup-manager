"""Centralized exception definitions for Backup Manager."""


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
