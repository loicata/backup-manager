"""Tests for src.core.backup_result — error accumulation and stats."""

from src.core.backup_result import BackupResult, PhaseError


class TestPhaseError:
    """Tests for the PhaseError dataclass."""

    def test_create_with_all_fields(self) -> None:
        """PhaseError stores all provided fields."""
        exc = OSError("disk full")
        error = PhaseError(
            phase="writer", file_path="/tmp/test.txt",
            message="write failed", exception=exc,
        )
        assert error.phase == "writer"
        assert error.file_path == "/tmp/test.txt"
        assert error.message == "write failed"
        assert error.exception is exc

    def test_exception_defaults_to_none(self) -> None:
        """exception field defaults to None."""
        error = PhaseError(phase="collector", file_path="", message="oops")
        assert error.exception is None


class TestBackupResultDefaults:
    """Tests for BackupResult default state."""

    def test_default_values(self) -> None:
        """Fresh BackupResult has zero counts and empty lists."""
        result = BackupResult()
        assert result.files_found == 0
        assert result.files_processed == 0
        assert result.files_skipped == 0
        assert result.bytes_source == 0
        assert result.duration_seconds == 0.0
        assert result.backup_path == ""
        assert result.mirror_results == []
        assert result.rotated_count == 0
        assert result.phase_errors == []

    def test_success_when_no_errors(self) -> None:
        """success is True when phase_errors is empty."""
        result = BackupResult()
        assert result.success is True

    def test_errors_count_is_zero(self) -> None:
        """errors property returns 0 when no errors."""
        result = BackupResult()
        assert result.errors == 0


class TestBackupResultAddError:
    """Tests for the add_error method."""

    def test_add_error_appends_phase_error(self) -> None:
        """add_error creates and appends a PhaseError."""
        result = BackupResult()
        result.add_error("writer", "file.txt", "copy failed")

        assert len(result.phase_errors) == 1
        assert result.phase_errors[0].phase == "writer"
        assert result.phase_errors[0].file_path == "file.txt"
        assert result.phase_errors[0].message == "copy failed"

    def test_add_error_with_exception(self) -> None:
        """add_error stores the optional exception."""
        result = BackupResult()
        exc = PermissionError("access denied")
        result.add_error("writer", "secret.dat", "perm error", exception=exc)

        assert result.phase_errors[0].exception is exc

    def test_add_multiple_errors(self) -> None:
        """Multiple errors accumulate correctly."""
        result = BackupResult()
        result.add_error("collector", "a.txt", "not found")
        result.add_error("writer", "b.txt", "write failed")
        result.add_error("encryptor", "c.txt", "key error")

        assert len(result.phase_errors) == 3
        assert result.errors == 3

    def test_success_false_after_error(self) -> None:
        """success is False after adding an error."""
        result = BackupResult()
        result.add_error("writer", "x.txt", "fail")
        assert result.success is False

    def test_errors_property_matches_phase_errors_length(self) -> None:
        """errors property is always len(phase_errors)."""
        result = BackupResult()
        for i in range(5):
            result.add_error("test", f"file{i}.txt", f"error {i}")
        assert result.errors == 5


class TestBackupResultErrorSummary:
    """Tests for the error_summary method."""

    def test_summary_no_errors(self) -> None:
        """Summary for zero errors reports success."""
        result = BackupResult()
        result.files_processed = 10
        summary = result.error_summary()
        assert "0" in summary or "success" in summary.lower() or "no error" in summary.lower()

    def test_summary_with_errors(self) -> None:
        """Summary includes error count."""
        result = BackupResult()
        result.files_processed = 10
        result.add_error("writer", "a.txt", "fail A")
        result.add_error("writer", "b.txt", "fail B")
        summary = result.error_summary()
        assert "2" in summary

    def test_summary_is_nonempty_string(self) -> None:
        """Summary always returns a non-empty string."""
        result = BackupResult()
        assert isinstance(result.error_summary(), str)
        assert len(result.error_summary()) > 0


class TestBackupResultBackwardCompat:
    """Verify backward compatibility with former BackupStats usage."""

    def test_all_former_backstats_fields_exist(self) -> None:
        """All fields from the old BackupStats are present."""
        result = BackupResult()
        # These were all fields of BackupStats
        _ = result.files_found
        _ = result.files_processed
        _ = result.files_skipped
        _ = result.errors
        _ = result.bytes_source
        _ = result.duration_seconds
        _ = result.backup_path
        _ = result.mirror_results
        _ = result.rotated_count

    def test_errors_field_is_int(self) -> None:
        """errors property returns an int (not a list)."""
        result = BackupResult()
        assert isinstance(result.errors, int)
