"""Tests for src.core.backup_result — error accumulation and stats."""

from src.core.backup_result import BackupResult, ErrorSeverity, PhaseError


class TestPhaseError:
    """Tests for the PhaseError dataclass."""

    def test_create_with_all_fields(self) -> None:
        """PhaseError stores all provided fields."""
        exc = OSError("disk full")
        error = PhaseError(
            phase="writer",
            file_path="/tmp/test.txt",
            message="write failed",
            exception=exc,
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


class TestBackupResultFields:
    """Verify all expected fields exist on BackupResult."""

    def test_all_expected_fields_exist(self) -> None:
        """All public fields are accessible."""
        result = BackupResult()
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


class TestErrorSeverity:
    """Tests for the ErrorSeverity enum."""

    def test_enum_values_exist(self) -> None:
        """All three severity levels are defined."""
        assert ErrorSeverity.WARNING.value == "warning"
        assert ErrorSeverity.ERROR.value == "error"
        assert ErrorSeverity.FATAL.value == "fatal"

    def test_enum_members_count(self) -> None:
        """Exactly three severity levels."""
        assert len(ErrorSeverity) == 3


class TestPhaseErrorSeverity:
    """Tests for severity field on PhaseError."""

    def test_default_severity_is_error(self) -> None:
        """PhaseError defaults to ERROR severity."""
        error = PhaseError(phase="writer", file_path="a.txt", message="fail")
        assert error.severity == ErrorSeverity.ERROR

    def test_explicit_severity(self) -> None:
        """PhaseError accepts explicit severity."""
        error = PhaseError(
            phase="collector",
            file_path="b.txt",
            message="skipped",
            severity=ErrorSeverity.WARNING,
        )
        assert error.severity == ErrorSeverity.WARNING

    def test_fatal_severity(self) -> None:
        """PhaseError accepts FATAL severity."""
        error = PhaseError(
            phase="writer",
            file_path="",
            message="disk gone",
            severity=ErrorSeverity.FATAL,
        )
        assert error.severity == ErrorSeverity.FATAL


class TestBackupResultWarnings:
    """Tests for warning-level errors in BackupResult."""

    def test_add_warning_does_not_fail_backup(self) -> None:
        """Warnings do not make success False."""
        result = BackupResult()
        result.add_warning("collector", "a.txt", "access slow")
        assert result.success is True

    def test_warnings_count(self) -> None:
        """warnings property counts WARNING entries."""
        result = BackupResult()
        result.add_warning("collector", "a.txt", "slow")
        result.add_warning("collector", "b.txt", "slow")
        assert result.warnings == 2

    def test_errors_excludes_warnings(self) -> None:
        """errors property does not count warnings."""
        result = BackupResult()
        result.add_warning("collector", "a.txt", "slow")
        result.add_error("writer", "b.txt", "fail")
        assert result.errors == 1
        assert result.warnings == 1

    def test_success_with_only_warnings(self) -> None:
        """Backup with only warnings is still successful."""
        result = BackupResult(files_processed=5)
        result.add_warning("collector", "a.txt", "retried")
        result.add_warning("collector", "b.txt", "retried")
        assert result.success is True
        assert "successful" in result.error_summary().lower()
        assert "warning" in result.error_summary().lower()

    def test_add_error_with_explicit_severity(self) -> None:
        """add_error accepts severity parameter."""
        result = BackupResult()
        result.add_error("writer", "x.txt", "fail", severity=ErrorSeverity.FATAL)
        assert result.phase_errors[0].severity == ErrorSeverity.FATAL


class TestBackupResultFatal:
    """Tests for FATAL severity in BackupResult."""

    def test_has_fatal_errors_false_by_default(self) -> None:
        """No fatal errors when result is empty."""
        result = BackupResult()
        assert result.has_fatal_errors is False

    def test_has_fatal_errors_true(self) -> None:
        """has_fatal_errors is True when FATAL error exists."""
        result = BackupResult()
        result.add_error("writer", "", "disk removed", severity=ErrorSeverity.FATAL)
        assert result.has_fatal_errors is True

    def test_success_false_with_fatal(self) -> None:
        """success is False with a FATAL error."""
        result = BackupResult()
        result.add_error("writer", "", "boom", severity=ErrorSeverity.FATAL)
        assert result.success is False

    def test_has_fatal_errors_false_with_only_warnings(self) -> None:
        """has_fatal_errors is False with only warnings."""
        result = BackupResult()
        result.add_warning("collector", "a.txt", "retried")
        assert result.has_fatal_errors is False


class TestErrorSummaryWithSeverity:
    """Tests for error_summary with mixed severity levels."""

    def test_summary_shows_severity_tags(self) -> None:
        """Error summary includes severity tags for non-warning errors."""
        result = BackupResult(files_processed=10)
        result.add_error("writer", "a.txt", "fail A")
        result.add_error("writer", "", "disk full", severity=ErrorSeverity.FATAL)
        summary = result.error_summary()
        assert "ERROR" in summary
        assert "FATAL" in summary

    def test_summary_warnings_not_shown_as_errors(self) -> None:
        """Warnings are summarized separately, not listed with errors."""
        result = BackupResult(files_processed=10)
        result.add_error("writer", "a.txt", "fail")
        result.add_warning("collector", "b.txt", "slow read")
        summary = result.error_summary()
        assert "1 error" in summary.lower()
        assert "warning" in summary.lower()
