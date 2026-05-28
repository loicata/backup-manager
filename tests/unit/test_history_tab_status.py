"""Tests for the History tab status classifier and row actions.

The History tab now shows a Status column and supports right-click
Open / Copy path / Delete on each row. These tests lock in the log
classifier (success / cancelled / failed / unknown) and the
``_selected_log_path`` indirection that every action depends on.
"""

import pytest

from src.ui.tabs.history_tab import HistoryTab


@pytest.fixture()
def history_tab(tk_root, tmp_path):
    tab = HistoryTab(tk_root, log_dir=tmp_path)
    yield tab
    tab.destroy()


def _write_log(tmp_path, name: str, body: str):
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return p


class TestExtractStatus:
    """Classifier maps the log body to one of four labels."""

    def test_backup_complete_is_success(self, tmp_path):
        log = _write_log(
            tmp_path,
            "backup_a_1.log",
            "Starting backup 'P'\nBackup complete: 42 files in 1.0s",
        )
        assert HistoryTab._extract_status(log) == "success"

    def test_cancelled_marker_classified_as_cancelled(self, tmp_path):
        log = _write_log(
            tmp_path,
            "backup_a_2.log",
            "Starting backup 'P'\nCancelling backup...\nBackup cancelled by user",
        )
        assert HistoryTab._extract_status(log) == "cancelled"

    def test_explicit_failed_marker(self, tmp_path):
        log = _write_log(
            tmp_path,
            "backup_a_3.log",
            "Starting backup 'P'\nBackup failed: connection refused",
        )
        assert HistoryTab._extract_status(log) == "failed"

    def test_error_word_flags_failed(self, tmp_path):
        log = _write_log(
            tmp_path,
            "backup_a_4.log",
            "Starting backup 'P'\nsome ERROR happened",
        )
        assert HistoryTab._extract_status(log) == "failed"

    def test_unknown_when_no_markers(self, tmp_path):
        log = _write_log(
            tmp_path,
            "backup_a_5.log",
            "Starting backup 'P'\nBuilding integrity manifest...",
        )
        assert HistoryTab._extract_status(log) == "unknown"

    def test_success_beats_cancelled_in_reordered_log(self, tmp_path):
        """A completed run wins over an earlier cancel attempt even if
        both strings appear (e.g. the user cancelled a precheck retry
        then finally completed)."""
        log = _write_log(
            tmp_path,
            "backup_a_6.log",
            "Cancelling backup...\nBackup cancelled by user\n"
            "Starting new run\nBackup complete: 10 files in 0.5s",
        )
        # Our classifier priorities ``success`` over ``cancelled`` — anchor that.
        assert HistoryTab._extract_status(log) == "success"

    def test_no_changes_detected_classified_as_skipped(self, tmp_path):
        """A differential / incremental run that finds 0 changes emits
        the dual marker "No changes detected — backup skipped" + the
        usual "Backup complete: 0 files" epilogue. Since 3.7.44 the
        History tab distinguishes this from a real success.
        """
        log = _write_log(
            tmp_path,
            "backup_a_7.log",
            "Starting backup 'AWS Backup'\n"
            "Filter: 0 changed, 20 unchanged\n"
            "No changes detected — backup skipped\n"
            "Backup complete: 0 files in 0.0 min",
        )
        assert HistoryTab._extract_status(log) == "skipped"

    def test_skipped_beats_success_when_both_markers_present(self, tmp_path):
        """The skipped marker takes precedence over the generic
        "Backup complete:" line — the engine emits BOTH on a no-
        changes run, and we want the user to see "Skipped", not
        "Success", in the Status column."""
        log = _write_log(
            tmp_path,
            "backup_a_skip_priority.log",
            "No changes detected — backup skipped\nBackup complete: 0 files",
        )
        assert HistoryTab._extract_status(log) == "skipped"

    def test_compound_skip_marker_required(self, tmp_path):
        """Either token alone does NOT trigger skipped — both
        ``No changes detected`` AND ``backup skipped`` must be present.
        Defends against a hypothetical future log line that uses one
        token in isolation (e.g. an exclude-pattern message that
        happens to contain ``skipped``).
        """
        log_one_token = _write_log(
            tmp_path,
            "backup_a_skip_partial.log",
            "Some file skipped due to permission\nBackup complete: 10 files",
        )
        assert HistoryTab._extract_status(log_one_token) == "success"

    def test_skipped_displays_as_skipped_in_treeview(self, history_tab, tmp_path):
        """End-to-end: a skipped log shows the right label in the
        Status column AND carries the right tag. Catches a regression
        where ``status_display`` dict misses the ``skipped`` key (would
        fall back to ``"—"`` even though the row is tagged correctly).
        """
        _write_log(
            tmp_path,
            "backup_skip_e2e_20260528_100021.log",
            "Starting backup 'AWS Backup'\n"
            "No changes detected — backup skipped\n"
            "Backup complete: 0 files in 0.0 min",
        )
        history_tab.refresh()
        rows = list(history_tab.log_tree.get_children())
        assert len(rows) == 1
        values = history_tab.log_tree.item(rows[0], "values")
        assert values[2] == "Skipped", (
            f"Status column must display 'Skipped', got {values[2]!r}"
        )
        tags = history_tab.log_tree.item(rows[0], "tags")
        assert "skipped" in tags, f"Row must carry the skipped tag, got {tags!r}"


class TestRefreshPopulatesStatusAndPath:
    """refresh() writes the Status column and wires iid -> log path."""

    def test_rows_include_status_column_and_path_mapping(self, history_tab, tmp_path):
        log_a = _write_log(
            tmp_path,
            "backup_aa_20260417_100000.log",
            "Starting backup 'Pa'\nBackup complete: 1 files in 0.1s",
        )
        log_b = _write_log(
            tmp_path,
            "backup_bb_20260417_110000.log",
            "Starting backup 'Pb'\nBackup cancelled by user",
        )
        history_tab.refresh()

        rows = {
            history_tab._iid_to_path[iid]: history_tab.log_tree.item(iid, "values")
            for iid in history_tab.log_tree.get_children()
        }
        assert log_a in rows
        assert log_b in rows
        # Status is the third column
        assert rows[log_a][2] == "Success"
        assert rows[log_b][2] == "Cancelled"

    def test_selected_log_path_returns_mapping(self, history_tab, tmp_path):
        log = _write_log(
            tmp_path,
            "backup_sel_1.log",
            "Starting backup 'P'\nBackup complete: 1 files in 0.1s",
        )
        history_tab.refresh()
        first_iid = history_tab.log_tree.get_children()[0]
        history_tab.log_tree.selection_set(first_iid)

        assert history_tab._selected_log_path() == log

    def test_selected_log_path_is_none_when_no_selection(self, history_tab, tmp_path):
        _write_log(
            tmp_path,
            "backup_sel_2.log",
            "Starting backup 'P'\nBackup complete: 1 files in 0.1s",
        )
        history_tab.refresh()
        history_tab.log_tree.selection_set()  # clear
        assert history_tab._selected_log_path() is None

    def test_copy_path_writes_to_clipboard(self, history_tab, tmp_path):
        log = _write_log(
            tmp_path,
            "backup_clip_1.log",
            "Starting backup 'P'\nBackup complete: 1 files in 0.1s",
        )
        history_tab.refresh()
        history_tab.log_tree.selection_set(history_tab.log_tree.get_children()[0])

        history_tab._copy_selected_path()

        assert history_tab.clipboard_get() == str(log)

    def test_delete_removes_file_and_row(self, history_tab, tmp_path, monkeypatch):
        log = _write_log(
            tmp_path,
            "backup_del_1.log",
            "Starting backup 'P'\nBackup complete: 1 files in 0.1s",
        )
        history_tab.refresh()
        history_tab.log_tree.selection_set(history_tab.log_tree.get_children()[0])

        # Auto-confirm the "are you sure" inline panel (3.7.41 migration:
        # ``confirm_fn`` replaces ``messagebox.askyesno``). The fixture
        # builds the tab without callbacks — inject a True-returning
        # stub directly on the instance for this scenario.
        monkeypatch.setattr(history_tab, "_confirm_fn", lambda **kw: True)

        history_tab._delete_selected()

        assert not log.exists()
        assert len(history_tab.log_tree.get_children()) == 0

    def test_delete_aborted_when_user_declines(self, history_tab, tmp_path, monkeypatch):
        log = _write_log(
            tmp_path,
            "backup_del_2.log",
            "Starting backup 'P'\nBackup complete: 1 files in 0.1s",
        )
        history_tab.refresh()
        history_tab.log_tree.selection_set(history_tab.log_tree.get_children()[0])

        # Decline the inline confirm — log file must stay on disk.
        monkeypatch.setattr(history_tab, "_confirm_fn", lambda **kw: False)

        history_tab._delete_selected()

        assert log.exists()  # still there
