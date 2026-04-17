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
        # Our classifier priorities ``success`` first — anchor that.
        assert HistoryTab._extract_status(log) == "success"


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

        # Auto-confirm the "are you sure" dialog
        monkeypatch.setattr("src.ui.tabs.history_tab.messagebox.askyesno", lambda *a, **kw: True)

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

        monkeypatch.setattr("src.ui.tabs.history_tab.messagebox.askyesno", lambda *a, **kw: False)

        history_tab._delete_selected()

        assert log.exists()  # still there
