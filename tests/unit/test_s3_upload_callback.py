"""Regression test: encrypted-S3 upload Callback emits progress and
honours cancellation (audit L7 finding #6).

The raw client.upload_file path used to pass no Callback, so the user's
Cancel button did nothing during the whole archive upload and the UI
got no progress.
"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.core.exceptions import CancelledError
from src.core.phases.remote_writer import _make_s3_upload_callback


class TestS3UploadCallback:
    def test_progress_accumulates(self):
        phase_log = SimpleNamespace(progress=Mock())
        cb = _make_s3_upload_callback(total_bytes=100, phase_log=phase_log, cancel_check=None)

        cb(40)
        cb(60)

        assert phase_log.progress.call_count == 2
        # Last call reports the full 100/100.
        last = phase_log.progress.call_args.kwargs
        assert last["current"] == 100
        assert last["total"] == 100

    def test_progress_capped_at_total(self):
        phase_log = SimpleNamespace(progress=Mock())
        cb = _make_s3_upload_callback(total_bytes=10, phase_log=phase_log, cancel_check=None)
        cb(50)  # overshoot
        assert phase_log.progress.call_args.kwargs["current"] == 10

    def test_cancel_raises_and_aborts(self):
        def _cancel():
            raise CancelledError("user cancelled")

        cb = _make_s3_upload_callback(total_bytes=100, phase_log=None, cancel_check=_cancel)
        with pytest.raises(CancelledError):
            cb(10)

    def test_no_phase_log_no_cancel_is_safe(self):
        cb = _make_s3_upload_callback(total_bytes=0, phase_log=None, cancel_check=None)
        cb(10)  # must not raise (total_bytes=0 → denominator floored at 1)
