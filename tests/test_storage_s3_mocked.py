"""S3 backend tests that rely only on unittest.mock (no moto).

These tests cover behaviors that moto does not easily simulate:
- delete_objects returning per-object Errors while HTTP 200 succeeds
- test_connection probing write unless Object Lock is active
- _make_progress_cb serializing concurrent increments from s3transfer
"""

import threading
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.storage.s3 import S3Storage


class TestS3DeleteErrorsSurfaced:
    """delete_backup must raise when S3 reports per-object failures.

    S3 returns HTTP 200 even when Object Lock / legal hold blocks
    individual deletes. Silently consuming the 200 made rotation
    believe objects were freed when they were still billed and still
    counted as "has_full" in the chain logic.
    """

    def test_delete_raises_when_errors_present(self):
        backend = S3Storage(
            bucket="b",
            prefix="",
            region="us-east-1",
            access_key="k",
            secret_key="s",
        )

        fake_client = MagicMock()
        fake_paginator = MagicMock()
        fake_paginator.paginate.return_value = [{"Contents": [{"Key": "locked_backup/file.bin"}]}]
        fake_client.get_paginator.return_value = fake_paginator
        fake_client.delete_objects.return_value = {
            "Errors": [
                {
                    "Key": "locked_backup/file.bin",
                    "Code": "AccessDenied",
                    "Message": "Object Lock retention active",
                }
            ],
        }
        with (
            patch.object(backend, "_get_client", return_value=fake_client),
            pytest.raises(OSError, match="Failed to delete"),
        ):
            backend.delete_backup("locked_backup")

    def test_delete_succeeds_when_no_errors(self):
        backend = S3Storage(
            bucket="b",
            prefix="",
            region="us-east-1",
            access_key="k",
            secret_key="s",
        )

        fake_client = MagicMock()
        fake_paginator = MagicMock()
        fake_paginator.paginate.return_value = [{"Contents": [{"Key": "ok_backup/file.bin"}]}]
        fake_client.get_paginator.return_value = fake_paginator
        # No Errors key, or empty list — both must be treated as success.
        fake_client.delete_objects.return_value = {"Errors": []}
        with patch.object(backend, "_get_client", return_value=fake_client):
            backend.delete_backup("ok_backup")  # Must not raise


class TestProgressCallbackThreadSafe:
    """_make_progress_cb must serialize concurrent increments.

    boto3's s3transfer dispatches the callback from multiple worker
    threads during multipart upload. Unsynchronized ``sent[0] += n``
    produces backwards jumps in the progress bar.
    """

    def test_concurrent_callbacks_are_monotonic(self):
        backend = S3Storage(bucket="b", prefix="", region="us-east-1")

        reported: list[int] = []
        reported_lock = threading.Lock()

        def ui_cb(sent: int, total: int) -> None:
            with reported_lock:
                reported.append(sent)

        backend.set_progress_callback(ui_cb)
        cb = backend._make_progress_cb(total=10_000)

        def worker():
            for _ in range(500):
                cb(1)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Every reported snapshot must be monotonically non-decreasing.
        for a, b in zip(reported, reported[1:], strict=False):
            assert a <= b, f"Progress went backwards: {a} -> {b}"
        # All 5000 increments were delivered.
        assert reported[-1] == 5000

    def test_no_callback_returns_none(self):
        """When no UI callback registered, _make_progress_cb returns None."""
        backend = S3Storage(bucket="b", prefix="", region="us-east-1")
        assert backend._make_progress_cb(total=100) is None


class TestS3TestConnectionProbesWrite:
    """test_connection must probe write unless under Object Lock."""

    def test_probe_write_called_when_no_object_lock(self):
        backend = S3Storage(
            bucket="b",
            prefix="",
            region="us-east-1",
            access_key="k",
            secret_key="s",
        )
        fake_client = MagicMock()
        with (
            patch.object(backend, "_get_client", return_value=fake_client),
            patch.object(backend, "_probe_write") as probe,
        ):
            ok, _ = backend.test_connection()
            assert ok is True
            probe.assert_called_once()

    def test_probe_write_skipped_under_object_lock(self):
        backend = S3Storage(
            bucket="b",
            prefix="",
            region="us-east-1",
            access_key="k",
            secret_key="s",
        )
        backend.set_retain_until(datetime.now(UTC) + timedelta(days=7))
        fake_client = MagicMock()
        with (
            patch.object(backend, "_get_client", return_value=fake_client),
            patch.object(backend, "_probe_write") as probe,
        ):
            ok, _ = backend.test_connection()
            assert ok is True
            probe.assert_not_called()

    def test_probe_write_cleans_up_probe_object(self):
        """The probe must delete the object it uploaded, even on success."""
        backend = S3Storage(
            bucket="b",
            prefix="prefix",
            region="us-east-1",
            access_key="k",
            secret_key="s",
        )
        fake_client = MagicMock()
        backend._probe_write(fake_client, "prefix/")

        assert fake_client.put_object.call_count == 1
        assert fake_client.delete_object.call_count == 1
        # Same key used for put and delete.
        put_key = fake_client.put_object.call_args.kwargs["Key"]
        del_key = fake_client.delete_object.call_args.kwargs["Key"]
        assert put_key == del_key

    def test_probe_write_cleans_up_after_put_failure(self):
        """Even if put_object raises, a delete must be attempted."""
        backend = S3Storage(
            bucket="b",
            prefix="",
            region="us-east-1",
            access_key="k",
            secret_key="s",
        )
        fake_client = MagicMock()
        fake_client.put_object.side_effect = RuntimeError("denied")

        with pytest.raises(RuntimeError):
            backend._probe_write(fake_client, "")

        fake_client.delete_object.assert_called_once()
