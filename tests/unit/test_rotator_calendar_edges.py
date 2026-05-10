"""Calendar-edge tests for ``rotator._rotate_gfs``.

``test_rotator_edge_cases.py`` already covers leap-year Feb 29, the
ISO-week boundary at year transitions, and the DIFF-parent protection
chain. This module fills the **timezone & clock** gaps the existing
suite does not pin down explicitly:

- DST forward / backward: the rotator runs in UTC, but mtimes on disk
  are produced by ``Path.stat().st_mtime`` which is a Unix timestamp
  derived from local wall-clock. A backup taken just before the
  spring-forward boundary and one just after must land in the right
  UTC day even when the host's local time made an hour disappear.
- Naive ``datetime.now`` monkey-patched in tests: production callers
  always pass aware UTC, but the test harness elsewhere injects naive
  datetimes. The rotator's defensive normalisation
  (``if now.tzinfo is None: now = now.replace(tzinfo=UTC)``) is
  exercised here explicitly so a future refactor cannot silently
  remove it.
- Clock skew: a backup with ``mtime`` strictly in the future of ``now``
  (a network share with a fast clock, a flaky NTP host) must not
  crash the rotator nor be silently kept under the daily window
  forever.
- Exact month-window boundary: a FULL backup whose age equals
  ``gfs_monthly`` *exactly* must fall outside the window
  (``<`` is strict in the rotator), confirming the off-by-one
  contract the user-facing UI relies on.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.core.config import RetentionConfig
from src.core.phases.rotator import rotate_backups


def _backup(name: str, dt: datetime) -> dict:
    """Build a backup dict with a Unix-epoch ``modified`` timestamp.

    ``dt.timestamp()`` follows the platform's calendar — for an aware
    datetime this is unambiguous, for a naive one it implies local
    time. We only feed aware datetimes here so the conversion is
    deterministic regardless of host timezone.
    """
    return {"name": name, "modified": dt.timestamp()}


def _make_backend(backups: list[dict]) -> MagicMock:
    backend = MagicMock()
    backend.list_backups.return_value = backups
    return backend


# ---------------------------------------------------------------------------
# DST transitions
# ---------------------------------------------------------------------------


class TestDstTransitions:
    """The rotator is tz-aware (UTC) so DST shifts must not split days."""

    def test_dst_spring_forward_does_not_split_daily_window(self):
        """Two backups straddling EU spring-forward (2026-03-29 01:00 UTC)
        are both inside a 1-day window when ``now`` is the same evening UTC.

        Without the UTC normalisation a naive ``(local_now - local_dt).days``
        would gain or lose an hour and could cap the window at 0 days for
        the older backup, evicting it before the user expects.
        """
        # 2026-03-29: in the UE Paris time, 02:00 jumps to 03:00.
        # In UTC the moment is 2026-03-29 01:00 — unaffected.
        before = datetime(2026, 3, 29, 0, 30, tzinfo=UTC)  # 01:30 Paris
        after = datetime(2026, 3, 29, 1, 30, tzinfo=UTC)  # 03:30 Paris (post-jump)
        now_utc = datetime(2026, 3, 29, 22, 0, tzinfo=UTC)

        backups = [
            _backup("Pf_FULL_2026-03-29_013000", before),
            _backup("Pf_FULL_2026-03-29_033000", after),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_utc
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention, profile_name="Pf")

        # Both within the 1-day window — neither must be evicted.
        backend.delete_backup.assert_not_called()

    def test_dst_fall_back_does_not_double_count(self):
        """At 2026-10-25 the EU "falls back" — wall-clock 02:30 happens
        twice. Backups before and inside the repeated hour must still be
        ordered correctly when stored as UTC timestamps.
        """
        # Before the cutover: 00:30 UTC = 02:30 Paris (CEST, UTC+2)
        # After the cutover:  01:30 UTC = 02:30 Paris (CET,  UTC+1)
        # Same wall-clock, different UTC instants — must order naturally.
        first = datetime(2026, 10, 25, 0, 30, tzinfo=UTC)
        second = datetime(2026, 10, 25, 1, 30, tzinfo=UTC)
        now_utc = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)

        backups = [
            _backup("Pf_FULL_2026-10-25_023001", first),
            _backup("Pf_FULL_2026-10-25_023002", second),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_utc
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention, profile_name="Pf")

        # Both within the daily window, neither pruned.
        backend.delete_backup.assert_not_called()


# ---------------------------------------------------------------------------
# Naive ``now`` normalisation
# ---------------------------------------------------------------------------


class TestNaiveNowNormalisation:
    """``datetime.now`` mocked with a naive value must be treated as UTC.

    Documented at ``rotator.py:117`` — the normalisation guards every
    test in the suite that does the easy ``datetime(2026, ...)`` mock
    instead of an aware ``datetime(..., tzinfo=UTC)``. We pin it here.
    """

    def test_naive_now_equivalent_to_aware_utc(self):
        """A naive ``now`` produces the same delete decisions as aware UTC."""
        ancient_dt = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        now_naive = datetime(2026, 3, 15, 12, 0)  # tzinfo is None
        now_aware = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)

        retention = RetentionConfig(gfs_daily=7, gfs_weekly=0, gfs_monthly=0)

        # Run with naive now.
        backend_a = _make_backend(
            [
                _backup("recent", now_aware),
                _backup("ancient", ancient_dt),
            ]
        )
        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_naive
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted_naive = rotate_backups(backend_a, retention)

        # Run with aware now.
        backend_b = _make_backend(
            [
                _backup("recent", now_aware),
                _backup("ancient", ancient_dt),
            ]
        )
        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_aware
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted_aware = rotate_backups(backend_b, retention)

        # Same input shape → same output count and same backups deleted.
        assert deleted_naive == deleted_aware == 1
        deleted_a = {c.args[0] for c in backend_a.delete_backup.call_args_list}
        deleted_b = {c.args[0] for c in backend_b.delete_backup.call_args_list}
        assert deleted_a == deleted_b == {"ancient"}


# ---------------------------------------------------------------------------
# Non-UTC mtime in the on-disk timestamp
# ---------------------------------------------------------------------------


class TestNonUtcMtimeRoundTrip:
    """``Path.stat().st_mtime`` is a Unix timestamp.

    The OS produces it from local wall-clock plus its tz database, so
    the interpretation is unambiguous — but the rotator must reach the
    right UTC instant via ``datetime.fromtimestamp(mtime, tz=UTC)``.
    A regression that dropped ``tz=UTC`` would shift every comparison
    by the host's UTC offset.
    """

    def test_eastern_authored_mtime_lands_on_correct_utc_day(self):
        """A backup authored at EST 23:30 lands on the next UTC day.

        Stored as a Unix timestamp, the rotator must place it on
        ``Y-M-D+1`` UTC, not on ``Y-M-D``.
        """
        # 2026-03-15 23:30 in UTC-5 (EST) = 2026-03-16 04:30 UTC
        est = timezone(timedelta(hours=-5))
        local_authored = datetime(2026, 3, 15, 23, 30, tzinfo=est)
        # ``now`` is one full UTC day after the file was authored.
        now_utc = datetime(2026, 3, 17, 4, 30, tzinfo=UTC)

        backups = [
            _backup("Pf_FULL_2026-03-16_043000", local_authored),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=2, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_utc
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention, profile_name="Pf")

        # Backup is 1 UTC day old, daily window is 2 → kept.
        backend.delete_backup.assert_not_called()


# ---------------------------------------------------------------------------
# Clock skew: future mtime
# ---------------------------------------------------------------------------


class TestFutureMtimeClockSkew:
    """A backup with ``mtime > now`` (NAS/NTP drift) must not crash."""

    def test_future_mtime_does_not_crash_and_is_kept(self):
        """The "most recent" guard always retains the newest backup,
        even if its mtime is in the future."""
        now_utc = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
        future = now_utc + timedelta(hours=2)  # NAS clock 2h ahead

        backups = [_backup("future_backup", future)]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_utc
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention)

        assert deleted == 0
        backend.delete_backup.assert_not_called()

    def test_future_mtime_alongside_old_does_not_evict_newest(self):
        """A future-dated newest + a real-old must keep the future as 'most recent'."""
        now_utc = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
        future = now_utc + timedelta(hours=2)
        ancient = now_utc - timedelta(days=400)

        backups = [
            _backup("future", future),
            _backup("ancient", ancient),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_utc
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention)

        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        # The newest (even though future-dated) survives via the
        # "always keep most recent" rule. Ancient is pruned.
        assert "future" not in deleted_names
        assert deleted == 1


# ---------------------------------------------------------------------------
# Exact monthly-window boundary (off-by-one contract)
# ---------------------------------------------------------------------------


class TestMonthlyWindowExactBoundary:
    """``months_ago < gfs_monthly`` is strict; the boundary must evict.

    UI label: "Keep the last 12 months" — a backup whose ``months_ago``
    equals 12 is **outside** the window. This is the contract a casual
    reader would expect (12 buckets total, indexed 0..11). A regression
    flipping to ``<=`` would silently keep one more month than promised.
    """

    def test_exactly_twelve_months_old_is_pruned(self):
        now_utc = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
        twelve_months_ago = datetime(2025, 3, 15, 12, 0, tzinfo=UTC)

        backups = [
            _backup("Pf_FULL_2026-03-15_120000", now_utc),
            _backup("Pf_FULL_2025-03-15_120000", twelve_months_ago),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=12)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_utc
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention, profile_name="Pf")

        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        assert "Pf_FULL_2025-03-15_120000" in deleted_names
        assert "Pf_FULL_2026-03-15_120000" not in deleted_names

    def test_eleven_months_old_is_kept(self):
        """Sanity guard for the symmetric strict-less-than: 11 < 12 → kept."""
        now_utc = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
        eleven_months_ago = datetime(2025, 4, 15, 12, 0, tzinfo=UTC)

        backups = [
            _backup("Pf_FULL_2026-03-15_120000", now_utc),
            _backup("Pf_FULL_2025-04-15_120000", eleven_months_ago),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=12)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now_utc
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention, profile_name="Pf")

        backend.delete_backup.assert_not_called()
