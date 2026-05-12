"""Tests for the auto-detection helpers and cost-formatting utilities
in :mod:`src.storage.s3_setup`.

These functions are exercised by the setup wizard to pick a sensible
default AWS region and currency for the user. They have to degrade
gracefully when the network call fails or the locale data is exotic,
because the wizard runs offline-tolerant.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.storage.s3_setup import (
    _detect_region_by_ip,
    _detect_region_by_timezone,
    _fmt_number,
    _region_from_coords,
    detect_local_currency,
    detect_nearest_region,
    format_cost,
)

# ---------------------------------------------------------------------------
# _region_from_coords
# ---------------------------------------------------------------------------


class TestRegionFromCoords:
    """Pure mapping function — coordinate ranges → AWS region code.

    Each branch in the code corresponds to one assertion below; the
    point is to pin every region's bounding box so a future tweak to
    the lat/lon zones cannot silently misroute users.
    """

    @pytest.mark.parametrize(
        "lat,lon,expected",
        [
            # Western Europe (lon < 0)
            (51.5, -0.1, "eu-west-1"),  # London
            # France/Benelux (0 ≤ lon < 5)
            (48.85, 2.35, "eu-west-3"),  # Paris
            # Central Europe (5 ≤ lon < 20)
            (52.52, 13.4, "eu-central-1"),  # Berlin
            # Eastern/Northern Europe (lon ≥ 20)
            (60.17, 24.94, "eu-north-1"),  # Helsinki
        ],
    )
    def test_europe_zones(self, lat: float, lon: float, expected: str) -> None:
        assert _region_from_coords(lat, lon) == expected

    @pytest.mark.parametrize(
        "lat,lon,expected",
        [
            # us-west-2 (lon < -100)
            (47.6, -122.3, "us-west-2"),  # Seattle
            # us-east-2 (-100 ≤ lon < -85)
            (41.9, -87.6, "us-east-2"),  # Chicago
            # us-east-1 (lon ≥ -85)
            (40.7, -74.0, "us-east-1"),  # New York
        ],
    )
    def test_north_america_zones(self, lat: float, lon: float, expected: str) -> None:
        assert _region_from_coords(lat, lon) == expected

    def test_south_america(self) -> None:
        # São Paulo (-23.55, -46.63)
        assert _region_from_coords(-23.55, -46.63) == "sa-east-1"

    @pytest.mark.parametrize(
        "lat,lon,expected",
        [
            (35.7, 139.7, "ap-northeast-1"),  # Tokyo (lon > 120)
            (1.35, 103.8, "ap-southeast-1"),  # Singapore (95 < lon ≤ 120)
            (28.6, 77.2, "ap-south-1"),  # Delhi (lon ≤ 95)
        ],
    )
    def test_asia_pacific_zones(self, lat: float, lon: float, expected: str) -> None:
        assert _region_from_coords(lat, lon) == expected

    def test_australia(self) -> None:
        # Sydney (-33.87, 151.21)
        assert _region_from_coords(-33.87, 151.21) == "ap-southeast-2"

    def test_unmappable_returns_empty_string(self) -> None:
        # Antarctica — no AWS region nearby.
        assert _region_from_coords(-80.0, 0.0) == ""
        # Mid-Atlantic
        assert _region_from_coords(0.0, -30.0) == ""


# ---------------------------------------------------------------------------
# _detect_region_by_ip
# ---------------------------------------------------------------------------


def _fake_urlopen(payload: dict | str, raise_exc: Exception | None = None):
    """Build a mock for ``urllib.request.urlopen`` that returns *payload*.

    Used to patch the network call without making real HTTP requests.
    The mock supports being used as a context manager (``with urlopen
    (...) as resp``).
    """
    if raise_exc is not None:
        cm = MagicMock(side_effect=raise_exc)
        return cm

    raw = payload if isinstance(payload, str) else json.dumps(payload)
    response = MagicMock()
    response.read.return_value = raw.encode("utf-8")
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=response)
    cm.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=cm)


class TestDetectRegionByIp:
    """Network-mocked tests for the IP geolocation path."""

    def test_known_timezone_in_response(self) -> None:
        payload = {"lat": 0, "lon": 0, "timezone": "Europe/Paris"}
        with patch("urllib.request.urlopen", _fake_urlopen(payload)):
            assert _detect_region_by_ip() == "eu-west-3"

    def test_unknown_timezone_falls_back_to_coords(self) -> None:
        # ``Atlantis/Lost`` is not in the timezone table, so the
        # function tries lat/lon next — Berlin-ish coords → eu-central-1.
        payload = {"lat": 52.5, "lon": 13.4, "timezone": "Atlantis/Lost"}
        with patch("urllib.request.urlopen", _fake_urlopen(payload)):
            assert _detect_region_by_ip() == "eu-central-1"

    def test_unmappable_coords_returns_empty(self) -> None:
        # Antarctica — no zone matches, no timezone match.
        payload = {"lat": -80.0, "lon": 0.0, "timezone": "Antarctica/Vostok"}
        with patch("urllib.request.urlopen", _fake_urlopen(payload)):
            assert _detect_region_by_ip() == ""

    def test_network_error_returns_empty(self) -> None:
        # OSError is the broadest network failure — DNS, connect, etc.
        with patch("urllib.request.urlopen", side_effect=OSError("dns failure")):
            assert _detect_region_by_ip() == ""

    def test_invalid_json_returns_empty(self) -> None:
        with patch("urllib.request.urlopen", _fake_urlopen("not-valid-json")):
            assert _detect_region_by_ip() == ""

    def test_missing_fields_returns_empty(self) -> None:
        # Response is valid JSON but missing the keys we need.
        with patch("urllib.request.urlopen", _fake_urlopen({})):
            assert _detect_region_by_ip() == ""


# ---------------------------------------------------------------------------
# _detect_region_by_timezone
# ---------------------------------------------------------------------------


class TestDetectRegionByTimezone:
    """Local-system fallback used when the IP API is unreachable."""

    def test_known_iana_name_substring(self) -> None:
        # ``time.tzname[0]`` returning a string contained in a known IANA
        # name (e.g. "Europe/Paris" matches the substring "Europe/Paris")
        # picks that region directly.
        with patch("time.tzname", ("Europe/Paris", "")):
            assert _detect_region_by_timezone() == "eu-west-3"

    def test_unknown_name_falls_back_to_utc_offset(self) -> None:
        # Windows-style timezone — not an IANA name. We mock both
        # ``time.tzname`` AND the offset so the UTC fallback kicks in.
        with (
            patch("time.tzname", ("Romance Standard Time", "Romance Daylight Time")),
            patch("time.timezone", -3600),  # UTC+1 (timezone is signed opposite)
            patch("time.daylight", 0),
            patch("time.altzone", -7200),
        ):
            # offset_seconds = -timezone = 3600 → 1 hour → eu-west-3
            assert _detect_region_by_timezone() == "eu-west-3"

    def test_unknown_offset_returns_default(self) -> None:
        # An offset not present in ``_UTC_OFFSET_TO_REGION`` (e.g. UTC+12)
        # collapses to the documented default eu-west-1.
        with (
            patch("time.tzname", ("XYZ", "XYZ")),
            patch("time.timezone", -43200),  # UTC+12
            patch("time.daylight", 0),
        ):
            assert _detect_region_by_timezone() == "eu-west-1"

    def test_offset_calculation_during_dst(self) -> None:
        # ``daylight != 0`` → the function should use ``-time.altzone``
        # rather than ``-time.timezone`` so a DST'd machine still maps
        # to the active offset.
        with (
            patch("time.tzname", ("XYZ", "XYZ")),
            patch("time.timezone", 0),  # not used in DST branch
            patch("time.daylight", 1),
            patch("time.altzone", -3600),  # DST UTC+1
        ):
            assert _detect_region_by_timezone() == "eu-west-3"

    def test_exception_returns_default(self) -> None:
        # If the offset arithmetic blows up (e.g. ``time.timezone`` is
        # of an exotic non-int type after some C-extension monkey-patch),
        # the function must still produce the documented default. We
        # emulate the failure by replacing ``time.altzone`` with a
        # string — the unary minus then raises ``TypeError`` which the
        # ``except Exception`` branch catches.
        with (
            patch("time.tzname", ("XYZ", "XYZ")),
            patch("time.daylight", 1),  # forces -time.altzone path
            patch("time.altzone", "not-an-int"),
        ):
            assert _detect_region_by_timezone() == "eu-west-1"


# ---------------------------------------------------------------------------
# detect_nearest_region (top-level orchestrator)
# ---------------------------------------------------------------------------


class TestDetectNearestRegion:
    """Composition of ``_detect_region_by_ip`` and ``_detect_region_by_timezone``."""

    def test_ip_succeeds_short_circuits(self) -> None:
        with patch("src.storage.s3_setup._detect_region_by_ip", return_value="us-west-2"):
            assert detect_nearest_region() == "us-west-2"

    def test_ip_empty_falls_back_to_timezone(self) -> None:
        with (
            patch("src.storage.s3_setup._detect_region_by_ip", return_value=""),
            patch(
                "src.storage.s3_setup._detect_region_by_timezone",
                return_value="eu-central-1",
            ),
        ):
            assert detect_nearest_region() == "eu-central-1"


# ---------------------------------------------------------------------------
# detect_local_currency
# ---------------------------------------------------------------------------


def _two_call_urlopen(first: dict, second: dict) -> MagicMock:
    """Mock ``urlopen`` returning *first* on call #1 and *second* on call #2."""
    responses = []
    for payload in (first, second):
        resp = MagicMock()
        resp.read.return_value = json.dumps(payload).encode("utf-8")
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=resp)
        cm.__exit__ = MagicMock(return_value=False)
        responses.append(cm)
    return MagicMock(side_effect=responses)


class TestDetectLocalCurrency:
    """Currency detection is best-effort and must always return a tuple."""

    def test_usd_returns_dollar_and_one(self) -> None:
        with patch("urllib.request.urlopen", _fake_urlopen({"currency": "USD"})):
            symbol, rate = detect_local_currency()
        assert symbol == "$"
        assert rate == 1.0

    def test_eur_uses_currency_map_symbol(self) -> None:
        ip_payload = {"currency": "EUR"}
        rates_payload = {"rates": {"EUR": 0.92}}
        with patch("urllib.request.urlopen", _two_call_urlopen(ip_payload, rates_payload)):
            symbol, rate = detect_local_currency()
        assert symbol == "€"  # €
        assert rate == 0.92

    def test_unknown_currency_uses_code_as_symbol(self) -> None:
        # ZAR is not in the symbol map → the code itself is the symbol.
        ip_payload = {"currency": "ZAR"}
        rates_payload = {"rates": {"ZAR": 18.5}}
        with patch("urllib.request.urlopen", _two_call_urlopen(ip_payload, rates_payload)):
            symbol, rate = detect_local_currency()
        assert symbol == "ZAR"
        assert rate == 18.5

    def test_missing_rate_uses_one(self) -> None:
        ip_payload = {"currency": "EUR"}
        rates_payload = {"rates": {}}  # No EUR rate
        with patch("urllib.request.urlopen", _two_call_urlopen(ip_payload, rates_payload)):
            symbol, rate = detect_local_currency()
        # Symbol still resolved from map, rate falls back to 1.0.
        assert symbol == "€"
        assert rate == 1.0

    def test_network_error_returns_dollar_and_one(self) -> None:
        with patch("urllib.request.urlopen", side_effect=OSError("offline")):
            symbol, rate = detect_local_currency()
        assert symbol == "$"
        assert rate == 1.0

    def test_default_currency_when_field_missing(self) -> None:
        # ``data.get("currency", "USD")`` → defaults to USD when the key
        # is absent, so the function returns the dollar tuple without
        # making the second (rates) HTTP call.
        with patch("urllib.request.urlopen", _fake_urlopen({})):
            symbol, rate = detect_local_currency()
        assert symbol == "$"
        assert rate == 1.0


# ---------------------------------------------------------------------------
# _fmt_number
# ---------------------------------------------------------------------------


class TestFmtNumber:
    """Thousands-separator formatter using a non-breaking-style space."""

    @pytest.mark.parametrize(
        "n,expected",
        [
            (0, "0"),
            (1, "1"),
            (12, "12"),
            (123, "123"),
            (1_000, "1 000"),
            (12_345, "12 345"),
            (1_234_567, "1 234 567"),
            (1_000_000_000, "1 000 000 000"),
        ],
    )
    def test_grouping(self, n: int, expected: str) -> None:
        assert _fmt_number(n) == expected


# ---------------------------------------------------------------------------
# format_cost
# ---------------------------------------------------------------------------


class TestFormatCost:
    """Cost rendering with optional local currency."""

    def test_usd_only_when_rate_one(self) -> None:
        # rate == 1.0 → no local-currency suffix even if a symbol is given.
        assert format_cost(53.0, "$", 1.0) == "$53"

    def test_usd_only_when_symbol_dollar(self) -> None:
        # Symbol ``$`` is treated as USD even if the rate is non-1.0
        # (defensive — rate 1.0 with a different symbol is meaningless).
        assert format_cost(53.0, "$", 0.92) == "$53"

    def test_eur_appended_when_real_local_currency(self) -> None:
        result = format_cost(100.0, "EUR", 0.92)
        assert result.startswith("$100")
        assert "EUR" in result
        assert "92" in result  # 100 × 0.92 = 92

    def test_unicode_symbol_round_trips(self) -> None:
        result = format_cost(100.0, "€", 0.92)
        assert "€" in result

    def test_rounding_to_integer(self) -> None:
        # 53.49 rounds to 53; 53.51 rounds to 54.
        assert format_cost(53.49, "$", 1.0) == "$53"
        assert format_cost(53.51, "$", 1.0) == "$54"

    def test_thousands_separator_in_both_amounts(self) -> None:
        result = format_cost(12_345.0, "EUR", 1.5)
        # Both numbers (12 345 and 18 518) carry the space separator.
        assert "12 345" in result
        assert "18 518" in result or "18 517" in result  # rounding tolerance
