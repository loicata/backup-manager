"""Tests for ``ProfilePrefixFilter`` and the thread-local context.

Two parallel runs each set their own profile name on their own thread
and emit log lines; the prefix filter must tag each line with the
right name regardless of interleaving.
"""

from __future__ import annotations

import logging
import threading

import pytest

from src.core.log_context import (
    ProfilePrefixFilter,
    clear_profile_context,
    current_profile_context,
    set_profile_context,
)


@pytest.fixture(autouse=True)
def _clear_ctx_after_each_test():
    yield
    clear_profile_context()


def _make_record(message: str, *args) -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg=message,
        args=args or None,
        exc_info=None,
    )


class TestContextHelpers:
    def test_default_context_is_none(self):
        assert current_profile_context() is None

    def test_set_then_get(self):
        set_profile_context("Alpha")
        assert current_profile_context() == "Alpha"

    def test_clear_resets_to_none(self):
        set_profile_context("Alpha")
        clear_profile_context()
        assert current_profile_context() is None


class TestProfilePrefixFilter:
    def test_no_context_leaves_message_untouched(self):
        record = _make_record("hello")
        flt = ProfilePrefixFilter()

        assert flt.filter(record) is True
        assert record.msg == "hello"

    def test_with_context_prefixes_message(self):
        set_profile_context("L2")
        record = _make_record("starting backup")
        flt = ProfilePrefixFilter()

        flt.filter(record)

        assert record.msg == "[L2] starting backup"

    def test_lazy_format_is_resolved_before_prefix(self):
        set_profile_context("TestLoic")
        record = _make_record("Saved profile %s", "abc")
        flt = ProfilePrefixFilter()

        flt.filter(record)

        assert record.msg == "[TestLoic] Saved profile abc"
        assert record.args is None

    def test_filter_returns_true_to_keep_record(self):
        set_profile_context("X")
        record = _make_record("anything")
        flt = ProfilePrefixFilter()

        assert flt.filter(record) is True


class TestThreadIsolation:
    """Each thread carries its own profile context. Two parallel runs
    must not bleed into each other's log lines."""

    def test_two_threads_see_independent_contexts(self):
        seen = {}
        ready = threading.Event()
        proceed = threading.Event()

        def _worker(name: str) -> None:
            set_profile_context(name)
            ready.wait()
            seen[name] = current_profile_context()
            proceed.set()

        threads = [
            threading.Thread(target=_worker, args=("A",)),
            threading.Thread(target=_worker, args=("B",)),
        ]
        for thread in threads:
            thread.start()

        ready.set()
        for thread in threads:
            thread.join(timeout=5)

        assert seen == {"A": "A", "B": "B"}
        # The main thread should still see no context — neither
        # worker leaked their value.
        assert current_profile_context() is None
