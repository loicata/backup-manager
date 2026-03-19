"""Tests for src.core.events — EventBus."""

import threading

from src.core.events import EventBus, PROGRESS, LOG, STATUS


class TestEventBus:
    def test_subscribe_and_emit_calls_callback(self):
        bus = EventBus()
        results = []
        bus.subscribe(PROGRESS, lambda **kw: results.append(kw))
        bus.emit(PROGRESS, current=1, total=10)
        assert results == [{"current": 1, "total": 10}]

    def test_multiple_subscribers_all_called(self):
        bus = EventBus()
        r1, r2 = [], []
        bus.subscribe(LOG, lambda **kw: r1.append(kw))
        bus.subscribe(LOG, lambda **kw: r2.append(kw))
        bus.emit(LOG, message="test")
        assert len(r1) == 1
        assert len(r2) == 1

    def test_different_event_types_independent(self):
        bus = EventBus()
        results = []
        bus.subscribe(PROGRESS, lambda **kw: results.append("progress"))
        bus.subscribe(LOG, lambda **kw: results.append("log"))
        bus.emit(PROGRESS, current=1, total=1)
        assert results == ["progress"]

    def test_unsubscribe_removes_callback(self):
        bus = EventBus()
        results = []
        cb = lambda **kw: results.append(1)
        bus.subscribe(LOG, cb)
        bus.unsubscribe(LOG, cb)
        bus.emit(LOG, message="test")
        assert results == []

    def test_unsubscribe_nonexistent_callback_no_error(self):
        bus = EventBus()
        bus.unsubscribe(LOG, lambda **kw: None)  # Should not raise

    def test_emit_unknown_event_no_error(self):
        bus = EventBus()
        bus.emit("unknown_event", data="test")  # Should not raise

    def test_callback_exception_does_not_block_others(self):
        bus = EventBus()
        results = []

        def bad_cb(**kw):
            raise ValueError("boom")

        bus.subscribe(LOG, bad_cb)
        bus.subscribe(LOG, lambda **kw: results.append("ok"))
        bus.emit(LOG, message="test")
        assert results == ["ok"]

    def test_duplicate_subscribe_ignored(self):
        bus = EventBus()
        results = []
        cb = lambda **kw: results.append(1)
        bus.subscribe(LOG, cb)
        bus.subscribe(LOG, cb)
        bus.emit(LOG, message="test")
        assert results == [1]  # Called once, not twice

    def test_clear_removes_all_subscribers(self):
        bus = EventBus()
        results = []
        bus.subscribe(LOG, lambda **kw: results.append(1))
        bus.subscribe(PROGRESS, lambda **kw: results.append(2))
        bus.clear()
        bus.emit(LOG, message="test")
        bus.emit(PROGRESS, current=1, total=1)
        assert results == []

    def test_thread_safety(self):
        bus = EventBus()
        results = []
        lock = threading.Lock()

        def cb(**kw):
            with lock:
                results.append(threading.current_thread().name)

        bus.subscribe(LOG, cb)

        threads = []
        for i in range(10):
            t = threading.Thread(
                target=lambda: bus.emit(LOG, message="test"),
                name=f"t-{i}",
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 10

    def test_emit_with_no_data(self):
        bus = EventBus()
        results = []
        bus.subscribe(STATUS, lambda **kw: results.append(kw))
        bus.emit(STATUS)
        assert results == [{}]
