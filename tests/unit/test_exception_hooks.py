"""Tests for src.__main__._install_exception_hooks.

In the packaged windowed build there is no console, so the default Tk
``report_callback_exception`` and ``threading.excepthook`` (both writing
to ``sys.stderr``) discard tracebacks entirely. The hooks must redirect
them to the logger so a crash leaves a diagnosable trail.
"""

import threading
from unittest.mock import MagicMock

from src.__main__ import _install_exception_hooks


def test_tk_callback_hook_logs_exception():
    root = MagicMock()
    logger = MagicMock()

    _install_exception_hooks(root, logger)

    # The hook must have been wired onto the root window.
    hook = root.report_callback_exception
    assert callable(hook)

    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        hook(*sys.exc_info())

    logger.error.assert_called_once()
    # Full traceback is forwarded via exc_info=
    assert logger.error.call_args.kwargs.get("exc_info") is not None


def test_thread_hook_logs_exception():
    original = threading.excepthook
    try:
        root = MagicMock()
        logger = MagicMock()
        _install_exception_hooks(root, logger)

        args = MagicMock()
        args.exc_type = RuntimeError
        args.exc_value = RuntimeError("thread boom")
        args.exc_traceback = None
        args.thread = MagicMock()
        args.thread.name = "worker-1"

        threading.excepthook(args)

        logger.error.assert_called_once()
        assert "worker-1" in str(logger.error.call_args)
    finally:
        threading.excepthook = original


def test_thread_hook_ignores_system_exit():
    original = threading.excepthook
    try:
        root = MagicMock()
        logger = MagicMock()
        _install_exception_hooks(root, logger)

        args = MagicMock()
        args.exc_type = SystemExit
        args.exc_value = SystemExit(0)
        args.exc_traceback = None
        args.thread = MagicMock()

        threading.excepthook(args)

        # Intentional shutdown — must not be logged as an error.
        logger.error.assert_not_called()
    finally:
        threading.excepthook = original
