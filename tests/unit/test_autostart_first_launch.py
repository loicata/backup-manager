"""Tests for the first-launch autostart gating helper.

The app is expected to enable Windows autostart on the very first launch
from a packaged binary (PyInstaller or Nuitka) unless the user has
already configured it. Previously the gate only checked ``sys.frozen``,
which missed Nuitka builds (Nuitka exposes ``__compiled__`` instead).
"""

from __future__ import annotations

import sys

import src.__main__ as main_module
from src.__main__ import _should_auto_enable_autostart


def test_returns_true_when_sys_frozen(monkeypatch):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(main_module, "_is_nuitka", lambda: False)

    assert _should_auto_enable_autostart() is True


def test_returns_true_when_nuitka(monkeypatch):
    monkeypatch.delattr(sys, "frozen", raising=False)
    monkeypatch.setattr(main_module, "_is_nuitka", lambda: True)

    assert _should_auto_enable_autostart() is True


def test_returns_true_when_both(monkeypatch):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(main_module, "_is_nuitka", lambda: True)

    assert _should_auto_enable_autostart() is True


def test_returns_false_in_dev_mode(monkeypatch):
    monkeypatch.delattr(sys, "frozen", raising=False)
    monkeypatch.setattr(main_module, "_is_nuitka", lambda: False)

    assert _should_auto_enable_autostart() is False


def test_returns_false_when_frozen_attr_is_false(monkeypatch):
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(main_module, "_is_nuitka", lambda: False)

    assert _should_auto_enable_autostart() is False
