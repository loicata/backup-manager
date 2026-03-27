"""Backup Manager v3 — Backup management system."""

import importlib.metadata as _meta

try:
    __version__ = _meta.metadata("backup-manager")["Version"]
except _meta.PackageNotFoundError:
    __version__ = "3.1.3"  # Fallback for dev/PyInstaller builds

__author__ = "Loic Ader"
