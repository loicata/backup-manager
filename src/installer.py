"""Feature detection for optional dependencies.

Checks if optional packages (paramiko, boto3, pyotp) are installed
and provides graceful degradation when they're missing.
"""

import importlib
import logging

logger = logging.getLogger(__name__)

FEAT_ENCRYPTION = "encryption"
FEAT_S3 = "s3"
FEAT_SFTP = "sftp"
FEAT_PROTON = "proton"

_FEATURE_DEPS = {
    FEAT_ENCRYPTION: ["cryptography"],
    FEAT_S3: ["boto3", "botocore"],
    FEAT_SFTP: ["paramiko"],
    FEAT_PROTON: [],  # rclone is external, checked at runtime
}


def check_module(name: str) -> bool:
    """Check if a Python module is importable."""
    try:
        importlib.import_module(name)
        return True
    except ImportError:
        return False


def check_all() -> dict[str, dict]:
    """Check all optional features.

    Returns:
        Dict mapping feature name to {available: bool, missing: list[str]}.
    """
    results = {}
    for feat, deps in _FEATURE_DEPS.items():
        missing = [d for d in deps if not check_module(d)]
        results[feat] = {
            "available": len(missing) == 0,
            "missing": missing,
        }
    return results


def get_available_features() -> set[str]:
    """Get set of available feature IDs."""
    return {feat for feat, info in check_all().items() if info["available"]}


def get_unavailable_features_detail() -> dict[str, list[str]]:
    """Get details about unavailable features.

    Returns:
        Dict mapping feature name to list of missing dependencies.
    """
    return {feat: info["missing"] for feat, info in check_all().items() if not info["available"]}
