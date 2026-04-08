"""Tests for GeneralTab bandwidth percentage radio buttons.

Verifies that the UI correctly loads, displays, and collects
the bandwidth_percent setting (25, 50, 75, 100).
"""

import pytest

from src.core.config import BackupProfile
from src.ui.tabs.general_tab import GeneralTab


@pytest.fixture()
def general_tab(tk_root):
    """Create a fresh GeneralTab for each test."""
    tab = GeneralTab(tk_root)
    yield tab
    tab.destroy()


class TestBandwidthPercentUI:
    def test_default_value_is_75(self, general_tab):
        assert general_tab.bw_percent_var.get() == 75

    def test_load_profile_sets_percent(self, general_tab):
        profile = BackupProfile()
        profile.bandwidth_percent = 50
        general_tab.load_profile(profile)
        assert general_tab.bw_percent_var.get() == 50

    def test_collect_config_returns_percent(self, general_tab):
        general_tab.bw_percent_var.set(75)
        config = general_tab.collect_config()
        assert config["bandwidth_percent"] == 75

    def test_all_values_settable(self, general_tab):
        for pct in (25, 50, 75, 100):
            general_tab.bw_percent_var.set(pct)
            config = general_tab.collect_config()
            assert config["bandwidth_percent"] == pct

    def test_no_bandwidth_limit_kbps_in_config(self, general_tab):
        config = general_tab.collect_config()
        assert "bandwidth_limit_kbps" not in config
