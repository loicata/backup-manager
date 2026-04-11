"""Tests for PROVIDER_REGIONS consistency with PROVIDER_ENDPOINTS."""

from src.storage.s3 import PROVIDER_ENDPOINTS, PROVIDER_REGIONS


class TestProviderRegions:
    """Ensure PROVIDER_REGIONS is consistent with PROVIDER_ENDPOINTS."""

    def test_all_endpoints_have_regions(self):
        """Every provider in PROVIDER_ENDPOINTS must have a PROVIDER_REGIONS entry."""
        for provider in PROVIDER_ENDPOINTS:
            assert provider in PROVIDER_REGIONS, f"Missing PROVIDER_REGIONS for {provider}"

    def test_all_regions_have_endpoints(self):
        """Every provider in PROVIDER_REGIONS must have a PROVIDER_ENDPOINTS entry."""
        for provider in PROVIDER_REGIONS:
            assert provider in PROVIDER_ENDPOINTS, f"Missing PROVIDER_ENDPOINTS for {provider}"

    def test_regions_are_lists(self):
        """All region values must be lists."""
        for provider, regions in PROVIDER_REGIONS.items():
            assert isinstance(regions, list), f"PROVIDER_REGIONS[{provider}] is not a list"

    def test_non_empty_regions_for_templated_endpoints(self):
        """Providers with {region} in their endpoint must have at least one region."""
        for provider, endpoint in PROVIDER_ENDPOINTS.items():
            if endpoint and "{region}" in endpoint:
                regions = PROVIDER_REGIONS.get(provider, [])
                assert (
                    len(regions) > 0
                ), f"Provider {provider} has {{region}} in endpoint but no regions defined"

    def test_scaleway_regions(self):
        """Scaleway must have fr-par, nl-ams, pl-waw."""
        regions = PROVIDER_REGIONS["scaleway"]
        assert "fr-par" in regions
        assert "nl-ams" in regions
        assert "pl-waw" in regions

    def test_cloudflare_region_is_auto(self):
        """Cloudflare R2 uses auto region (no region selection)."""
        regions = PROVIDER_REGIONS["cloudflare"]
        assert regions == ["auto"]

    def test_aws_default_region_is_first(self):
        """AWS default region should be first in the list."""
        regions = PROVIDER_REGIONS["Amazon AWS"]
        assert len(regions) > 0
        assert regions[0] == "eu-west-1"

    def test_digitalocean_regions(self):
        """DigitalOcean Spaces must have typical regions."""
        regions = PROVIDER_REGIONS["digitalocean"]
        assert "nyc3" in regions
        assert "ams3" in regions

    def test_backblaze_regions(self):
        """Backblaze B2 must have us-west and eu-central regions."""
        regions = PROVIDER_REGIONS["backblaze_s3"]
        assert any("us-west" in r for r in regions)
        assert any("eu-central" in r for r in regions)
