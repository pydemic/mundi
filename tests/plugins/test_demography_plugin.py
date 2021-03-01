import pytest

import mundi


class TestDemographyPlugin:
    @pytest.fixture
    def br(self):
        return mundi.region("BR")

    @pytest.fixture
    def region(self, br):
        return br

    @pytest.fixture
    def regions(self):
        return mundi.regions_dataframe(name="Brazil", type="country")

    def test_region_age_distribution_compatible_with_population(self, region):
        print(region.age_distribution)
        assert region.age_distribution.sum() == region.population

    def test_regions_age_distribution_compatible_with_population(self, regions):
        print(regions.mundi["age_distribution"])
        assert (
            regions.mundi["age_distribution"].sum(axis=1) == regions.mundi["population"]
        ).all()
