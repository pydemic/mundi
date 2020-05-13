import pytest

from mundi import Region


class TestRegion:
    def test_regions_are_unique(self):
        assert id(Region("BR")) == id(Region("BR"))

    def test_region_basic_usage(self):
        br = Region("BR")
        assert br.id == "BR"
        assert hash(br)
        assert {br} == {br}

        with pytest.raises(AttributeError):
            br.name = "Brazil"

    def test_region_dynamic_attribute_fetch(self):
        br = Region("BR")
        assert br.name == "Brazil"
        assert br.short_code == "BR"
        assert br.long_code == "BRA"

    def test_region_fetch_children(self):
        br = Region("BR")
        regions = br.children()
        assert Region("BR-1") in regions

    def test_region_fetch_children_deep(self):
        br = Region("BR-DF")
        regions = br.children(deep=True)
        assert set(regions) == {
            Region("BR-5301"),
            Region("BR-530101"),
            Region("BR-5300108"),
            Region("BR-SUS:53001"),
        }

        regions = br.children(deep=False)
        assert set(regions) == {Region("BR-5301"), Region("BR-SUS:53001")}

    def test_region_children_in_multiple_hierarchies(self):
        europe = Region("XEU")
        asia = Region("XAS")
        russia = Region("RU")

        assert russia in asia.children()
        assert russia not in asia.children(only_primary=True)

        assert russia in europe.children()
        assert russia in europe.children(only_primary=True)