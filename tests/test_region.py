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
            Region("BR-530010805"),
            Region("BR-SUS:5301"),
        }

        regions = br.children(deep=False)
        assert set(regions) == {Region("BR-5301"), Region("BR-SUS:5301")}

    def test_parents(self):
        brb = Region("BR-5300108")
        assert brb.parent.id == "BR-530101"
        assert brb.parent.parent.id == "BR-5301"
        assert brb.parent.parent.parent.id == "BR-DF"
        assert brb.parent.parent.parent.parent.id == "BR-5"
        assert brb.parent.parent.parent.parent.parent.id == "BR"
        assert brb.parent.parent.parent.parent.parent.parent.id == "XSA"
        assert brb.parent.parent.parent.parent.parent.parent.parent.id == "XX"

    def test_region_children_in_multiple_hierarchies(self):
        europe = Region("XEU")
        asia = Region("XAS")
        russia = Region("RU")

        assert russia in asia.children()
        assert russia not in asia.children(which="primary")

        assert russia in europe.children()
        assert russia in europe.children(which="primary")
