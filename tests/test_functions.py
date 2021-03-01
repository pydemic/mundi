import mundi
from mundi import Region, RegionSet
from pandas.testing import assert_frame_equal
import pytest


class TestMundiFunctions:
    @pytest.fixture
    def country(self):
        return mundi.region("br")

    @pytest.fixture
    def countries(self):
        return mundi.countries_dataframe(["br", "it", "cn"])

    def test_country_id(self):
        assert mundi.country_id("BR") == "BR"
        assert mundi.country_id("br") == "BR"
        assert mundi.country_id("BRA") == "BR"
        assert mundi.country_id("bra") == "BR"
        assert mundi.country_id("076") == "BR"
        assert mundi.country_id("Brazil") == "BR"
        assert mundi.country_id("brazil") == "BR"

    def test_code_function(self):
        assert mundi.code("Brazil") == "BR"
        assert mundi.code("brazil") == "BR"
        assert mundi.code("BR") == "BR"
        assert mundi.code("br") == "BR"
        assert mundi.code("BR-DF") == "BR-DF"
        assert mundi.code("BR/Distrito Federal") == "BR-DF"
        assert mundi.code("Brazil/Distrito Federal") == "BR-DF"
        assert mundi.code("brazil/distrito federal") == "BR-DF"
        assert mundi.code("Brazil/Brasília") == "BR-5300108"

    def test_load_country(self):
        br = mundi.region("BR")
        assert isinstance(br, Region)
        assert br.id == "BR"
        assert br["name"] == "Brazil"
        assert br["type"] == "country"
        assert br["short_code"] == "BR"
        assert br["numeric_code"] == "076"
        assert br["long_code"] == "BRA"
        assert br["country_id"] is None
        assert br["parent_id"] == "XSA"

        ar = mundi.region("ar")
        assert isinstance(ar, Region)
        assert ar["name"] == "Argentina"
        assert ar["type"] == "country"
        assert ar["short_code"] == "AR"
        assert ar["numeric_code"] == "032"
        assert ar["long_code"] == "ARG"
        assert ar["country_id"] is None
        assert br["parent_id"] == "XSA"

    def test_load_countries(self):
        db = mundi.countries()
        assert len(db) == 255

    def test_load_countries_is_an_alias_to_regions(self):
        db1 = mundi.regions_dataframe(type="country")
        db2 = mundi.countries_dataframe()
        assert_frame_equal(db1, db2)

        db1 = mundi.regions(type="country")
        db2 = mundi.countries()
        assert db1 == db2

    def test_load_states_from_regions(self):
        db = mundi.regions_dataframe(type="state", country_id="BR")
        assert len(db) == 27


class TestBR:
    def test_fn_is_not_a_state(self):
        with pytest.raises(LookupError):
            mundi.region("BR-FN")

    def test_df_is_a_state(self):
        with pytest.raises(LookupError):
            mundi.region("BR-FN")
