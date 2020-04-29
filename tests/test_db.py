import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import mundi
import mundi.functions


class TestDb:
    @pytest.fixture
    def country(self):
        return mundi.region("br")

    @pytest.fixture
    def countries(self):
        return mundi.functions.countries(["br", "it", "cn"])

    def test_load_countries(self):
        db = mundi.functions.countries()
        assert (db["type"] == "country").all()
        assert len(db) == 255

    def test_load_countries_is_an_alias_to_regions(self):
        db1 = mundi.functions.regions(type="country")
        db2 = mundi.functions.countries()
        assert_frame_equal(db1, db2)

    def test_load_states_from_regions(self):
        db = mundi.functions.regions(type="state", country="BR")
        assert len(db) == 27

    def test_load_country(self):
        br = mundi.region("br")
        assert isinstance(br, pd.Series)
        assert len(br.index) == 7
        assert br["name"] == "Brazil"
        assert br["type"] == "country"
        assert br["code"] == "BR"
        assert br["numeric_code"] == "076"
        assert br["long_code"] == "BRA"
        assert br["country_code"] is pd.NA
        assert br["parent_id"] is pd.NA


class TestBR:
    def test_fn_is_not_a_state(self):
        with pytest.raises(LookupError):
            mundi.region("BR-FN")

    def test_df_is_a_state(self):
        with pytest.raises(LookupError):
            mundi.region("BR-FN")
