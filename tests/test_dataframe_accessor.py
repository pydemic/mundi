import pandas as pd
import pytest

import mundi


@pytest.fixture(scope="class")
def db():
    db = mundi.countries_dataframe()
    return db.iloc[:10]


class TestDataFrameAccessor:
    def test_extract_extra_columns(self, db):
        extra = db.mundi["region"]
        assert isinstance(extra, pd.Series)
        assert extra.shape == (10,)

    def test_extract_multiple_columns(self, db):
        extra = db.mundi[["type", "subtype", "region"]]
        assert extra.shape == (10, 3)

    def test_extract_and_append_extra_columns(self, db):
        extra = db.mundi[..., ["region"]]
        assert extra.shape == (10, 2)


class TestDataFrameAccessorPlugin:
    def test_extract_multiple_columns_with_plugins(self, db):
        extra = db.mundi[["population", "region"]]
        assert extra.shape == (10, 3)
