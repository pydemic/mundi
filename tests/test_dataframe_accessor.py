import pytest

import mundi


class TestDataFrameAccessor:
    @pytest.fixture(scope="class")
    def db(self):
        db = mundi.countries()
        return db.iloc[:10]

    def test_extract_extra_columns(self, db):
        extra = db.mundi["income_group"]
        assert extra.shape == (10, 1)

    def test_extract_and_append_extra_columns(self, db):
        extra = db.mundi[..., "income_group"]
        assert extra.shape == (10, 2)

    def test_extract_multiple_columns(self, db):
        extra = db.mundi["region", "income_group"]
        assert extra.shape == (10, 2)
