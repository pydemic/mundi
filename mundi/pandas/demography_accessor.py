from datetime import datetime

import pandas as pd


@pd.api.extensions.register_dataframe_accessor("demography")
class DemographyDataFrameAccessor:
    def __new__(cls, obj):
        if "demography" in obj:
            return obj["demography"]
        return super().__new__(cls)

    def __init__(self, obj):
        self._data = obj

    def now(self, year_col="year"):
        """
        Filter dataframe to the current year if it has an year column.
        """
        year = datetime.now().year
        df = self._data
        if year_col not in self._data:
            return df
        df = df[df[year_col] == year]
        return df.drop(columns=year_col)

    def latest(self, year_col="year", id_col="id"):
        """
        Filter dataframe to the latest year if it has an year column.
        """
        df = self._data
        if year_col not in self._data:
            return df
        df = df.groupby(id_col).max(year_col)
        return df.drop(columns=year_col)

    def merge_pyramid(self, col="gender", values=None, index=None):
        """
        Merge all categories of a age pyramid distribution stratified by age and
        "col", and return a simple age distribution.
        """
        df, *rest = self._unpivot_pyramid_worker(col, values, index).values()
        for part in rest:
            df += part
        return df

    def unpivot_pyramid(self, col="gender", values=None, index=None):
        """
        Takes a pivot table and
        """
        data = self._unpivot_pyramid_worker(col, values, index)
        return pd.concat(
            [*data.values()], axis=1, keys=[*data.keys()], names=[col, "age"]
        )

    def _unpivot_pyramid_worker(self, col, values, index) -> dict:
        df = self._data
        if values is None:
            values = sorted(set(df[col]))

        if index is None:
            index = non_age_columns(df.columns)
            del index[index.index(col)]

        return {v: df[df[col] == v].drop(columns=col).set_index(index) for v in values}

    def encode_demography(self, cols=None):
        """
        Convert columns to
        """
        ...

    def decode_demography(self, col):
        """
        Convert columns to
        """
        ...


def is_age_column(col):
    """
    Return True if a column name represents an age stratification.
    """
    return isinstance(col, int) or isinstance(col, str) and col.isdigit()


def age_columns(columns):
    """
    Filter columns to show only valid age strata.
    """
    return [col for col in columns if is_age_column(col)]


def non_age_columns(columns):
    """
    Filter columns to show only invalid age strata.
    """
    return [col for col in columns if not is_age_column(col)]


def to_bytes(data, name="bytes"):
    """
    Covert data to a single column of np.ndarray represented as binary
    blobs.
    """
    if data is pd.NA:
        return pd.NA
    rows = [row.tobytes() for row in data.values]
    df = pd.Series(rows, index=data.index)
    df.name = name
    return df
