from functools import partial
from typing import Union

import pandas as pd

from .. import db

Pandas = Union[pd.Series, pd.DataFrame]
REGION_UNIVERSE = db.Universe.REGION


@pd.api.extensions.register_dataframe_accessor("mundi")
class MundiDataFrameAccessor:
    def __new__(cls, obj):
        if "mundi" in obj:
            return obj["mundi"]
        return super().__new__(cls)

    def __init__(self, obj):
        self._data = obj

    def __getitem__(self, key):
        if isinstance(key, tuple):
            left, right = key
            if left is ...:
                other = self[right if isinstance(right, list) else [right]]
                return pd.concat([self._data, other], axis=1)
            elif right is ...:
                other = self[left if isinstance(left, list) else [left]]
                return pd.concat([other, self._data], axis=1)
            else:
                raise IndexError(f"invalid index: {key}")

        if isinstance(key, list):
            return self._get_columns(key)
        else:
            return self._get_column(key)

    def _get_column(self, column, force_dataframe=False) -> Pandas:
        """
        Retrieve column as series or dataframe.
        """
        try:
            col = REGION_UNIVERSE.column(column)
        except ValueError:
            raise KeyError(column)

        pks = self._data.index
        out = col.select(pks)
        if force_dataframe and not isinstance(out, pd.DataFrame):
            return pd.DataFrame({column: out})
        return out

    def _get_columns(self, columns) -> pd.DataFrame:
        """
        Retrieve columns as dataframe.
        """
        cols = [self._get_column(col, force_dataframe=True) for col in columns]
        return pd.concat(cols, axis=1)

    def filter(self, **kwargs):
        """
        Select interface of mundi.
        """
        df = self._data
        (k, v), *rest = kwargs.items()
        m = mask(df, k, v)
        for k, v in rest:
            m &= mask(df, k, v)
        return df[m.fillna(False)]


def mask(data: Pandas, col: str, value) -> pd.Series:
    """
    Return a boolean mask with values in which df[col] == value
    """
    if col not in data.columns:
        data = data.mundi[[col]]
    return data[col].__eq__(value)


def level_dims(idx):
    try:
        shape = idx.levshape
    except AttributeError:
        return 1
    else:
        return len(shape)


def fill_idx(x, level: int) -> tuple:
    if isinstance(x, tuple):
        n = len(x)
        if n == level:
            return x
        else:
            rest = ("",) * (level - n)
            return (*x, *rest)
    else:
        rest = ("",) * (level - 1)
        return (x, *rest)


def prepend_to_tuple(level, x):
    return (level, *x) if isinstance(x, tuple) else (level, x)


def prepend_to_tuples(level, seq):
    return map(partial(prepend_to_tuple, level), seq)


def add_multi_index_level(level, idx):
    return pd.MultiIndex.from_tuples(prepend_to_tuples(level, idx))
