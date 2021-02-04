from functools import partial
from itertools import chain
from operator import attrgetter
from typing import Union, Sequence

import pandas as pd

from .. import db

Pandas = Union[pd.Series, pd.DataFrame]


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
                return pd.concat([self._data, self[right]], axis=1)
            elif right is ...:
                return pd.concat([self[left], self._data], axis=1)
            else:
                raise IndexError(f"invalid index: {key}")

        index = self._data.index
        if isinstance(key, list):
            values = db.values_for(index, *key, null=pd.NA)
            table = pd.DataFrame(list(values), columns=key, index=index)
        else:
            return self[[key]][key]
        return table.reindex(index)

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
