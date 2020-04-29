from typing import Union

import pandas as pd
import sidekick as sk
from sidekick import X

from .loader import DATA_LOADERS

Pandas = Union[pd.Series, pd.DataFrame]


@pd.api.extensions.register_dataframe_accessor("mundi")
class MundiDataFrameAcessor:
    def __new__(cls, obj):
        if "mundi" in obj:
            return obj["mundi"]
        return super().__new__(cls)

    def __init__(self, obj):
        self._data = obj

    def __getitem__(self, cols):
        if not isinstance(cols, tuple):
            cols = (cols,)

        left, right = map(list, sk.partition_at((X == ...), cols))
        frames = [self.extra(left)] if left else []
        if right:
            _, *right = right  # remove ellipsis
            frames.append(self.extend(right))
        return pd.concat(frames, axis=1)

    def select(self, **kwargs):
        """
        Select interface of mundi.
        """
        df = self._data
        (k, v), *rest = kwargs.items()
        m = mask(df, k, v)
        for k, v in rest:
            m &= mask(df, k, v)
        return df[m.fillna(False)]

    def extend(self, cols):
        """
        Extend dataframe with the given columns.
        """
        df = self._data.copy()
        for col in cols:
            if col not in df:
                extra = extend(df, col)
                df[extra.columns] = extra
        return df

    def extra(self, cols):
        """
        Extend dataframe with the given columns.
        """
        data = [extend(self._data, col) for col in cols]
        return pd.concat(data, axis=1)


def mask(data: Pandas, col: str, value) -> pd.Series:
    """
    Return a boolean mask with values in which df[col] == value
    """
    if col not in data:
        data = extend(data, col)
    return data[col].__eq__(value)


def extend(data: Pandas, name: str) -> Pandas:
    """
    Load additional data from the given data source.
    """
    try:
        fn = DATA_LOADERS[name]
    except KeyError:
        msg = f"Mundi does not know how to load {name!r} columns."
        raise ValueError(msg)
    return fn(data)
