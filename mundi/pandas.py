from functools import partial
from itertools import chain
from operator import attrgetter
from typing import Union, Sequence

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
        frames = extend_columns(df, cols, init=[df])
        idx_dims = max(level_dims(f.columns) for f in frames)
        data = pd.concat(frames, axis=1)
        if idx_dims > 1:
            cols = (fill_idx(x, idx_dims) for x in data.columns)
            data.columns = pd.MultiIndex.from_tuples(cols)
        return data

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


def extend_columns(data: pd.DataFrame, cols: Sequence[str], init=None, repeat=False):
    """
    Extend columns for given dataframe.
    Args:
        data:
            Input dataframe.
        cols:
            List of columns names to extend.
        init:
            Initial list of frames
        repeat:
            If true, repeat columns that are already present in dataframe or the
            init list.
    """
    frames = list(init or ())
    used_columns = set(chain(data.columns, *map(attrgetter("columns"), frames)))

    for col in cols:
        if col not in used_columns or repeat:
            extra = extend(data, col)
            if not isinstance(extra, pd.DataFrame):
                extra = pd.DataFrame({col: extra})
            elif extra.shape[1] == 1 and extra.columns == [col]:
                pass
            else:
                extra.columns = add_multi_index_level(col, extra.columns)
            frames.append(extra)
    return frames


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
