import os
from pathlib import Path
from typing import Union

import pandas as pd

EXT_KINDS = {"pkl.gz": "pickle"}


# TODO: is it a bug? report it? check which versions are affected by it
def fix_string_columns_bug(df):
    """
    It seems that pandas do not load pickled dataframes with string columns
    with pd.NA values.

    It seems to work in small dataframes, but not large(ish) ones.
    """

    assert len(df) == len(set(df.index))

    if not hasattr(df.dtypes, "items"):
        return df

    columns = list(df.columns)

    for col_name, dtype in df.dtypes.items():
        if isinstance(dtype, pd.StringDtype):
            col = df.pop(col_name).astype(str)
            col = col[col != "<NA>"]
            col = col[~col.isna()]
            col = col.astype("string")
            df[col_name] = col

    return df[columns]


def read_file(path: Path, kind=None, **kwargs):
    """
    Read file from path.
    """
    if kind is None:
        method = reader_from_filename(path)
    else:
        method = getattr(pd, f"read_{kind}")
    data = method(path, **kwargs)
    return fix_string_columns_bug(data)


def reader_from_filename(path: Union[str, Path]):
    """
    Return file kind from filename.
    """
    _, ext = os.path.splitext(path)
    kind = EXT_KINDS[ext]
    return getattr(pd, f"read_{kind}")
