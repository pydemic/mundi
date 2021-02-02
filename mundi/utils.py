import os
from collections import Counter
from pathlib import Path
from typing import Union, Dict, Any, Iterable, List

import pandas as pd
from sidekick import api as sk

EXT_KINDS = {".pkl.gz": "pickle", ".pkl": "pickle", ".csv": "csv", ".csv.gz": "csv"}


# TODO: is it a bug? report it? check which versions are affected by it
def fix_string_columns_bug(df):
    """
    It seems that pandas do not load pickled dataframes with string columns
    with pd.NA values.

    It seems to work in small dataframes, but not large(ish) ones.
    """

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
    base, ext = os.path.splitext(path)
    try:
        kind = EXT_KINDS[ext]
    except KeyError:
        if ext in (".gz", ".bz2"):
            base, ext_extra = os.path.splitext(base)
            if not base:
                raise
            kind = EXT_KINDS[ext_extra + ext]
        else:
            raise
    return getattr(pd, f"read_{kind}")


def assign(data: pd.DataFrame, **values) -> pd.DataFrame:
    """
    Assign values to all missing columns specified as keyword arguments.

    Similar to a DataFrame's assign method, but do not overwrite existing
    columns.
    """

    def normalize(x):
        if x is pd.NA or isinstance(x, str):
            return x, "string"
        return x if isinstance(x, tuple) else (x, None)

    values = {k: normalize(x) for k, x in values.items()}
    for col in data.keys():
        if col in values:
            del values[col]

    constants = {k: v[0] for k, v in values.items()}
    types = {k: v[1] for k, v in values.items() if v[1] is not None}
    return data.assign(**constants).astype(types)


def check_unique_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assert that dataframe has a unique index and return an error showing
    repetitions in case it don't.
    """
    if df.index.is_unique:
        return df

    count = Counter(df.index)
    common = count.most_common(5)
    raise ValueError(f"index is not unique: {common}")


def check_no_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check there is no column of dtype=object
    """
    for col, dtype in df.dtypes.items():
        if dtype == object:
            try:
                # Try to convert to string with pd.NA missing data
                df[col] = df[col].fillna(pd.NA).astype("string")
            except Exception:
                raise ValueError(f"column {col!r} is of invalid dtype object")
    return df


@sk.curry(2)
def check_column_types(types: Dict[str, Any], table: pd.DataFrame) -> pd.DataFrame:
    """
    Check if table has all types.

    This is a curried static method, and hence can be used in a pipe by
    passing a single argument.

    Args:
        types:
            A mapping of columns to expected types.
        table:
            A data frame with the expected columns.

    Returns:
        Reorder columns according to the order in the types dict and raises
        an error if dataframe is missing some column, has extra columns or
        if some column is of the wrong type.
    """
    for col, expected in types.items():
        try:
            dtype = table[col].dtype
        except KeyError:
            raise ValueError(f"missing column: {col!r} (dtype = {expected})")

        # Handle string columns. This seems to be a bug in pandas that do
        # not recognize pd.StringDtype as a normal dtype.
        try:
            is_different = dtype != expected
        except TypeError:
            is_different = True
        if is_different:
            msg = f"invalid type for column {col!r}: expect {expected}, got {dtype}"
            raise ValueError(msg)

    extra = set(table.keys()) - set(types)
    if extra:
        raise ValueError(f"invalid columns: {extra}")

    return table[list(types)]


def safe_concat(*args):
    """
    Concatenate series of dataframes, but raises errors if columns have
    different dtypes.
    """
    if len(args) == 1:
        (frames,) = args
    else:
        frames = args

    dtypes = frames[0].dtypes.to_dict()
    columns = set(frames[0].columns)
    for i, df in enumerate(frames[1:], 2):
        # Dataframe must contain the same columns
        if columns != set(df.columns):
            missing = columns - set(df.columns)
            if missing:
                raise ValueError(f"missing columns on argument {i} : {missing}")
            extra = set(df.columns) - columns
            raise ValueError(f"unknown columns on argument {i} : {extra}")

        # Column types must also be the same.
        tdict = df.dtypes.to_dict()
        while tdict:
            col, dtype = tdict.popitem()
            try:
                is_different = dtypes[col] != dtype
            except TypeError:  # pandas does not handle pd.StringDtype correctly
                is_different = True
            if is_different:
                msg = f'column "{col}" at arg {i} should be {dtypes[col]}, got {dtype}'
                raise TypeError(msg)
    return pd.concat(frames)


def sort_region_names(lst: Iterable[str]) -> List[str]:
    """
    Sort a list of regions, prioritizing the world region XX.
    """

    def key(r):
        return len(r), not r.startswith("XX"), r.upper()

    return sorted(lst, key=key)


def sort_plugin_names(lst: Iterable[str]) -> List[str]:
    """
    Sort a list of plugins, prioritizing the main plugin.
    """

    def key(r):
        return r != "main", r.upper()

    return sorted(lst, key=key)
