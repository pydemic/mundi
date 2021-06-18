import datetime
import os
from collections import Counter
from pathlib import Path
from typing import Union, Dict, Any, Iterable, List, TypeVar
from sidekick.functions import curry
from sidekick.seq import dedupe
import pandas as pd

EXT_KINDS = {".pkl.gz": "pickle", ".pkl": "pickle", ".csv": "csv", ".csv.gz": "csv"}


#
# Bug fixes
#

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


#
# IO operations
#
def read_file(path: Path, kind=None, **kwargs):
    """
    Read file from path.
    """
    if kind is None:
        method = reader_from_filename(path)
    else:
        method = getattr(pd, f"read_{kind}")
    data = method(path, **kwargs)
    if isinstance(data, dict):
        return {k: fix_string_columns_bug(v) for k, v in data.items()}
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


#
# Validation
#
def check_unique_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assert that dataframe has a unique index and return an error showing
    repetitions in case it doesn't.
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


@curry(2)
def check_column_types(
    types: Dict[str, Any], table: pd.DataFrame, *, name="<unknown>"
) -> pd.DataFrame:
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
            col_data = table[col]
            if table.columns.nlevels > 1:
                try:
                    col_data = table[col, ""]
                except KeyError:
                    pass

        except KeyError:
            raise ValueError(
                f"for dataframe {name!r}:\n{table.dtypes}\n"
                f"missing column: {col!r} (dtype = {expected})"
            )

        if isinstance(col_data, pd.Series):
            dtype = col_data.dtype
        elif isinstance(col_data, pd.DataFrame):
            dtypes = set(col_data.dtypes.values)
            if len(dtypes) == 0:
                dtype = expected
            elif len(dtypes) > 1:
                raise ValueError(
                    f"for dataframe {name!r}:\n{table.dtypes}\n"
                    f"sub-dataframe {col} is not of a uniform type. Got {dtypes}"
                )
            else:
                (dtype,) = dtypes
        else:
            raise TypeError(
                f"for dataframe {name!r}:\n{table.dtypes}\n"
                f"invalid type for column: {type(col_data)}"
            )

        # Handle string columns. This seems to be a bug in pandas that do
        # not recognize pd.StringDtype as a normal dtype.
        expected_types = expected if isinstance(expected, set) else {expected}
        try:
            is_different = all(dtype != e for e in expected_types)
        except TypeError:
            is_different = True
        if is_different:
            raise ValueError(
                f"for dataframe {name!r}:\n{table.dtypes}\n"
                f"invalid type for column {col!r}: expect {expected}, got {dtype}"
            )

    if table.columns.nlevels == 1:
        extra = set(table.columns)
    else:
        extra = set(c[0] for c in table.columns)
    extra -= set(types)

    if extra:
        raise ValueError(
            f"for dataframe {name!r}:\n{table.dtypes}\n" f"invalid columns: {extra}"
        )

    return table[list(types)]


#
# Dataframe transformations
#
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

    df = pd.concat(frames)
    df.index.name = frames[0].index.name
    return df


#
# Data frame and series conversions
#
def dataframe_to_bytes(df: pd.DataFrame, **kwargs) -> pd.Series:
    """
    Convert dataframe to series with a single column that stores data as bytes.
    """
    rows = [row.tobytes() for row in df.astype("Int32").values]
    return pd.Series(rows, index=df.index, **kwargs)


def dataframe_from_bytes(series: pd.Series, **kwargs) -> pd.DataFrame:
    """
    Create a dataframe from a series of bytes rows.

    This inverts :func:`dataframe_from_bytes`.
    """
    raise NotImplementedError


def row_from_bytes(data: bytes, **kwargs) -> pd.Series:
    """
    Return a single series object from bytes.

    This inverts a single row of :func:`dataframe_from_bytes`.
    """
    raise NotImplementedError


#
# Indexes
#
def to_index_level(
    index: pd.Index, level: int, reverse: bool = False, fill: Any = "", names=None
) -> pd.Index:
    """
    Create a multi index with the minimum given level filling tuple with 'fill'
    value.

    Args:
        index:
            Sequence or index.
        level:
            Minimum level or resulting index.
        reverse:
            If True, include fill elements to the beginning of each element.
        fill:
            Value used to fill empty elements of index
        names:
            List of names passed to the multi index constructor.
    """
    if index.nlevels >= level:
        if names is None:
            return index
        return pd.MultiIndex.from_tuples(index, names=names)
    elif isinstance(index, pd.MultiIndex):
        data = index.to_list()
    else:
        data = [(x,) for x in index]

    extra = (fill,) * (level - index.nlevels)
    if reverse:
        data = [(*extra, *x) for x in data]
    else:
        data = [(*x, *extra) for x in data]
    return pd.MultiIndex.from_tuples(data, names=names)


def with_index_level(
    data: pd.DataFrame,
    level: int,
    reverse: bool = False,
    fill: Any = "",
    names=None,
    axis=0,
) -> pd.DataFrame:
    """
    Similar to :func:`to_index_level`, but works on dataframes indexes (axis=0)
    or columns (axis=1).
    """
    if axis == 0:
        data = data.copy(deep=False)
        data.index = to_index_level(data.index, level, reverse, fill, names)
    elif axis == 1:
        data = data.copy(deep=False)
        data.columns = to_index_level(data.columns, level, reverse, fill, names)
    else:
        raise ValueError(f"invalid axis: {axis}")
    return data


def index_roots(index: pd.Index) -> list:
    """
    Return root elements of index removing duplicates.
    """
    if index.nlevels == 1:
        return [*index]
    return [*dedupe(index.get_level_values(0))]


def add_index_level(data, level, **kwargs):
    if isinstance(data, pd.Index):
        n = data.nlevels
        return to_index_level(data, n + 1, fill=level, reverse=True, **kwargs)
    elif isinstance(data, (pd.DataFrame, pd.Series)):
        axis = kwargs.get("axis", 0)
        n = data.index.nlevels if axis == 0 else data.columns.nlevels
        return with_index_level(data, n + 1, fill=level, reverse=True, **kwargs)
    else:
        raise TypeError


#
# Dates
#
def today(n=0) -> datetime.date:
    """
    Return the date today.
    """
    date = now().date()
    if n:
        return date + datetime.timedelta(days=n)
    return date


def now() -> datetime.datetime:
    """
    Return a datetime timestamp.
    """
    return datetime.datetime.now()


#
# Plugin system
#
T = TypeVar("T", str, Path)


def sort_region_names(lst: Iterable[T]) -> List[T]:
    """
    Sort a list of regions, prioritizing the world region XX.
    """

    def key(r):
        r = str(r)
        return len(r), not r.startswith("XX"), r.upper()

    return sorted(lst, key=key)


def sort_plugin_names(lst: Iterable[str]) -> List[str]:
    """
    Sort a list of plugins, prioritizing the main plugin.
    """

    def key(r):
        return r != "main", r.upper()

    return sorted(lst, key=key)


if __name__ == "__main__":
    import click

    @click.command()
    @click.argument("path", type=click.Path())
    def main(path):
        df = read_file(path)
        print(df)

    main()
