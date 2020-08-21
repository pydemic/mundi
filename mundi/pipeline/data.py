import os
import sys
from abc import ABC, abstractmethod
from collections import Counter
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import sidekick.api as sk

from .._utils import read_file
from ..logging import log


class Data(ABC):
    """
    Abstract data preparation interface.

    Data subclasses have 4 main methods:
        - main (classmethod): execute CLI interface.
        - collect: return dictionary mapping table names with data.
        - validate: validate data before saving
        - save: save data to build path.
    """

    path: Path
    script_name: str
    plugin_name: str
    region_name: str

    @classmethod
    def main(cls, **kwargs):
        """
        Create instance and execute.
        """
        import click

        @click.command()
        @click.option("--show", "-s", is_flag=True, help="Show data instead of saving it")
        @click.option(
            "--validate", "-v", default=False, is_flag=True, help="Validates data"
        )
        def main(show, validate):
            data = cls(**kwargs)
            if show:
                for k, v in data.collect().items():
                    try:
                        data = data.validate(v, k) if validate else v
                    except Exception:
                        print(
                            f"Error processing {k}. We stopped with data in this "
                            f"state:"
                        )
                        print(v)
                        raise

                    print("TABLE:", k, "\n")
                    print(data, "\n\nTYPES:", data.dtypes, end="\n\n")

            else:
                data.save()

        main()

    def __init__(self, path=None):
        if path is None:
            cls = type(self)
            path, _ = os.path.splitext(sys.modules[cls.__module__].__file__)
        path = Path(path)
        self.path = path.parent
        self.script_name = path.name
        self.plugin_name = path.parent.name
        self.region_name = path.parent.parent.name

    @abstractmethod
    def collect(self) -> Dict[str, pd.DataFrame]:
        """
        Main method that collect data and return a dictionary from table names
        to their corresponding data.
        """
        raise NotImplementedError("must be implemented in sub-classes")

    def validate(self, table: pd.DataFrame, name: str = None) -> pd.DataFrame:
        """
        Validate table and prepare it to be saved.
        """

        try:
            dtypes = getattr(self, f"{name.upper()}_DATA_TYPES")
        except AttributeError:
            pass
        else:
            table = check_column_types(dtypes, table)

        return sk.pipe(table, check_unique_index, check_no_object_columns)

    def save(self):
        """
        Save data to default location.
        """

        save_path = self.path.parent.parent.parent / "build" / "chunks"
        save_path.mkdir(exist_ok=True, parents=True)
        name = "{table}-%s" % self.region_name
        template = f"{save_path.absolute()}/{name}.pkl.gz"

        for table, data in self.collect().items():
            path = template.format(table=table)
            data = self.validate(data, table)
            data.to_pickle(path)

            rows, cols = data.shape
            log.info(str(data.dtypes))
            log.info(f"[mundi.data]: {rows}x{cols} data saved to {path}")


#
# Utility methods for common dataframe manipulations
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

    return data.assign(**{k: v[0] for k, v in values.items()}).astype(
        {k: v[1] for k, v in values.items() if v[1] is not None}
    )


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
                raise ValueError(f"column {col} is of invalid dtype object")
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
    for col, dtype in types.items():
        try:
            col_type = table[col].dtype
        except KeyError:
            raise ValueError(f"missing column: {col!r} (dtype = {dtype})")
        if col_type != dtype:
            msg = f"invalid type for column {col}: expect {dtype}, got {col_type}"
            raise ValueError(msg)

    extra = set(table.keys()) - set(types)
    if extra:
        raise ValueError(f"invalid columns: {extra}")

    return table[list(types)]


#
# Mixin classes
#
class DataIOMixin:
    """
    Methods to interact with files.
    """

    def read_csv(self, path, **kwargs):
        """
        Read csv file from current path.
        """
        kwargs.setdefault("index_col", 0)
        return self.read_data(path, kind="csv", **kwargs)

    @staticmethod
    def read_data(path, **kwargs):
        """
        Read file from current path.
        """
        return read_file(path, **kwargs)


class DataTransformMixin:
    """
    Methods to perform data transformations.
    """

    assign = staticmethod(assign)


class DataValidationMixin:
    """
    Expose validation functions as methods, if necessary.
    """

    check_column_types = staticmethod(check_column_types)
    check_unique_index = staticmethod(check_unique_index)
    check_no_object_columns = staticmethod(check_no_object_columns)
