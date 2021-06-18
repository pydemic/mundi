import contextlib
import gzip
import io
import os
import pickle
import sys
import time
from abc import ABC
from pathlib import Path
from typing import Dict, Iterator, List

import pandas as pd
from sidekick.functions import pipe

from .. import config
from ..logging import log
from ..utils import (
    read_file,
    assign,
    check_unique_index,
    check_no_object_columns,
    check_column_types,
    safe_concat,
    sort_region_names,
    sort_plugin_names,
)


class Data(ABC):
    """
    Abstract data preparation interface.

    The prepare.py scripts should read some data source and save the
    post-processed data into the appropriate file under build/chunks/. This
    process can be implemented manually, but this class helps with repetitive
    tasks of data validation, imputation and finding the correct paths so save
    and load files.

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
    table_dtypes: Dict[str, dict]
    _as_script = True
    _path = None

    @classmethod
    def cli(cls, **kwargs):
        """
        Create instance and execute.
        """

        def main(show=False, validate=False, table=None):
            data = cls(**kwargs)
            if show:
                if table:
                    values = {table: data.collect_table(table)}
                else:
                    values = data.collect()
                if table is not None:
                    values = {table: values[table]}
                for k, v in values.items():
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
                    with pd.option_context(
                        "display.max_rows", 200, "display.max_columns", None
                    ):
                        print(data)

                    print("\nTYPES:", k, "\n")
                    with pd.option_context(
                        "display.max_rows", None, "display.max_columns", None
                    ):
                        if isinstance(data, dict):
                            for key, table in data.items():
                                print(f"\nKEY: {key}")
                                print(table.dtypes)
                            print()
                        else:
                            print(data.dtypes, end="\n\n")

            else:
                data.save()

        if cls._as_script:
            import click

            @click.command()
            @click.option(
                "--show", "-s", is_flag=True, help="Show data instead of saving it"
            )
            @click.option(
                "--validate", "-v", default=False, is_flag=True, help="Validates data."
            )
            @click.option("--table", "-t", help="Filter to single table.")
            def _main(**kwargs):
                return main(**kwargs)

            return _main()

        main()

    def __init__(self, path=None, table_dtypes=None):
        if path is None:
            cls = type(self)
            if cls._path is not None:
                path = cls._path
            else:
                path = Path(sys.modules[cls.__module__].__file__).parent

        path = Path(path)
        self.path = path
        self.plugin_name = path.name
        self.region_name = path.parent.name

        if table_dtypes is None:
            if not hasattr(self, "table_dtypes"):
                self.table_dtypes = {}
        else:
            self.table_dtypes = table_dtypes

    def collect(self) -> Dict[str, pd.DataFrame]:
        """
        Main method that collect data and return a dictionary from table names
        to their corresponding data.
        """
        return {k: self.collect_table(k) for k in self.collected_tables()}

    def collect_table(self, table):
        """
        Collect table from name.
        """
        return getattr(self, table)

    def collected_tables(self) -> List[str]:
        """
        Return a list of collected tables.
        """
        return list(self.table_dtypes)

    def validate(self, table: pd.DataFrame, name: str = None) -> pd.DataFrame:
        """
        Validate table and prepare it to be saved.
        """

        try:
            dtypes = self.table_dtypes[name]
        except KeyError:
            raise NotImplementedError(
                f"class does not declare an table_dtypes dictionary"
            )
        else:
            if dtypes is None:
                pass
            elif isinstance(table, dict):
                table = check_column_types_ex(dtypes, table, name=name)

        return pipe(table, check_unique_index_ex, check_no_object_columns_ex)

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
            if isinstance(data, dict):
                self._save_pickle(data, path)
                keys = list(data.keys())
                log.info(f"[mundi.data]: {keys} tables saved to {path}")
            else:
                data.to_pickle(path)
                rows, cols = data.shape
                log.info(str(data.dtypes))
                log.info(f"[mundi.data]: {rows}x{cols} data saved to {path}")

    def _save_pickle(self, data, path):
        with gzip.open(path, "wb") as fd:
            pickle.dump(data, fd)


class DataIO(Data, ABC):
    """
    A data subclass with methods for making IO.

    Normally, most plugins should inherit from this class, since it has a
    richer and more convenient interface.
    """

    def read_csv(self, path, **kwargs):
        """
        Read csv file from current path.
        """
        kwargs.setdefault("index_col", 0)
        return self.read_data(path, kind="csv", **kwargs)

    def read_data(self, path, **kwargs):
        """
        Read file from current path.
        """
        path = self._normalize_path(path)
        return read_file(path, **kwargs)

    def _normalize_path(self, path):
        if os.path.exists(path):
            return path

        full_path = os.path.abspath(path)
        if os.path.exists(full_path):
            return full_path

        mod = sys.modules[type(self).__module__]
        mod_path = os.path.dirname(mod.__file__)
        cls_path = os.path.join(mod_path, path)

        if os.path.exists(cls_path):
            return cls_path

        raise FileNotFoundError(path)

    # Data transformations
    assign = staticmethod(assign)
    safe_concat = staticmethod(safe_concat)

    # Data validation
    def check_column_types(self, types, table, *, name="<table>"):
        return check_column_types_ex(types, table, name=name)

    def check_unique_index(self, table):
        return check_unique_index_ex(table)

    def check_no_object_columns(self, table):
        return check_no_object_columns_ex(table)


def check_column_types_ex(types, table, name="<unknown>"):
    """
    Expand check_column_types to accept a dictionary of tables.
    """
    if isinstance(table, dict):
        out = {}
        for k, data in table.items():
            out[k] = check_column_types(types[k], data, name=f"{name}/{k}")
        return out
    return check_column_types(types, table, name=name)


def check_unique_index_ex(table):
    """
    Expand check_unique_index to accept a dictionary of tables.
    """
    if isinstance(table, dict):
        return {k: check_unique_index(v) for k, v in table.items()}
    return check_unique_index(table)


def check_no_object_columns_ex(table):
    """
    Expand check_no_object_columns to accept a dictionary of tables.
    """
    if isinstance(table, dict):
        return {k: check_no_object_columns(v) for k, v in table.items()}
    return check_no_object_columns(table)


def find_prepare_scripts(path: Path, verbose: bool = False) -> Iterator[Path]:
    """
    Return an iterator with (plugin, region, path) with the location of all
    prepare.py plugins under the current mundi-data path.
    """
    if verbose:
        print("scripts")
    for region in sort_region_names(os.listdir(path)):
        for plugin in sort_plugin_names(os.listdir(path / region)):
            prepare = (path / region / plugin / "prepare.py").absolute()
            if os.path.exists(prepare):
                yield plugin, region, prepare


def execute_prepare_script(
    path: Path, verbose: bool = False, plugin="<plugin>", region="<region>"
) -> None:
    """
    Execute all prepare scripts in package.
    """
    if verbose:
        print(f'Script: "{path}"')

    directory = path.parent
    t0 = time.time()
    env = {
        "__file__": str(path),
        "__name__": "__main__",
    }

    cwd = os.getcwd()
    out = io.StringIO()
    try:
        Data._as_script = False
        os.chdir(str(directory))
        with contextlib.redirect_stdout(out):
            try:
                Data._path = directory
                exec(path.read_text(), env)
            finally:
                Data._path = None
    except Exception as ex:
        raise RuntimeError(
            f"script failed with error ([{plugin}.{region}]): \n" f"{ex}"
        ) from ex
    finally:
        Data._as_script = True
        os.chdir(cwd)

    out = out.getvalue()
    if out and verbose:
        lines = out.splitlines()
        print("OUT:")
        print("\n".join(f"  - {ln.rstrip()}" for ln in lines))

    dt = time.time() - t0
    if verbose:
        print(f"Script executed in {dt:3.2} seconds.\n", flush=True)


def _main():
    for part in find_prepare_scripts(config.mundi_data_path() / "data"):
        plugin, region, path = part
        print(f"{plugin}[{region}]")


if __name__ == "__main__":
    _main()
