import contextlib
import io
import os
import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Iterator

import pandas as pd
import sidekick.api as sk

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
    _as_script = True

    @classmethod
    def cli(cls, **kwargs):
        """
        Create instance and execute.
        """

        def main(show=False, validate=False):
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

        if cls._as_script:
            import click

            @click.command()
            @click.option(
                "--show", "-s", is_flag=True, help="Show data instead of saving it"
            )
            @click.option(
                "--validate", "-v", default=False, is_flag=True, help="Validates data."
            )
            def _main(**kwargs):
                return main(**kwargs)

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

        attr = f"{name.upper()}_DATA_TYPES"
        try:
            dtypes = getattr(self, attr)
        except AttributeError:
            raise NotImplementedError(f"class must implement {attr} dictionary")
        else:
            if dtypes is not None:
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
    check_column_types = staticmethod(check_column_types)
    check_unique_index = staticmethod(check_unique_index)
    check_no_object_columns = staticmethod(check_no_object_columns)


def find_prepare_scripts(path: Path) -> Iterator[Path]:
    """
    Return an iterator with (plugin, region, path) with the location of all
    prepare.py plugins under the current mundi-data path.
    """
    print("scripts")
    for region in sort_region_names(os.listdir(path)):
        for plugin in sort_plugin_names(os.listdir(path / region)):
            prepare = (path / region / plugin / "prepare.py").absolute()
            if os.path.exists(prepare):
                yield plugin, region, prepare


def _main():
    for part in find_prepare_scripts(config.mundi_data_path() / "data"):
        plugin, region, path = part
        print(f"{plugin}[{region}]")


def execute_prepare_script(path: Path, verbose: bool = False) -> None:
    """
    Execute all prepare scripts in package.
    """
    if verbose:
        print(f'Script: "{path}"')

    dir = path.parent
    t0 = time.time()
    env = {"__file__": str(path), "__name__": "__main__"}

    cwd = os.getcwd()
    out = io.StringIO()
    try:
        Data._as_script = False
        os.chdir(str(dir))
        with contextlib.redirect_stdout(out):
            exec(path.read_text(), env)
    except Exception as ex:
        raise RuntimeError(f"script failed with error: {ex}")
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


if __name__ == "__main__":
    _main()
