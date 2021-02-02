import importlib
import os
import re
import subprocess
import sys
import time
from abc import ABC
from itertools import chain
from pathlib import Path
from typing import Dict, List, Iterable
from typing import TypeVar

import pandas as pd
import sidekick.api as sk

from mundi._utils import fix_string_columns_bug
from ..functions import regions
from ..types import PandasT

sqlite3 = sk.import_later("sqlite3")
IS_CODE_PATH = re.compile(r"[A-Z]{2}(-\w+)?")
PICKLE_EXTENSIONS = (".pkl", ".pickle", ".pkl.gz", ".pickle.gz")
SQL_EXTENSIONS = (".sql", ".sqlite")
CSV_EXTENSIONS = (".csv", ".csv.gz", ".csv.bz2")
HDF5_EXTENSIONS = (".hdf", ".hfd5")

db = sk.deferred(regions)

T = TypeVar("T")


class Plugin(ABC):
    """
    Base plugin class.
    """

    name: str = NotImplemented
    _instance: "Plugin"

    @classmethod
    @sk.once
    def initialize(cls):
        """
        Default method for initializing plugin when mundi starts.

        Plugins are (quasi)-singletons, in which the default constructor accepts
        only a single instance of the plugin and recycle them in future
        invocations.
        """
        raise

    def __new__(cls: type, *args, recycle=True, **kwargs):
        if ABC in cls.__bases__:
            raise TypeError("cannot create instance of abstract type!")
        elif recycle and "_instance" in cls.__dict__:
            return cls._instance
        elif not recycle:
            return super().__new__(cls, *args, **kwargs)
        else:
            cls._instance = new = cls(*args, recycle=False, **kwargs)
            return new

    def __init__(self, name=None, recycle=True):
        if recycle:
            return
        if name is None and not hasattr(self, "name"):
            raise RuntimeError("Class does not implement a name attribute.")
        self.name = name or self.name

    #
    # Query properties
    #
    def tables(self) -> Iterable[str]:
        """
        Iterates over all table names exposed by the plugin.
        """
        raise NotImplementedError

    def is_loaded(self, table) -> bool:
        """
        Return True if data is already loaded to the database.
        """
        raise NotImplementedError

    #
    # Pipeline
    #
    def pipeline(self: T, path, force=False) -> T:
        """
        Run the complete data processing pipeline from preparation, collection,
        loading to registering.

        Args:
            path:
                Path for the data repository. Must contain the data, chunks and
                build sub-folders.
            force:
                If true, forces execution of pipeline even if it contains a
                successful build.
        """
        if force:
            self.prepare(path)
            for table in self.tables():
                data = self.collect(path, table, force=True)
                self.load(table, data, force=True)
        else:
            raise NotImplementedError

        return self

    def prepare(self, path):
        """
        Execute all prepare.py scripts in the given path.
        """
        print("preparing...", path)
        print(find_data_scripts(path))
        raise NotImplementedError

    def collect(self, path, table, force=False):
        """
        Collect chunks for the given table.
        """
        raise NotImplementedError

    def load_data(self, table, data, force=False):
        """
        Load collected data into database.
        """
        raise NotImplementedError

    def register(self):
        """
        Register itself in the plugins registry that maps fields to SQL queries
        to the database.
        """
        raise NotImplementedError

    def unregister(self):
        """
        Un-register plugin from the global plugin registry.
        """
        raise NotImplementedError


# class _Plugin(ABC):
#     """
#     Plugins store new data in the central database and offer a mundi API to
#     access them.
#
#     Plugins are instantiated with the register classmethod:
#
#     >>> Plugin.register()
#     """
#
#     # Metadata attributes
#     __tables__: Dict[str, Type['Base']] = None
#     __mundi_namespace__: str = sk.lazy(X.name)
#     __data_location__: str = None
#
#     # Computed properties, aliases, delegates and transformations
#     columns: Dict[str, 'Field'] = sk.alias('_columns', transform=MappingProxyType)
#
#     @sk.lazy
#     def plugin_key(self) -> str:
#         cls = type(self)
#         return f'{cls.__module__}.{cls.__qualname__}'
#
#     def __init_subclass__(cls, **kwargs):
#         key = f'{cls.__module__}.{cls.__qualname__}'
#         PLUGIN_DB[key] = cls
#
#     def __init__(self):
#         self._columns = {}
#         self._bind_columns()
#
#     def _bind_columns(self):
#         annotations = getattr(self, '__annotations__', None) or {}
#         cls = type(self)
#         for k in dir(cls):
#             v = getattr(cls, k, None)
#             if isinstance(v, Field):
#                 kind = annotations.get(k)
#                 new: Field = v.update(plugin=self, type=kind, name=k)
#                 setattr(self, k, new)
#                 self._columns[k] = new
#
#     def create_tables(self):
#         """
#         Create and populate necessary SQL tables.
#         """
#
#         create_tables()
#         for name, table in self.__tables__.items():
#             if not MundiRegistry.has_populated_table(self, name):
#                 url = self.__data_location__.format(table=name)
#                 data = mundi_data(name, url)
#                 self.populate(name, data)
#
#     def populate(self, table: str, data: pd.DataFrame):
#         """
#         Populate table with data from the given dataframe.
#         """
#
#         Model = self.__tables__[table]
#         session = new_session()
#
#         # NA types seems to cause surprises. In some cases it produces more than
#         # one instance when pickled from a saved dataframe.
#         for ref, row in data.to_dict('index'):
#             kwargs = {k: v for k, v in row.items() if not isinstance(v, NAType)}
#             session.add(Model(id=ref, **kwargs))
#
#         session.commit()


#
# Utility functions
#
def find_data_path(package) -> Path:
    """
    Find the data folder for package.

    This assumes the package is installed as as symlink from the main
    repository folder.
    """
    try:
        mod = sys.modules[package]
    except KeyError:
        mod = importlib.import_module(package)
    return Path(mod.__file__).resolve().absolute().parent.parent / "data"


def find_data_sources(path) -> Dict[str, Path]:
    """
    Return a map from codes to paths to the locations to data sources for
    each region.
    """
    out = {}
    for sub in os.listdir(str(path)):
        if IS_CODE_PATH.fullmatch(sub):
            out[sub] = path / sub
    return out


def find_data_scripts(path) -> Dict[str, List[Path]]:
    """
    Return a map from codes to paths to the locations of the prepare scripts
    for the given folder.
    """
    out = {}
    for key, sub_path in find_data_sources(path).items():
        out[key] = [
            sub_path / p
            for p in sorted(os.listdir(str(sub_path)))
            if p.startswith("prepare") and p.endswith(".py")
        ]
    return out


def execute_prepare_scripts(package, verbose=False) -> None:
    """
    Execute all prepare scripts in package.
    """
    for code, scripts in find_data_scripts(package).items():
        for script in scripts:
            if verbose:
                print(f'Script: "{code}/{script.name}"')
            dir = script.parent
            name = script.name
            t0 = time.time()
            out = subprocess.check_output([sys.executable, name], cwd=dir)
            if out:
                lines = out.decode("utf8").splitlines()
                print("OUT:")
                print("\n".join(f"  - {ln.rstrip()}" for ln in lines))
            dt = time.time() - t0
            print(f"Script executed in {dt:3.2} seconds.\n\n", flush=True)


def collect_processed_paths(package, kind=None) -> Dict[str, List[Path]]:
    """
    Return a map from codes to paths to the locations of the prepare data in
    each folder.
    """
    kind = kind or ""
    out = {}
    for key, path in find_data_sources(package).items():
        path = path / "processed"
        out[key] = [
            path / p
            for p in os.listdir(str(path))
            if p.startswith(kind) or p.startswith("db")
        ]
    return out


def collect_processed_data(package, kind=None) -> PandasT:
    """
    Return a list of dataframes collected from <code>/processed folders.

    Dataframes are ordered lexicographically first by name of the collected file,
    then by code.
    """
    paths = collect_processed_paths(package, kind)
    paths = list(chain(*paths.values()))
    paths.sort(key=lambda x: x.name)
    if not paths:
        raise ValueError(f"Empty list of datasets for {package}/{kind}")

    cols = None
    datasets = []
    for p in paths:
        df = read_path(p, kind)
        df.index.name = "id"
        if isinstance(df, pd.Series):
            pass
        elif cols is None:
            cols = set(df.columns)
        elif set(df.columns) != cols:
            invalid = cols.symmetric_difference(df.columns)
            raise ValueError(
                f"different columns in two paths:\n"
                f"- {p}\n"
                f"- {paths[0]}"
                f"- Columns: {invalid}"
            )

        if package != "mundi" and not df.index.isin(db.index).all():
            invalid = set(df.index) - db.index
            raise ValueError(
                f"index contain invalid mundi codes:\n"
                f"- {p}\n"
                f"- Invalid codes: {invalid}"
            )

        datasets.append(df)

    data = pd.concat(datasets).reset_index()
    if isinstance(data, pd.Series):
        data = data.drop_duplicates(keep="last")
    elif isinstance(data.columns, pd.MultiIndex):
        col = data.columns[0]
        data = data.drop_duplicates(col, keep="last")
        data = data.set_index("id")
    else:
        data = data.drop_duplicates("id", keep="last")
        data = data.set_index("id")
    return data.sort_index()


def clean_processed_data(package, kind=None, verbose=False):
    """
    Clean all files in the <code>/processed folders.
    """
    for code, paths in collect_processed_paths(package, kind=kind).items():
        for path in paths:
            os.unlink(str(path))
            print("  -", Path("/".join(path.parts[-3:])))


def read_path(path: Path, key: str = None) -> PandasT:
    """
    Read dataframe from path. Optional key is only used with certain file types
    such as sql and hdf5. Otherwise it is ignored.
    """
    name = path.name
    if endswith(name, PICKLE_EXTENSIONS):
        return fix_string_columns_bug(pd.read_pickle(path))
    else:
        raise ValueError(f'could not infer data type: "{path}"')


def save_path(data: PandasT, path: Path, key: str = None, **kwargs) -> None:
    """
    Save dataframe at given path. Optional key is only used with certain file
    types such as sql and hdf5. Otherwise it is ignored.

    Source type is inferred from extension.
    """
    name = path.name

    if endswith(name, PICKLE_EXTENSIONS):
        kwargs.setdefault("protocol", 3)
        data.to_pickle(str(path), **kwargs)

    elif endswith(name, SQL_EXTENSIONS):
        with sqlite3.connect(path) as conn:
            data.to_sql(key, conn, if_exists="replace")

    else:
        raise ValueError(f'could not infer data type: "{path}"')


def endswith(st: str, suffixes: Iterable[str]) -> bool:
    """
    Return true if string ends with one of the given suffixes.
    """
    return any(st.endswith(ext) for ext in suffixes)
