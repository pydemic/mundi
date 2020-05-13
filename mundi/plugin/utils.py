import importlib
import os
import re
import subprocess
import sys
import time
from itertools import chain
from pathlib import Path
from typing import Dict, List, Iterable

import pandas as pd
import sidekick as sk

from ..functions import regions
from ..types import PandasT

sqlite3 = sk.import_later("sqlite3")
IS_CODE_PATH = re.compile(r"[A-Z]{2}(-\w+)?")
PICKLE_EXTENSIONS = (".pkl", ".pickle", ".pkl.gz", ".pickle.gz")
SQL_EXTENSIONS = (".sql", ".sqlite")
CSV_EXTENSIONS = (".csv", ".csv.gz", ".csv.bz2")
HDF5_EXTENSIONS = (".hdf", ".hfd5")

db = sk.deferred(regions)


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


def find_data_sources(package) -> Dict[str, Path]:
    """
    Return a map from codes to paths to the locations to data sources for
    each region.
    """
    out = {}
    path = find_data_path(package)
    for sub in os.listdir(str(path)):
        if IS_CODE_PATH.fullmatch(sub):
            out[sub] = path / sub
    return out


def find_data_scripts(package) -> Dict[str, List[Path]]:
    """
    Return a map from codes to paths to the locations of the prepare scripts
    for the given folder.
    """
    out = {}
    for key, path in find_data_sources(package).items():
        out[key] = [
            path / p
            for p in sorted(os.listdir(str(path)))
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
