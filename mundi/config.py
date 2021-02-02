import os
from pathlib import Path
from typing import Tuple

import pandas as pd
from sidekick import api as sk
from sqlalchemy import create_engine


@sk.once
def mundi_data_path() -> Path:
    """
    Return local path to the mundi-data repository, if it exists.
    """

    if os.environ.get("MUNDI_DATA_PATH"):
        return Path(os.environ.get("MUNDI_DATA_PATH"))
    if os.path.exists("mundi-data"):
        return Path("mundi-data").absolute()

    import mundi

    mundi_dir = Path(mundi.__file__).parent.resolve().parent
    if os.path.exists(mundi_dir / "mundi-data"):
        return (mundi_dir / "mundi-data").absolute()

    raise ValueError("could not find mundi data repository")


@sk.once
def mundi_db_engine():
    """
    Return the SQL engine that access the mundi database.
    """
    return create_engine(f"sqlite:///{mundi_db_path()}", echo=False)


@sk.once
def mundi_lib_path() -> Path:
    """
    Path to mundi library in user HOME directory.
    """
    path = Path(os.path.expanduser("~/.local/lib/mundi/"))
    path.mkdir(parents=True, exist_ok=True)
    return path


@sk.once
def mundi_db_path() -> Path:
    """
    Path to sql library under the mundi_path.
    """
    return mundi_lib_path() / "db.sqlite3"


def mundi_data_uri(table) -> Tuple[Path, bool]:
    """
    Return a pair with (URI, is_remote) with the location of data for the
    given table.

    The result can be either a local URI, in which case is_remote is False or
    a remnote URL.
    """
    path = mundi_data_path()
    return path / f"{table}.pkl.gz", False


def mundi_dataframe(name) -> pd.DataFrame:
    """
    Return a dataframe with mundi data with the given name.

    If data is not found locally or if name is None, fallback to downloading
    from the given URL.
    """
    if name:
        for ext in (".pkl", ".pkl.gz", ".pkl.bz2"):
            path = MUNDI_PATH / "data" / (name + ext)
            if path.exists():
                break
        else:
            path = url
    else:
        path = url
    if not path:
        raise ValueError("name or valid url must be given")
    data = pd.read_pickle(path)
    return fix_string_columns_bug(data)
