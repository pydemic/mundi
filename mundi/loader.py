from pathlib import Path

import pandas as pd
from sidekick import import_later

from .db import db

DATABASES = Path(__file__).parent / "databases"
DATA_LOADERS = {
    "region": import_later("mundi.extra:load_region"),
    "income_group": import_later("mundi.extra:load_income_group"),
    "name": db.column_loader("name"),
    "type": db.column_loader("type"),
    "subtype": db.column_loader("subtype"),
    "country_code": db.column_loader("country_code"),
    "short_code": db.column_loader("short_code"),
    "long_code": db.column_loader("long_code"),
    "numeric_code": db.column_loader("numeric_code"),
    "parent_id": db.column_loader("parent_id"),
    "alt_parents": db.column_loader("alt_parents"),
}


def filter_from_data(data, df):
    """
    Filter data in the first argument to present only the rows present in the
    second.
    """
    return data.loc[df.index.dropna()]


def filtering_from_data(cols=slice(None)):
    """
    Transforms a function that loads a data frame into a function that filters
    the rows of this data frame from the data in the first argument.
    """

    def decorator(fn):
        extra = None
        valid = None

        def loader(data):
            nonlocal extra, valid

            if extra is None:
                extra = fn()[cols]
                valid = set(extra.index)

            mask = [k for k in data.index if k in valid]
            out = extra.loc[mask]
            return pd.DataFrame(out, index=data.index)

        return loader

    return decorator


def load_database(name):
    """
    Load data from databases.
    """
    return pd.read_pickle(DATABASES / name)


def register(name, loader):
    """
    Register a loader function for the given column.
    """
    if isinstance(loader, str):
        loader = import_later(loader)
    DATA_LOADERS[name] = loader


def unregister(name, loader=None):
    """
    Unregister a loader function for the given column.
    """

    registered = DATA_LOADERS.get(name)
    if registered is None:
        return
    elif loader is None:
        del DATA_LOADERS[name]
    elif loader == registered:
        del DATA_LOADERS[name]
