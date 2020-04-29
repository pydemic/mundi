from pathlib import Path

import pandas as pd
from sidekick import import_later

DATABASES = Path(__file__).parent / "databases"
DATA_LOADERS = {
    "region": import_later("mundi.extra:load_region"),
    "income_group": import_later("mundi.extra:load_income_group"),
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
