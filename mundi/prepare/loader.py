from pathlib import Path

import pandas as pd


class Loader:
    """
    Prepare raw d ata and save it in a useful form.
    """

    DATA = Path("./data").resolve().absolute()
    if not DATA.exists():
        DATA = Path(__file__).resolve().parent.parent.parent / "data"

    DATABASES = (Path(__file__).parent.parent / "databases").resolve().absolute()
    TMP_DATABASES = property(lambda self: self.DATA / "tmp")
    IS_TEMPORARY = True

    KEY = property(lambda self: type(self).__name__.lower())
    TARGET = "db"
    FORMAT = "pickle"

    def __init__(self, path=None, target=None):
        self.data_dir = Path(path or self.DATA)
        self.target_dir = Path(target or self.DATABASES)

    def load(self) -> pd.DataFrame:
        """
        Load data from path and return a dataframe.
        """
        raise NotImplementedError

    def load_cached(self):
        """
        Try loading from temporary location and, if that fails, execute the
        load() method.
        """
        path = self.database_path(base=self.TMP_DATABASES, format="pickle")
        if path.exists():
            return fix_string_columns_bug(pd.read_pickle(path))
        else:
            data = self.load()
            data.to_pickle(path)
            return data

    def save(self, data, format=None, **kwargs):
        """
        Save data to the default persistent location.
        """
        format = format or self.FORMAT
        if format == "sql":
            self.save_sqlite(data, **kwargs)
        elif format == "hdf5":
            self.save_hdf5(data, **kwargs)
        elif format == "pickle":
            self.save_pickle(data, **kwargs)
        else:
            raise ValueError(f"invalid format: {format!r}")

    def save_sqlite(self, data, target=None, base=None):
        """
        Save data to a sqlite database.
        """
        import sqlite3

        path = self.database_path(target, format="sql", base=base)
        with sqlite3.connect(path) as conn:
            data.to_sql(self.KEY, conn, if_exists="replace")

    def save_hdf5(self, data, target=None, base=None):
        """
        Save data to a hdf5 database.
        """
        path = self.database_path(target, format="hdf5", base=base)
        data.to_hdf(path, self.KEY, mode="w", format="table")

    def save_pickle(self, data, target=None, base=None):
        """
        Save data to a pickle file.
        """
        path = self.database_path(target, format="pickle", base=base)
        data.to_pickle(path)

    def run(self, use_cache=True):
        """
        Load and saves data to the desired database.
        """
        if use_cache:
            data = self.load_cached()
        else:
            data = self.load()
        self.save(data)

    def log(self, msg):
        """
        Print message to the console.
        """
        print(msg, flush=True)

    def database_path(self, target=None, base=None, format=None):
        """
        Return the full path to the database file.
        """
        base = base or self.DATABASES
        format = format or self.FORMAT
        if format == "pickle":
            return base / (target or self.TARGET + f"-{self.KEY}.pkl")
        elif format == "sql":
            return base / (target or self.TARGET + ".sqlite")
        elif format == "hdf5":
            return base / (target or self.TARGET + ".hdf5")
        else:
            raise RuntimeError("invalid format!")


# TODO: report bug, check which versions are affected by it
def fix_string_columns_bug(df):
    """
    It seems that pandas do not load pickled dataframes with string columns
    with pd.NA values.

    It seems to work in small dataframes, but not large(ish) ones.
    """
    for col, dtype in df.dtypes.items():
        if dtype == "string":
            df[col] = df[col].astype(str).fillna(pd.NA).astype("string")
    return df
