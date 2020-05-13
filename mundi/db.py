import sqlite3
from collections.abc import Sequence
from functools import lru_cache
from pathlib import Path

import pandas as pd

from .constants import DATA_COLUMNS

SQLITE_PATH = Path(__file__).parent.resolve() / "databases" / "db.sqlite"
COLUMNS = DATA_COLUMNS["mundi"]
UN_COLUMNS = ["region", "income_group"]
UN_COLUMN_TYPES = {"region": "category", "income_group": "category"}


class RegionsDB:
    """
    Implements the countries(), regions() and region() callables.
    """

    columns = COLUMNS
    column_types = {k: "string" for k in columns}
    column_types["type"] = "category"

    def __init__(self, ref, path=SQLITE_PATH, **kwargs):
        self._path = path
        self._ref = ref
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __call__(self, *args, **kwargs):
        kws = {k: v for k, v in kwargs.items() if k in self.columns}
        if kws:
            kwargs = {k: v for k, v in kwargs.items() if k not in kws}
        df = self.query(**kws)
        if args:
            df = df.mundi.extend(*args)
        if kwargs:
            df = df.mundi.select(**kwargs)
        return df

    def __hash__(self):
        return id(self)

    def query(self, case_sensitive=False, cols=("id", "name"), **kwargs):
        """
        Query database with given filtering parameters.
        """

        cols = "*" if cols is None else ", ".join(cols)
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        filters = [f"{filter_command(k, v, case_sensitive)}" for k, v in kwargs.items()]
        filters_suffix = ""
        if filters:
            filters_suffix = " WHERE " + " AND ".join(filters)
        cmd = "SELECT %s FROM {table}%s;" % (cols, filters_suffix)
        return self.sql(cmd, index=cols != "id")

    def get(self, *args, **kwargs):
        """
        Get single element from database.
        """

        if args:
            (id_,) = args
        else:
            id_ = None

        if kwargs and id_:
            raise TypeError("canoot pass id and query parameters simultaneously")
        elif id_:
            cmd = 'SELECT * FROM {table} WHERE id="%s" COLLATE NOCASE;' % id_
            data = self.sql(cmd)
        elif kwargs:
            data = self.query(**kwargs)
        else:
            raise TypeError("must pass id or some filter for query parameters")

        if len(data) == 0:
            raise LookupError("no element found with the given ID")
        elif len(data) > 1:
            raise LookupError("found multiple elements")

        res = data.iloc[0]
        res.name = res.get("id", data.index[0])
        return res

    def column_loader(self, col):
        """
        Return a loader function for the given column.
        """

        return lambda df: self.load_column(col, df.index)

    def load_column(self, col, ids=None):
        """
        Load column from database.
        """

        ids = ",".join(map(repr, ids))
        cmd = "SELECT id, %s FROM {table} WHERE id IN (%s);" % (col, ids)
        return self.sql(cmd, index=True)

    def sql(self, sql, copy=True, index=False):
        """
        Execute raw SQL command.
        """

        sql = sql.format(table=self._ref)
        df = read_sql(self, self._path, sql, index=index)
        return df.copy() if copy else df

    def raw_sql(self, sql):
        """
        Execute raw SQL command.
        """

        sql = sql.format(table=self._ref)
        with sqlite3.connect(self._path) as conn:
            c = conn.cursor()
            return c.execute(sql)


def filter_command(k, v, case_sensitive=True):
    if isinstance(v, str) or not isinstance(v, Sequence):
        cmd = f'{k} = "{repr(v)[1:-1]}"'
    else:
        sep = " or "
        args = (filter_command(k, vi) for vi in v)
        cmd = f"({sep.join(args)})"

    if not case_sensitive:
        cmd += " COLLATE NOCASE"
    return cmd


@lru_cache(256)
def read_sql(db, path, sql, index=True):
    kwargs = {"index_col": "id"} if index else {}

    with sqlite3.connect(path) as conn:
        df = pd.read_sql(sql, conn, **kwargs)

        if index:
            cols = [c for c in db.columns if c in df.columns]
            types = {c: db.column_types[c] for c in cols}
            df = df[cols].astype(types)

    return df


db = RegionsDB("mundi")
db_un = RegionsDB("un", columns=UN_COLUMNS, column_types=UN_COLUMN_TYPES)
