import sqlite3
from functools import lru_cache
from pathlib import Path

import pandas as pd

SQLITE_PATH = Path(__file__).parent.resolve() / "databases" / "db.sqlite"
DEFAULT_COLUMNS = [
    "name",
    "type",
    "code",
    "numeric_code",
    "long_code",
    "country_code",
    "parent_id",
]
EXTRA_COLUMNS = []
ALL_COLUMNS = [*DEFAULT_COLUMNS, *EXTRA_COLUMNS]


class RegionsDB:
    """
    Implements the countries(), regions() and region() callables.
    """

    columns = DEFAULT_COLUMNS
    column_types = {k: "string" for k in columns}
    column_types["type"] = "category"

    def __init__(self, ref, path=SQLITE_PATH):
        self._path = path
        self._ref = ref

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

    def query(self, case_sensitive=False, **kwargs):
        """
        Query database with given filtering parameters.
        """

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        filters = [f"{filter_command(k, v, case_sensitive)}" for k, v in kwargs.items()]
        filters_suffix = ""
        if filters:
            filters_suffix = " WHERE " + " AND ".join(filters)
        cmd = "SELECT * FROM {table}%s;" % filters_suffix
        return self._read_sql(cmd)

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
            data = self._read_sql(cmd)
        elif kwargs:
            data = self.query(**kwargs)
        else:
            raise TypeError("must pass id or some filter for query parameters")

        if len(data) == 0:
            raise LookupError("no element found with the given ID")
        elif len(data) > 1:
            raise LookupError("found multiple elements")

        return data.iloc[0]

    def _read_sql(self, sql, copy=True):
        sql = sql.format(table=self._ref)
        df = read_sql(self._path, sql)
        return df.copy() if copy else df


def filter_command(k, v, case_sensitive=True):
    if isinstance(v, str):
        cmd = f'{k} = "{repr(v)[1:-1]}"'
    else:
        sep = " or "
        args = (filter_command(k, vi) for vi in v)
        cmd = f"({sep.join(args)})"

    if not case_sensitive:
        cmd += " COLLATE NOCASE"
    return cmd


@lru_cache(256)
def read_sql(path, sql):
    with sqlite3.connect(path) as conn:
        df = pd.read_sql(sql, conn)
        df.index = df.pop("id")
        df = df.astype(RegionsDB.column_types)[RegionsDB.columns]
    return df


db = RegionsDB("region")
