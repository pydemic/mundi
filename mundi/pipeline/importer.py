from abc import ABC
from pathlib import Path
from typing import Iterable, Callable, Dict, Union

import numpy as np
import pandas as pd
from sidekick.properties import delegate_to, lazy

from .. import config
from .. import db
from ..logging import log
from ..utils import read_file

EMPTY = object()


class Importer(ABC):
    """
    Base class responsible for importing data from dataframes to the SQL
    database.

    Importers do not validate data.
    """

    REGISTRY = {}
    _init = False
    path: Path
    info: db.TableInfo
    universe: db.Universe = delegate_to("info")
    default_chunk_size = None

    @classmethod
    def register(
        cls,
        table: Union[db.TableInfo, str],
        importer_cls=None,
        *,
        universe: db.Universe = None,
    ):
        """
        Register importer class to the given table.
        """

        if isinstance(table, str):
            if universe is None:
                raise TypeError("must give a universe when initialized from string.")
            if isinstance(universe, str):
                universe = db.Universe.from_string(universe)
            table = universe.table_info(table)

        def decorator(importer_cls_):
            cls.REGISTRY[table] = importer_cls_
            return importer_cls_

        if importer_cls is not None:
            return decorator(importer_cls)

        return decorator

    def __init__(self, path, info: db.TableInfo, chunk_size=None):
        if not self._init:
            self.path = Path(path)
            self.info = info
            self.chunk_size = chunk_size or self.default_chunk_size
        self._init = True

    def __new__(cls, path, info, chunk_size=None):
        if cls is Importer:
            cls = Importer.REGISTRY.get(info, SQLImporter)
        return object().__new__(cls)

    def save(self):
        """
        Load data into database.
        """
        raise NotImplementedError

    def load_chunks(self, chunk_size=None) -> Iterable[pd.DataFrame]:
        """
        Loads data from main source.

        Return an iterable with data chunks that comfortably fits in memory.
        """

        if chunk_size is None:
            chunk_size = self.chunk_size
        yield from iter_chunks(self.load_all(), chunk_size)

    def load_all(self) -> pd.DataFrame:
        """
        Load data as a single chunk
        """
        return read_file(self.path / f"{self.info.name}.pkl.gz")


class SQLImporter(Importer):
    """
    Import data to SQL database.
    """

    default_chunk_size = 2048

    @lazy
    def model(self) -> db.Table:
        if not self.info.is_sql:
            raise RuntimeError("Cannot import non-SQL tables.")
        return self.info.row_type

    def save(self, append=False):
        """
        Load data to SQL database.
        """
        if not append:
            self.clear()

        for chunk in self.load_chunks():
            self.save_chunk(chunk)

    def save_chunk(self, data: pd.DataFrame):
        """
        Saves chunk of data to the database.
        """
        session = db.session()
        rows = []

        if set(data.index.names) == set(self.universe.indexes()):
            data = data.reset_index()

        for row_data in data.to_dict("records"):
            row = clean_row(row_data)
            rows.append(self.model(**row))

        log.debug(f"[{self.info}] saving {len(rows)} rows to db")
        session.add_all(rows)
        session.commit()

    def clear(self):
        """
        Clear data in table.
        """

        session = db.session()
        session.query(self.model).delete()
        session.commit()
        log.info(f"[{self.info}] deleted all entries")


class HDF5Importer(Importer):
    """
    Import data to HDF5 database.
    """

    def save(self):
        data = self.load_all()
        path = config.mundi_lib_path() / f"db.h5"
        key = f"{self.info.universe.value}/{self.info.name}"
        if isinstance(data, dict):
            for k, df in data.items():
                df.to_hdf(str(path), key=f"{key}/{k}")
        else:
            raise NotImplementedError


# @lru_cache(64)
def column_getters(index: pd.Index) -> Dict[str, Callable]:
    from operator import itemgetter

    return {col: itemgetter(col) for col in set(index.get_level_values(0))}


def clean_row(data):
    """
    Clean a record dictionary replacing pd.NA to None.
    """
    return {k: clean_value(v) for k, v in data.items()}


def clean_value(value):
    if value is pd.NA:
        return None
    elif isinstance(value, (np.int32, np.int64)):
        return int(value)
    return value


def iter_chunks(data, chunk_size):
    """
    Iterate over sliceable object in chunks.
    """
    idx = 0
    size = len(data)
    while idx < size:
        chunk = data[idx : idx + chunk_size]
        yield chunk
        idx += chunk_size
