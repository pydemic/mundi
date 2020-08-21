import json
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from pathlib import Path
from typing import Iterable

import pandas as pd
import sidekick.api as sk

from ..typing import Data


class Importer(ABC):
    """
    Base class responsible for importing data from dataframes to the SQL
    database.

    Importers do not validate data.
    """

    def __init__(self,):
        self.cache = cache()

    @abstractmethod
    def load(self) -> Iterable[Data]:
        """
        Loads data from main source.

        Return an iterable with data chunks that comfortably fits in memory.
        """
        raise NotImplementedError

    @abstractmethod
    def save(self, data: Data):
        """
        Saves chunk of data to the database.
        """
        raise NotImplementedError

    def process(self):
        """
        Load data in save all data chunks in iterable.
        """
        for chunk in self.load():
            self.save(chunk)


class Cache(MutableMapping):
    """
    A file-based chunks cache with dataframe iterators.
    """

    path: Path

    def __init__(self, path=None, chunk_size=100):
        self.path = path
        self.chunk_size = chunk_size

    def __iter__(self):
        for f in self.path.iterdir():
            if f.is_file() and f.name.endswith(".csv.gz"):
                yield f.name[:-7]  # strip .pkl.gz from the end

    def __len__(self):
        return sum(1 for _ in self)

    def __getitem__(self, key):
        kwargs = self._read_args(json.load(self._file(key, ".json")))
        return pd.read_csv(self._file(key), **kwargs)

    def __setitem__(self, key, value: Iterable[Data]):
        value = sk.iter(value)
        kwargs = self._prepare_args(value[0])

        with self._file(key).open("bw") as fd:
            for i, chunk in enumerate(value):
                extra = {"header": False} if i != 0 else {}
                chunk.to_csv(fd, **extra, **kwargs)

        with self._file(key, ".json").open("bw") as fd:
            json.dump(kwargs, fd)

    def __delitem__(self, key):
        try:
            self._file(key).unlink()
            self._file(key, ".json").unlink()
        except FileNotFoundError:
            raise KeyError(key)

    def _file(self, key: str, ext: str = ".csv.gz") -> Path:
        return self.path / (key + ext)

    def _read_args(self, json) -> dict:
        return {"chunksize": self.chunk_size, **json}

    def _prepare_args(self, data) -> dict:
        """
        Prepare a json-compatible representation of arguments used by the CSV
        data loader.
        """
        return {}


@sk.once
def cache():
    return Cache(mundi_cache_path())


@sk.once
def mundi_cache_path() -> Path:
    return Path(".")
