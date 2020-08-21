from abc import ABC, abstractmethod
from functools import cmp_to_key
from pathlib import Path

import pandas as pd

from .._utils import read_file


class Collector(ABC):
    """
    Abstract collector interface.
    """

    path: Path
    table: str = None

    @classmethod
    def main(cls):
        """
        Main CLI interface for data collectors.
        """

        import click

        @click.command()
        @click.argument("path")
        @click.option("table", default=cls.table)
        def run(path, table):
            cls(path, table).process()

        run()

    @abstractmethod
    def load(self) -> pd.DataFrame:
        """
        Load and merge chunks from data path. Return merged result.
        """
        raise NotImplementedError

    @abstractmethod
    def process(self) -> None:
        """
        Load and save data to <path>/databases/<table>.pkl.gz
        """
        data = self.load()
        data.to_pickle(str(self.path / "databases" / (self.table + ".pkl.gz")))

    def __init__(self, path, table=None):
        self.path = Path(path)
        if self.table is not None:
            self.table = table


class ChunksCollector(Collector):
    """
    Collect chunks from path.
    """

    def __init__(self, path, table=None, cmp=None):
        super().__init__(path, table)
        self._chunk_cmp = cmp or chunk_cmp

    def load(self):
        """
        Load and prepare chunks created with data scripts.

        This method looks at <path>/chunks/ and concatenates all dataframes
        created for the current table. The default logic implemented by this
        method removes duplicates by prioritizing dataframes with higher
        average level (and thus more specific to some region) than the ones with
        lower level values.
        """

        path = self.path / "chunks"
        if not path.exists():
            raise ValueError("could not find a chunks/ directory in path.")

        chunks = []
        for filename in path.iterdir():
            tb, _, tail = str(filename.name).partition("-")
            if tb != self.table:
                continue

            region, _, ext = tail.partition(".")
            suffix, _, region = region.rpartition("-")

            data = read_file(filename)
            chunks.append((region, suffix, data))
        chunks.sort(key=cmp_to_key(self._chunk_cmp))
        return (
            pd.concat([chunk[-1] for chunk in chunks])
            .reset_index()
            .drop_duplicates("id", keep="last")
            .set_index("id")
        )


def chunk_cmp(a, b):
    """
    Compare chunks in a way that the first chunks are inserted into the
    database followed by the more specific ones, which may override data
    inserted by previous rows.
    """
    region_a, suffix_a, data_a = a
    region_b, suffix_b, data_b = b

    if region_a == "XX" and region_b != "XX":
        return -1
    elif region_a == "XX" and region_b == "XX":
        return -1 if not suffix_a or suffix_a < suffix_b else 1
    else:
        level_a = chunk_level(data_a)
        level_b = chunk_level(data_b)
        if level_a == level_b:
            return 0
        elif level_a < level_b:
            return -1
        else:
            return 1


def chunk_level(data):
    """
    Compute the mean level of data.
    """

    try:
        levels = data["level"]
    except KeyError:
        raise NotImplementedError
    else:
        return levels.mean()
