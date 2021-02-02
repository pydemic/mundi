from abc import ABC
from functools import cmp_to_key
from pathlib import Path
from typing import List

import pandas as pd
import sidekick.api as sk

from ..logging import log
from ..utils import read_file


class Collector(ABC):
    """
    Collect chunks of data into an unified dataframe ready to be loaded into
    the final SQL database.
    """

    TABLE_MAPPER = {}
    path: Path
    table: str
    duplicate_indexes = "id"

    @classmethod
    def cli(cls, click=sk.import_later("click")):
        """
        Main CLI interface for data collectors.
        """

        @click.command()
        @click.argument("path")
        @click.option("--table", "-t", default="region")
        @click.option("--show", "-s", is_flag=True)
        @click.option("--verbose", "-v", is_flag=True, help="show debug messages")
        def collect(path, table, show, verbose):
            collector = cls(path, table=table)
            if verbose:
                log.setLevel("DEBUG")
            if show:
                print(collector.load())
            else:
                collector.process()

        collect()

    def __init__(self, path, table):
        self.path = Path(path)
        self.table = table

    def __new__(cls, path, table):
        if cls is Collector:
            cls = Collector.TABLE_MAPPER.get(table, Collector)
        return object().__new__(cls)

    def load(self):
        """
        Load and prepare chunks created with data scripts.

        This method looks at <path>/chunks/ and concatenates all dataframes
        created for the current table. The default logic implemented by this
        method removes duplicates by prioritizing dataframes with higher
        average level (and thus more specific to some region) than the ones with
        lower level values.
        """

        chunks = self.load_chunks()
        table = self.prepare_table(chunks)
        n = len(table)
        log.info(f"collected {n} rows for {self.table}")
        return table

    def load_chunks(self) -> List[pd.DataFrame]:
        """
        Load list of chunks.
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
            n = len(data)
            log.info(f"loading chunk file: {filename} ({n} rows)")
            chunks.append((region, suffix, data))

        if not chunks:
            raise ValueError(f"no chunk found for {self.table!r} table")

        chunks.sort(key=cmp_to_key(self._chunk_cmp))
        return [chunk for _, _, chunk in chunks]

    def prepare_table(self, chunks: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Join list of chunks in a single dataframe, normalizing result.

        Default implementation simply remove duplicates, keeping only the last
        observed value.
        """
        table = pd.concat([chunk for chunk in chunks])
        table.index.name = "id"
        data = (
            table.reset_index()
            .drop_duplicates(self.duplicate_indexes, keep="last")
            .set_index("id")
        )
        return data

    def process(self) -> None:
        """
        Load and save data to <path>/databases/<table>.pkl.gz
        """
        data = self.load()
        data.to_pickle(str(self.path / "databases" / (self.table + ".pkl.gz")))

    def _chunk_cmp(self, a, b):
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


class RegionM2MCollector(Collector):
    """
    Specialized collector for the region_m2m table.
    """

    duplicate_indexes = ["child_id", "parent_id", "relation"]

    def prepare_table(self, chunks):
        table = super().prepare_table(chunks)
        regions = self.path / "databases" / "region.pkl.gz"

        if regions.exists():
            extra = self.prepare_default_m2m(read_file(regions))
            table = pd.concat([table, extra]).drop_duplicates().reset_index(drop=True)
        else:
            warnings.warn(f"no region.pkl.gz found at {regions}")

        return table

    def prepare_default_m2m(self, data):
        return (
            data[["parent_id"]]
            .reset_index()
            .rename(columns={"id": "child_id"})
            .assign(relation="default")
            .astype("string")
            .dropna()
        )[["child_id", "parent_id", "relation"]]


Collector.TABLE_MAPPER["region_m2m"] = RegionM2MCollector


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


if __name__ == "__main__":
    Collector.cli()
