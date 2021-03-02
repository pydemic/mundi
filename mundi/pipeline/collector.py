import gzip
import pickle
from abc import ABC
from collections import defaultdict
from functools import cmp_to_key
from pathlib import Path
from typing import List, Iterable, Optional, Dict, Union
import sidekick.api as sk

import pandas as pd

from .. import db
from ..filling_policies import FillPolicy, apply_filling_policy
from ..logging import log
from ..utils import read_file


class Collector(ABC):
    """
    Collect chunks of data into an unified dataframe ready to be loaded into
    the final SQL database.
    """

    REGISTRY = {}
    FILL_NONE = FillPolicy.NONE
    FILL_INHERIT = FillPolicy.INHERIT
    FILL_SUM_CHILDREN = FillPolicy.SUM_CHILDREN
    FILL_MAX_CHILDREN = FillPolicy.MAX_CHILDREN
    FILL_MIN_CHILDREN = FillPolicy.MIN_CHILDREN
    DEFAULT_FILL_POLICY = FILL_NONE

    _init = False
    path: Path
    info: db.TableInfo
    keep_duplicate = "last"
    sort_column: str = None
    fill_policy_map: dict = None
    auto_index: bool = False
    max_size: Optional[int]
    table_name: str = sk.delegate_to('info.name')

    @classmethod
    def register(cls, table, universe: db.Universe):
        """
        Register collector class to the given table.
        """
        info = db.Universe.from_string(universe).table_info(table)

        def decorator(collector_cls):
            cls.REGISTRY[info] = collector_cls
            return collector_cls

        return decorator

    def __init__(self, path, info: db.TableInfo, max_size: int = None):
        if not self._init:
            self.path = Path(path)
            self.info = info
            self.max_size = max_size
        self._init = True

    def __new__(cls, path, info: db.TableInfo, max_size=None):
        if cls is Collector:
            cls = Collector.REGISTRY.get(info, Collector)
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
        try:
            (chunk_type,) = set(map(type, chunks.values()))
        except IndexError:
            if chunks:
                raise ValueError("chunks of different types")
            raise ValueError("empty list of chunks")

        if chunk_type is dict:
            pending = defaultdict(dict)
            for k, tables in chunks.items():
                for k_, v in tables.items():
                    pending[k_][k] = v
            table = {k: self.prepare_table(v) for k, v in pending.items()}
            n = max(map(len, table.values()))
            m = len(table)
            log.info(f"collected {m} tables of {n} rows for {self.info.name}")
        else:
            table = self.prepare_table(chunks)
            n = len(table)
            log.info(f"collected {n} rows for {self.info.name}")

        try:
            self.validate(table)
        except ValueError as ex:
            msg = f'validation error during processing of {self.table_name}:\n{ex}'
            raise ValueError(msg) from ex
        return table

    def validate(self, table: Union[pd.DataFrame, Dict[str, pd.DataFrame]]):
        """
        Validate data and raise a ValueError if some error is found.
        """

    def load_chunks(self) -> Dict[str, pd.DataFrame]:
        """
        Load list of chunks.
        """
        path = self.path / "chunks"
        if not path.exists():
            raise ValueError("could not find a chunks/ directory in path.")

        chunks = []
        size = 0
        for filename in sort_region_names(path.iterdir()):
            tb, _, tail = str(filename.name).partition("-")
            if tb != self.info.name:
                continue

            region, _, ext = tail.partition(".")

            data = read_file(filename)
            n = len(data)
            log.info(f"loading chunk file: {filename} ({n} rows)")
            chunks.append((region, data))

            size += len(data)
            if self.max_size is not None and size > self.max_size:
                break

        if not chunks:
            raise ValueError(f"no chunk found for {self.info.name!r} table")

        chunks.sort(key=cmp_to_key(self._chunk_cmp))
        return {region: chunk for region, chunk in chunks}

    def fill_policy(self, col: str) -> FillPolicy:
        """
        Return fill policy for column
        """
        if self.fill_policy_map is None:
            return self.DEFAULT_FILL_POLICY
        return self.fill_policy_map.get(col, self.DEFAULT_FILL_POLICY)

    def fill_missing(self, table: pd.DataFrame):
        """
        Fill missing data for all columns of table using the given policy.
        """
        columns = defaultdict(list)
        for col in table.columns:
            policy = self.fill_policy(col)
            columns[policy].append(table[col])

        if len(columns) == 1:
            policy, _ = columns.popitem()
            log.info(f'filling data [{self.info.name}]: {policy}')
            return apply_filling_policy(table, policy)
        else:
            raise NotImplementedError("multiple policies")

    def prepare_table(self, chunks: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Join list of chunks in a single dataframe, normalizing result.

        Default implementation simply remove duplicates, keeping only the last
        observed value.
        """
        (ra, a), *rest = chunks.items()
        for (rb, b) in rest:
            if (a.dtypes != b.dtypes).all():
                raise TypeError(
                    f"region {a} and {b} have conflicting chunk types:\n"
                    f"{a.dtypes}\n{b.dtypes}"
                )
            if a.index.names != b.index.names:
                raise TypeError(
                    f"[{self.info.name}] region {ra} and {rb} have conflicting index "
                    f"names:\n"
                    f"  {ra}: {a.index.names}\n"
                    f"  {rb}: {b.index.names}"
                )

        chunk_list = list(chunks.values())

        if self.max_size is not None:
            n = self.max_size
            chunk_list, chunk_list_ = [], chunk_list
            for chunk in chunk_list_:
                chunk = chunk.iloc[:n]
                chunk_list.append(chunk)
                n -= len(chunk)
                if n <= 0:
                    break

        table: pd.DataFrame = pd.concat(chunk_list)
        table.index.name = chunk_list[0].index.name

        if self.auto_index:
            table = table.reset_index(drop=True)
        elif self.keep_duplicate is not None:
            indexes = table.index.names
            columns = table.columns
            table = table.reset_index()
            table = table.drop_duplicates(indexes, keep=self.keep_duplicate)
            table = table.set_index(indexes)
            table.columns = columns

        if self.sort_column is not None:
            table = table.sort_values(self.sort_column)

        return self.fill_missing(table)

    def process(self, data=None) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Load and save data to <path>/databases/<table>.pkl.gz
        """
        if data is None:
            data = self.load()
        path = self.path / "databases" / (self.info.name + ".pkl.gz")
        path.parent.mkdir(exist_ok=True, parents=True)

        if hasattr(data, "to_pickle"):
            data.to_pickle(str(path))
        else:
            with gzip.open(str(path), "wb") as fd:
                pickle.dump(data, fd)
        return data

    def _chunk_cmp(self, a, b):
        """
        Compare chunks in a way that the first chunks are inserted into the
        database followed by the more specific ones, which may override data
        inserted by previous rows.
        """
        region_a, data_a = a
        region_b, data_b = b

        if region_a == "XX":
            return -1
        elif region_b == "XX":
            return 1
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
        if data.index.name == "id":
            levels = data.mundi["level"]
        else:
            return float("inf")
    return levels.mean()


def sort_region_names(names: Iterable[Path]) -> List[Path]:
    """
    Sort region names forcing world (XX) showing before other regions.
    """

    def key(p):
        base, _, ext = p.name.partition(".")
        table, _, region = base.rpartition("-")
        return region != "XX", len(base)  # False (0) < True (1)

    return sorted(names, key=key)
