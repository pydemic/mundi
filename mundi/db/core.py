import dataclasses
from enum import Enum
from functools import lru_cache
from typing import Tuple, Callable, Union, Any, List, Dict, cast, Type, Iterable, Iterator

import pandas as pd
from sidekick import api as sk
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.attributes import QueryableAttribute
from sqlalchemy.schema import MetaData

from .. import config
from ..config import mundi_db_engine, mundi_db_path

create_tables_executed = False


class Universe(Enum):
    """
    Data universes specify how data relate to other members of Mundi database.

    We define 3 universes:

    * REGION: default data universe. All tables have an 'id' primary key/index
      referencing to an specific region of the world. Tables
      store unique data about a column like name, population, type, etc.
      Plugins can export additional tables in this universe to export
      additional data about a region.
    * HISTORIC: Tables have a composite primary key/index of (id, year) and
      store information about a region in some specific year.
    * ARBITRARY: Arbitrary tables that are not connected to each other and may
      have arbitrary functions inside plugins or in the main Mundi database
      itself. A nice example of an ARBITRARY table is the region_m2m that
      declare regions into different hierarchies of parent/child relationships.
    """

    REGION = "region"
    HISTORIC = "historic"
    ARBITRARY = "arbitrary"

    @classmethod
    @lru_cache(16)
    def from_string(cls, st):
        """
        Create universe object from string description.
        """
        if isinstance(st, Universe):
            return st
        for universe in cls:
            if universe.value == st:
                return universe
        raise ValueError(f"invalid universe string: {st}")

    def tables(self):
        """
        Return a list of tables for the given universe.
        """
        raise NotImplementedError

    def indexes(self) -> List[str]:
        """
        Return standard indexes used for the given universe.
        """
        if self is self.REGION:
            return ["id"]
        elif self is self.HISTORIC:
            return ["id", "year"]
        elif self is self.ARBITRARY:
            return []
        else:
            raise RuntimeError

    @lru_cache(128)
    def table_info(self, name: str) -> "TableInfo":
        """
        Return a TableInfo object from table name.
        """
        if isinstance(name, TableInfo):
            return name
        try:
            return TableInfo.REGISTRY[self][name]
        except KeyError:
            raise ValueError(f"no table registered as '{self.value}.{name}'")

    @lru_cache(512)
    def column(self, ref: str) -> "Column":
        """
        Return Column object with the given ref.
        """
        try:
            registry = Column.REGISTRY[self]
            return registry[ref]
        except KeyError:
            raise ValueError(f'no column "{self.value}.{ref}" reference')

    def query(self, model: Type["Table"], *filters, **filter_dict):
        """
        Query model in universe using a list of filters.

        Filters can also be constructed from keyword arguments using a
        Django-inspired DSL.
        """
        models = model if isinstance(model, (tuple, list)) else (model,)
        query = session().query(*models)
        filters = (*filters, *map(self._to_filter, filter_dict.items()))
        if filters:
            return query.filter(*filters)
        return query

    def _to_filter(self, item):
        key, value = item
        name, *mods = key.split("__")

        if mods:
            raise NotImplementedError

        column = cast(SqlColumn, self.column(name))
        if not column.is_queryable:
            raise ValueError(f"column {name} do not accept queries")

        return column.expression == value


@dataclasses.dataclass(frozen=True)
class TableInfo:
    """
    Base class for SQL and HDF5 tables.
    """

    REGISTRY = {k: {} for k in Universe}
    REGION_UNIVERSE = Universe.REGION
    HISTORIC_UNIVERSE = Universe.HISTORIC
    ARBITRARY_UNIVERSE = Universe.ARBITRARY

    name: str
    is_queryable: bool = False
    columns: Tuple[str, ...] = ()
    universe: Universe = REGION_UNIVERSE
    row_type: type = None

    @property
    def is_sql(self) -> bool:
        return self.is_queryable

    @property
    def is_hdf5(self) -> bool:
        return not self.is_sql

    @classmethod
    def registered(
            cls, *args, internal_columns=(), export_columns=(), **kwargs
    ) -> "TableInfo":
        """
        Create a table info object and register it into global registry.
        """
        info = cls(*args, **kwargs)
        register_table_info(info)

        for fn in export_columns:
            register_column(fn(info))

        if internal_columns is True:
            internal_columns = kwargs.get("columns", ())
        register_table_columns(info, internal=internal_columns)

        return info


class SQLTableInfo(TableInfo):
    """
    Specialized version of TableInfo to describe SQL tables.
    """


class SQLTableBase:
    """
    Base class for all Declarative tables in Mundi.
    """

    REGISTRY = {}
    info: TableInfo = None
    metadata: MetaData
    universe = Universe.ARBITRARY

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, "__tablename__"):
            from sqlalchemy import Column as ColType

            is_column = lambda k: not k.startswith("_") and isinstance(
                getattr(cls, k), ColType
            )
            name = cls.__tablename__
            universe = cls.universe
            columns = tuple(filter(is_column, dir(cls)))
            info = SQLTableInfo(
                name,
                is_queryable=True,
                columns=columns,
                universe=universe,
                row_type=cls,
            )
            register_table_info(info)
            register_table_columns(info)
        else:
            info = None
        cls.info = info


@dataclasses.dataclass(frozen=True)
class Column:
    """
    Base class for SQL and HDF5 columns.
    """

    REGISTRY = {kind: {} for kind in Universe}
    REGISTRY[None] = {}

    name: str
    table: TableInfo
    universe: "Universe"
    is_queryable: bool = False
    is_internal: bool = False

    # Class attributes
    is_sql = False
    is_hdf5 = property(lambda self: not self.is_sql)

    @property
    def universe_key(self):
        """
        A unique identifier in the column universe.
        """
        if self.is_internal:
            return f"{self.table.name}.{self.name}"
        return self.name

    @property
    def global_key(self):
        """
        A unique global identifier for column.
        """
        return f"{self.universe.value}/{self.table.name}/{self.universe_key}"

    def __init__(self, ref, table, universe=None, is_queryable=None, is_internal=False):
        if universe is None:
            universe = table.universe
        if is_queryable is None:
            is_queryable = table.is_queryable
        super().__setattr__("name", ref)
        super().__setattr__("table", table)
        super().__setattr__("is_queryable", is_queryable)
        super().__setattr__("is_internal", is_internal)
        super().__setattr__("universe", universe)

    def get(self, pk) -> Any:
        """
        Retrieve value for cell in the given primary key and column.
        """
        raise NotImplementedError

    def select(self, pks) -> Union[pd.Series, pd.DataFrame]:
        """
        Select values for the list of primary keys.
        """
        raise NotImplementedError


class SqlColumn(Column):
    """
    Represents a column of data stored in a SQL database.

    All elements stored into SQL columns are scalars. SQL columns have the
    distinct advantage of being queryable, but are perhaps less storage
    efficient than HDF5 data.
    """

    is_sql = True
    is_queryable = True

    @sk.lazy
    def expression(self) -> QueryableAttribute:
        """
        Expression element
        """
        return getattr(self.model, self.name)

    @sk.lazy
    def model(self):
        return self.table.row_type

    def get(self, pk: str) -> Any:
        """
        Select scalar value for the given id.
        """
        value = session().query(self.model).get(pk)
        if value is None:
            return None
        return getattr(value, self.name)

    def select(self, pks: List[str], indexes=None) -> Union[pd.Series, pd.DataFrame]:
        """
        Select values for the list of ids.
        """
        if indexes is None:
            indexes = self.universe.indexes()
        if len(indexes) == 1:
            (idx,) = indexes
            filters = [getattr(self.model, idx).in_(pks)]
        else:
            raise NotImplementedError

        index_exprs = [getattr(self.model, name) for name in indexes]
        query = (
            session()
                .query(self.model)
                .filter(*filters)
                .values(*index_exprs, self.expression)
        )
        return (
            pd.DataFrame(query)
                .set_index(indexes)
                .reindex(pks)[self.name]
        )


class HDF5Column(Column):
    """
    Represents a column of data stored into a HDF5 database.

    This is used to store large blobs of data or to store data that span
    multiple columns.
    """

    @staticmethod
    @lru_cache(32)
    def _read(ref) -> pd.DataFrame:
        with HDF5Column._table() as db:
            return cast(pd.DataFrame, db.select(ref))

    @staticmethod
    def _table():
        path = config.mundi_lib_path() / f"db.h5"
        return pd.HDFStore(str(path), mode="r")

    @property
    def hdf5_key(self):
        return f"{self.universe.value}/{self.table.name}/{self.name}"

    def get(self, id: str) -> Any:
        """
        Select scalar value for the given id.
        """
        data = self._read(self.hdf5_key)
        return data.loc[id]

    def select(self, ids: List[str]) -> Union[pd.Series, pd.DataFrame]:
        """
        Select values for the list of ids.
        """
        data = self._read(self.hdf5_key)
        return data.reindex(ids)


@dataclasses.dataclass(frozen=True)
class ComputedColumn(Column):
    arguments: Tuple[str, ...] = ()
    to_scalar: Callable = None
    to_table: Callable = None

    def _to_scalar(self, *args):
        if len(args) == 1:
            return args[0]
        return args

    def _to_table(self, *args):
        return pd.concat({k: v for k, v in zip(self.arguments, args)})

    @property
    def argument_columns(self):
        """
        List of columns corresponding to computed arguments
        """
        try:
            return self._argument_columns
        except AttributeError:
            data = [self.universe.column(col) for col in self.arguments]
            super().__setattr__("_argument_columns", data)
            return data

    def __init__(
            self, ref, table, arguments, universe=None, *, to_scalar=None, to_table=None
    ):
        super().__init__(ref, table, universe=universe, is_queryable=False)
        super().__setattr__("arguments", arguments)
        super().__setattr__("to_scalar", to_scalar or self._to_scalar)
        super().__setattr__("to_table", to_table or self._to_table)

    def get(self, id):
        args = (col.get(id) for col in self.argument_columns)
        return self.to_scalar(*args)

    def select(self, ids):
        args = (col.select(ids) for col in self.argument_columns)
        return self.to_table(*args)


def register_table_info(info: TableInfo):
    """
    Register info object in global registry.
    """
    registry = TableInfo.REGISTRY[info.universe]
    if info.name in registry:
        raise ValueError(f'already registered: "{info.universe.value}.{info.name}"')
    registry[info.name] = info


def register_column(column: Column):
    """
    Register column to global registry.
    """
    info = column.table
    universe = column.universe
    if column.name in Column.REGISTRY[universe]:
        other = Column.REGISTRY[universe][column.name].info
        raise ValueError(
            f"registering '{universe.value}.{info.name}.{column.name}'\n"
            f"    column with ref {column.name!r} already registered for {other.name!r}"
        )
    Column.REGISTRY[universe][column.universe_key] = column
    Column.REGISTRY[None][column.global_key] = column


def register_table_columns(info: TableInfo, internal=()) -> Dict[str, Column]:
    """
    Create a mapping from table to a dictionary of registrable columns.
    """

    cols = [*_table_mapper(info, exclude=info.universe.indexes(), internal=internal)]

    for col in cols:
        register_column(col)

    return {col.name: col for col in cols}


def _table_mapper(info: TableInfo, exclude, internal) -> Column:
    col_class = SqlColumn if info.is_queryable else HDF5Column
    for col in info.columns:
        if col in exclude:
            continue
        yield col_class(col, table=info, is_internal=col in internal)


def session() -> Session:
    """
    Starts a new session to SQL database.
    """
    return BaseSession()


def connection():
    """
    Return a connection with the database.
    """
    return mundi_db_engine().connect()


def create_tables(*, force=False):
    """
    Create all SQL tables.
    """
    global create_tables_executed

    if mundi_db_path().exists() and force:
        mundi_db_path().unlink()
    if force or not create_tables_executed:
        Table.metadata.create_all(mundi_db_engine())
        create_tables_executed = True


def column_expressions(universe: Universe, columns: Iterable[str]) -> \
        Iterator[QueryableAttribute]:
    """
    Return sequence of SQL expressions from names.
    """
    for col in columns:
        col = universe.column(col)
        if not col.is_sql:
            raise ValueError(f'column "{col}" is not an SQL column')
        yield cast(SqlColumn, col).expression


# noinspection PyTypeChecker
Table: Type[SQLTableBase] = declarative_base(
    class_registry=SQLTableBase.REGISTRY, cls=SQLTableBase
)
BaseSession: Callable[[], Session] = sk.deferred(lambda: sessionmaker(mundi_db_engine()))
