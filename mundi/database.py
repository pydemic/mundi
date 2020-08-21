import os
from abc import ABC
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Type, Dict

import pandas as pd
import sidekick.api as sk
import sqlalchemy as sql
from pandas._libs.missing import NAType
from sidekick.api import X
from sidekick.types import Record
from sqlalchemy import create_engine, Column, String, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

from ._utils import fix_string_columns_bug
from .constants import MUNDI_PATH
from .enums import Fill

PLUGIN_DB = {}
PLUGIN_INSTANCES = {}
EMPTY_DICT = MappingProxyType({})
TYPE_MAP = {int: sql.Integer, str: sql.String}


class Field(Record):
    """
    Describes a column.
    """

    name: str = None
    type: type = None
    aggregation: Fill = Fill.NONE
    plugin: "Plugin" = None
    index: pd.Index = None
    columns: pd.Index = None

    agg = Fill

    def __repr__(self):
        cls = self.type
        cls = f"{cls.__name__}, " if cls else "name="
        return f"Info({cls}{self.name!r})"

    def astype(self, type):
        """
        Return Info bound to given type.
        """
        return self.copy(type=type)

    def copy(self, data: Mapping = EMPTY_DICT, **kwargs):
        """
        Return copy of object, changing the given keyword arguments
        """
        return Field(**{**dict(self), **data, **kwargs})

    def update(self, data: Mapping = EMPTY_DICT, **kwargs):
        """
        Like copy, but only changes null values.
        """
        update = {}
        for k, v in {**data, **kwargs}.items():
            if getattr(self, k) in (None, Fill.NONE):
                update[k] = v
        return self.copy(update)

    def query(self, db, *args, **kwargs):
        """

        Args:
            db:
            *args:
            **kwargs:

        Returns:

        """

    def load(self, db, ref):
        """
        Load value for the given id.
        """

    def to_sql_column(self):
        if self.type is None:
            kind = sql.String
        else:
            kind = TYPE_MAP[self.type]
        return sql.Info(kind)


@sk.once
def engine():
    return create_engine(f"sqlite:///{path()}", echo=True)


@sk.once
def path():
    path = Path(os.path.expanduser("~/.local/lib/mundi/"))
    path.mkdir(parents=True, exist_ok=True)
    return path / "db.sqlite3"


@sk.once
def session_maker() -> sessionmaker:
    return sessionmaker(engine())


@lru_cache(2)
def create_tables(*, force=False):
    if path().exists() and force:
        path().unlink()
    return Base.metadata.create_all(engine())


def new_session() -> Session:
    return session_maker()()


def connection():
    return engine.connection()


def mundi_data(name, url=None) -> pd.DataFrame:
    """
    Return a dataframe with mundi data with the given name.

    If data is not found locally or if name is None, fallback to downloading
    from the given URL.
    """
    if name:
        for ext in (".pkl", ".pkl.gz", ".pkl.bz2"):
            path = MUNDI_PATH / "data" / (name + ext)
            if path.exists():
                break
        else:
            path = url
    else:
        path = url
    if not path:
        raise ValueError("name or valid url must be given")
    data = pd.read_pickle(path)
    return fix_string_columns_bug(data)


Base = declarative_base()


class MundiRegistry(Base):
    """
    Register mundi elements
    """

    __tablename__ = "mundi_registry"

    plugin_key = Column(String(64), primary_key=True)
    table_name = Column(String(64), primary_key=True)
    is_populated = Column(Boolean, default=False)

    @classmethod
    def has_populated_table(cls, plugin, table=None):
        """
        Return true if registry has populated tables.
        """
        if not table:
            tables = plugin.__tables__
            return all(cls.has_populated_table(plugin, table) for table in tables)

        key = getattr(plugin, "plugin_key", plugin)
        query = (
            new_session().query(MundiRegistry).filter_by(plugin_key=key, table_name=table)
        )
        return getattr(query.first(), "is_populated", False)


def MundiRef(key=None, primary_key=False, **kwargs):
    """
    Create a foreign key reference to a mundi id. This method fixes the
    correct data type for char-based foreign key relations.

    Args:
        key:
            Name of the table.column to point to as a ForeignKey relationship.
        primary_key:
            If True, declares field as a primary key column for database.

    Keyword Args:
        Keywords are forwarded to the Column() constructor.
    """
    if not key and not primary_key:
        raise TypeError("either key or primary key must be given!")
    if key:
        return Column(String(16), ForeignKey(key), **kwargs)
    return Column(String(16), primary_key=True, **kwargs)
