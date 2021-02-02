from functools import lru_cache
from typing import Callable

import sidekick.api as sk
from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, Query

from ..config import mundi_db_engine, mundi_db_path

table_registry = {}
Base = declarative_base(class_registry=table_registry)
BaseSession: Callable[[], Session] = sk.deferred(lambda: sessionmaker(mundi_db_engine()))


@lru_cache(2)
def create_tables(*, force=False):
    """
    Create all SQL tables.
    """
    if mundi_db_path().exists() and force:
        mundi_db_path().unlink()
    return Base.metadata.create_all(mundi_db_engine())


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


@lru_cache(32)
def get_table(name):
    """
    Return the ORM class from table name.
    """
    for table in table_registry.values():
        if hasattr(table, "__table__") and table.__table__.name == name:
            return table
    raise ValueError(f"no table registered as {name!r}")


def query(model, **kwargs) -> Query:
    """
    Query model with optional filter parameters.
    """
    if isinstance(model, str):
        model = get_table(model)
    q = session().query(model)
    if kwargs:
        q = q.filter_by(**kwargs)
    return q


def MundiRef(key=None, primary_key=False, **kwargs) -> Column:
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
    if key or kwargs.pop("foreign_key", False):
        return Column(String(16), ForeignKey(key), **kwargs)
    return Column(String(16), primary_key=True, **kwargs)
