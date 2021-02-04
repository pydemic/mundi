from collections import defaultdict
from functools import lru_cache
from typing import Callable, Type

import sidekick.api as sk
from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, Query

from ..config import mundi_db_engine, mundi_db_path

create_tables_executed = False
table_registry = {}
Base = declarative_base(class_registry=table_registry)
BaseSession: Callable[[], Session] = sk.deferred(lambda: sessionmaker(mundi_db_engine()))


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
    if not primary_key:
        kwargs.setdefault("index", True)
    else:
        kwargs["primary_key"] = True

    if key is not None:
        return Column(String(16), ForeignKey(key), **kwargs)
    return Column(String(16), **kwargs)


def create_tables(*, force=False):
    """
    Create all SQL tables.
    """
    global create_tables_executed

    if mundi_db_path().exists() and force:
        mundi_db_path().unlink()
    if force or not create_tables_executed:
        Base.metadata.create_all(mundi_db_engine())
        create_tables_executed = True


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


def query(model=None, *args, **kwargs) -> Query:
    """
    Query model with optional filter parameters.
    """
    if isinstance(model, type) and issubclass(model, Base):
        models = (model,)
    elif model is None:
        if kwargs:
            models = set(_get_table_from_query_param(k) for k in kwargs)
        else:
            models = (get_table("region"),)
    elif isinstance(model, str):
        models = (get_table(model),)
    elif isinstance(model, (list, tuple)):
        models = (get_table(m) for m in model)
    else:
        raise TypeError(f"invalid model: {type(model)}")

    q = session().query(*models)
    if args or kwargs:
        args = [*args]
        for k, v in kwargs.items():
            fn = _get_expr_function_from_query_param(k)
            args.append(fn(v))
        q = q.filter(*args)
    return q


def values_for(ids, *fields, null=None):
    """
    Iterate over the values of the given fields for the given ids.
    """

    if not fields:
        raise TypeError("must specify at east one field")

    dispatch = defaultdict(list)
    ordering = []
    for f in fields:
        cls = _get_table_from_query_param(f)
        cls_fields = dispatch[cls]
        cls_fields.append(f)
        ordering.append((cls, len(cls_fields) - 1))

    if len(dispatch) == 1:
        model, fields = dispatch.popitem()
        yield from _values_for(ids, model, fields, null=null)
    else:
        parts = {
            model: _values_for(ids, model, fields) for model, fields in dispatch.items()
        }
        while True:
            try:
                row = {k: next(v) for k, v in parts.items()}
                yield tuple(row[cls][i] for cls, i in ordering)
            except StopIteration:
                break
            except KeyError as ex:
                raise RuntimeError(locals())


def _values_for(ids, model, fields, null=None):
    if not fields:
        raise TypeError("must specify at east one field")

    if model is Region:
        q = query(Region, Region.id.in_(ids))
    else:
        q = query(Region, Region.id.in_(ids)).join(model, model.id == Region.id)

    rows = {row[0]: row[1:] for row in q.values(Region.id, *fields)}
    for id in ids:
        try:
            yield rows[id]
        except KeyError:
            yield (null,) * len(fields)


@lru_cache(512)
def _get_table_from_query_param(param: str) -> Type[Base]:
    """
    Return the table name from the given query param string.
    """
    assert isinstance(param, str), param

    param, _, _ = param.partition("__")
    column = Plugin.get_column(param)
    table = column.table
    for model in table_registry.values():
        if getattr(model, "__table__", None) is table:
            return model
    else:
        raise RuntimeError("unexpected column.")


@lru_cache(512)
def _get_expr_function_from_query_param(param: str):
    column = _get_column_from_query_param(param)
    return lambda v: column == v


@lru_cache(512)
def _get_column_from_query_param(param: str):
    from .plugin import Plugin

    return Plugin.get_column(param)


def clear_caches():
    """
    Clear all LRU caches.
    """
    get_table.cache_clear()
    _get_table_from_query_param.cache_clear()
    _get_expr_function_from_query_param.cache_clear()
    _get_column_from_query_param.cache_clear()
    get_transformer.cache_clear()


@lru_cache(512)
def get_transformer(param: str):
    param, sep, extra = param.partition("__")
    if sep:
        raise NotImplementedError
    return Plugin.get_transformer(param)


from .tables import Region
from .plugin import Plugin
