import re
from functools import lru_cache
from typing import Union

import numpy as np
import pandas as pd

from . import db
from .db.core import column_expressions
from .types import Region, RegionSet

ISO2 = re.compile(r"[A-Z]{2}")
ISO3 = re.compile(r"[A-Z]{3}")
MUNDI_CODE = re.compile(r"[A-Z]{2}-\w+(:\w+)*")
TYPES_HIERARCHY = ["state", "city", "district", "region"]
REGION_UNIVERSE = db.Universe.REGION
REGION_CACHE = {'XX', 'XAF', 'XAN', 'XAS', 'XEU', 'XOC', 'XNA', 'XSA'}
RegionModel = db.Region


def filter_region(*args):
    return db.session().query(RegionModel).filter(*args)


def filter_country(*args):
    return filter_region(*args, RegionModel.type == "country")


def regions(country=None, **kwargs) -> RegionSet:
    """
    Query the regions/sub-divisions database.
    """
    if country and "country_id" in kwargs:
        raise TypeError("cannot specify both 'country' and 'country_id'")
    elif country:
        kwargs["country_id"] = country_id(country)

    query = REGION_UNIVERSE.query(RegionModel, join='auto', **kwargs)
    return RegionSet(r[0] for r in query.values(RegionModel.id))


def regions_dataframe(cols=("name",), **kwargs) -> pd.DataFrame:
    """
    Query the regions/sub-divisions database, returning a dataframe.
    """
    query = REGION_UNIVERSE.query(RegionModel, join='auto', **kwargs)
    cols = column_expressions(REGION_UNIVERSE, cols)
    return pd.DataFrame(
        query.values(RegionModel.id, *(col.expression for col in cols))
    ).set_index("id")


def countries(**kwargs) -> RegionSet:
    """
    Query the country database.
    """
    return regions(type="country", **kwargs)


def countries_dataframe(cols=("name",), **kwargs) -> pd.DataFrame:
    """
    Query the regions/sub-divisions database, returning a dataframe.
    """
    return regions_dataframe(cols, type="country", **kwargs)


def region(*args, country=None, **kwargs) -> Region:
    """
    Query the regions/sub-divisions database.

    If called with a single string argument, create a region from id, if the
    given id is valid.
    """
    if country:
        kwargs["country_id"] = country_id(country)

    if args:
        (ref,) = args
        if isinstance(ref, Region):
            return ref
        return Region(code(ref))
    else:
        row = REGION_UNIVERSE.query(RegionModel, join='auto', **kwargs).first()
        if row is None:
            raise LookupError("not found")
    return Region(row.id)


# noinspection PyUnresolvedReferences
@lru_cache(1024)
def country_id(ref: str) -> str:
    """
    Return the country code for the given country.

    Similar to the code() function, but only accept valid countries.
    """
    m = RegionModel

    if isinstance(ref, Region):
        if ref.type != "country":
            raise ValueError(f"region is not a country: {ref}")
        return ref.id

    ref_ = ref.upper()
    if ISO2.fullmatch(ref_):
        try:
            row = db.session().query(RegionModel).get(ref_)
            if row.type == "country":
                return row.id
        except LookupError:
            pass

    elif ISO3.fullmatch(ref_):
        try:
            row = filter_country(m.long_code == ref_).first()
            if row is not None:
                return row.id
        except (LookupError, IndexError):
            pass

    if ref.isdigit():
        row = filter_country(m.numeric_code == ref).first()
        if row is not None:
            return row.id

    elif "/" not in ref:
        row = filter_country(m.name == ref).first()
        if row is not None:
            return row.id

    row = filter_country(m.name.ilike(f"%{ref}%")).first()
    if row is not None:
        return row.id

    raise LookupError(ref)


@lru_cache(32_768)
def code(ref: Union[Region, str]) -> str:
    """
    Return the mundi code for the given region.
    """
    if isinstance(ref, Region):
        return ref.id

    if ref in REGION_CACHE:
        return ref

    try:
        return country_id(ref)
    except LookupError:
        pass

    ref_ = ref.upper()
    if MUNDI_CODE.fullmatch(ref_):
        res = db.session().query(RegionModel).get(ref_)
        if res is not None:
            return res.id
        country, _, division = ref.partition("-")

    elif "/" in ref:
        country, _, division = ref.partition("/")

    else:
        raise LookupError(ref)

    country = country_id(country)
    return _subdivision_code(country, division)


# noinspection PyUnresolvedReferences
@lru_cache(32_768)
def _subdivision_code(country: str, subdivision: str) -> str:
    """
    Return the mundi code for the given subdivision of a country.
    """
    m = RegionModel
    country_query = m.country_id == country

    if subdivision.isdigit():
        res = filter_region(
            m.numeric_code == subdivision, country_query
        ).first()
        return f"{country}-{res.name}"

    else:
        for lookup in ["short_code", "long_code"]:
            filters = getattr(m, lookup) == subdivision, country_query
            try:
                res = filter_region(*filters).first()
                if res is not None:
                    return res.id
            except LookupError:
                pass

        values = pd.DataFrame(
            list(
                filter_region(country_query, m.name == subdivision)
                    .values(m.id, m.type, m.subtype)
            )
            or list(
                filter_region(
                    m.country_id == country, m.name.ilike(f"%{subdivision}%")
                ).values(m.id, m.type, m.subtype)
            )
        ).set_index("id")

        if len(values) == 1:
            return values.index[0]
        elif len(values) > 1:
            pos = np.argsort([TYPES_HIERARCHY.index(x) for x in values["type"]])
            return values.index[pos[0]]
        else:
            raise LookupError(code)
