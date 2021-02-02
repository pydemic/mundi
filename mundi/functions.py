import re
from functools import lru_cache
from typing import Union

import numpy as np
import pandas as pd

from . import db
from .types import Region, RegionSet

ISO2 = re.compile(r"[A-Z]{2}")
ISO3 = re.compile(r"[A-Z]{3}")
MUNDI_CODE = re.compile(r"[A-Z]{2}-\w+(:\w+)*")
TYPES_HIERARCHY = ["state", "city", "district", "region"]


def regions(country=None, **kwargs) -> RegionSet:
    """
    Query the regions/sub-divisions database.
    """
    if country and "country_id" in kwargs:
        raise TypeError("cannot specify both 'country' and 'country_id'")
    elif country:
        kwargs["country_id"] = country_id(country)
    return RegionSet(db.query(**kwargs).values("id"))


def regions_dataframe(cols=("name",), **kwargs) -> pd.DataFrame:
    """
    Query the regions/sub-divisions database, returning a dataframe.
    """
    return pd.DataFrame(db.query(**kwargs).values("id", *cols)).set_index("id")


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
    """
    if country:
        kwargs["country_id"] = country_id(country)

    if args:
        (ref,) = args
        if isinstance(ref, Region):
            return ref
        return Region(code(ref))
    else:
        row = db.query(**kwargs).first()
        if row is None:
            raise LookupError("not found")
    return Region(row.id)


@lru_cache(1024)
def country_id(ref: str) -> str:
    """
    Return the country code for the given country.

    Similar to the code() function, but only accept valid countries.
    """
    if isinstance(ref, Region):
        if ref.type != "country":
            raise ValueError(f"region is not a country: {ref}")
        return ref.id

    ref_ = ref.upper()
    if ISO2.fullmatch(ref_):
        try:
            res = db.query().get(ref_)
            if res.type == "country":
                return res.id
        except LookupError:
            pass

    elif ISO3.fullmatch(ref_):
        try:
            res = db.query(long_code=ref_, type="country").first()
            if res is not None:
                return res.id
        except (LookupError, IndexError):
            pass

    if ref.isdigit():
        res = db.query(numeric_code=ref, type="country").first()
        if res is not None:
            return res.id

    elif "/" not in ref:
        res = db.query(name=ref, type="country").first()
        if res is not None:
            return res.id

        res = db.query(type="country").filter(db.Region.name.ilike(f"%{ref}%")).first()
        if res is not None:
            return res.id

    raise LookupError(ref)


@lru_cache(32_768)
def code(ref: Union[Region, str]) -> str:
    """
    Return the mundi code for the given region.
    """
    if isinstance(ref, Region):
        return ref.id

    try:
        return country_id(ref)
    except LookupError:
        pass

    ref_ = ref.upper()
    if MUNDI_CODE.fullmatch(ref_):
        res = db.query().get(ref_)
        if res is not None:
            return res.id
        country, _, division = ref.partition("-")

    elif "/" in ref:
        country, _, division = ref.partition("/")

    else:
        raise LookupError(ref)

    country = country_id(country)
    return _subdivision_code(country, division)


@lru_cache(32_768)
def _subdivision_code(country: str, subdivision: str) -> str:
    """
    Return the mundi code for the given subdivision of a country.
    """
    if subdivision.isdigit():
        res = db.query(numeric_code=subdivision, country_id=country).first()
        return f"{country}-{res.name}"

    else:
        for lookup in ["short_code", "long_code"]:
            kwargs = {lookup: subdivision, "country_id": country}
            try:
                res = db.query(**kwargs).first()
                if res is not None:
                    return res.id
            except LookupError:
                pass

        values = list(
            db.query(country_id=country, name=subdivision).values("id", "type", "subtype")
        ) or list(
            db.query(country_id=country)
            .filter(db.Region.name.ilike(f"%{subdivision}%"))
            .values("id", "type", "subtype")
        )

        values = pd.DataFrame(values).set_index("id")
        if len(values) == 1:
            return values.index[0]
        elif len(values) > 1:
            pos = np.argsort([TYPES_HIERARCHY.index(x) for x in values["type"]])
            return values.index[pos[0]]
        else:
            raise LookupError(code)
