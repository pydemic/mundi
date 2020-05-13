import re
from functools import lru_cache
from typing import Union

import numpy as np
import pandas as pd
import sidekick as sk

from .types import Region

db = sk.import_later(".db:db", package=__package__)
ISO2 = re.compile(r"[A-Z]{2}")
ISO3 = re.compile(r"[A-Z]{3}")
MUNDI_CODE = re.compile(r"[A-Z]{2}-\w+(:\w+)*")
TYPES_HIERARCHY = ["state", "city", "district", "region"]


def regions(country=None, **kwargs) -> pd.DataFrame:
    """
    Query the regions/sub-divisions database.
    """
    if country and "country_code" in kwargs:
        raise TypeError("cannot specify country and country_code")
    elif country:
        kwargs["country_code"] = country_code(country)
    return db.query(**kwargs)


def countries(**kwargs) -> pd.DataFrame:
    """
    Query the country database.
    """
    return regions(type="country", **kwargs)


def region(*args, country=None, **kwargs) -> Region:
    """
    Query the regions/sub-divisions database.
    """
    if country:
        kwargs["country_code"] = country_code(country)

    if args:
        (ref,) = args
        if isinstance(ref, Region):
            return ref
        row = db.get(code(ref))
    else:
        row = db.get(**kwargs)
    return Region(row.name)


@lru_cache(1024)
def country_code(code: str) -> str:
    """
    Return the country code for the given country.

    Similar to the code() function, but only accept valid countries.
    """
    if isinstance(code, Region):
        if code.type != "country":
            raise ValueError(f"region is not a country: {code}")
        return code.id

    if ISO2.fullmatch(code.upper()):
        try:
            db.get(code)
            return code.upper()
        except LookupError:
            pass

    elif ISO3.fullmatch(code.upper()):
        try:
            res = db.get(long_code=code.upper(), type="country")
            return res.name
        except LookupError:
            pass

    if code.isdigit():
        res = db.get(numeric_code=code, type="country")
        return res.name

    elif "/" not in code:
        res = db.get(name=code, type="country")
        return res.name

    raise LookupError(code)


@lru_cache(32_000)
def code(code: Union[Region, str]) -> str:
    """
    Return the mundi code for the given region.
    """
    if isinstance(code, Region):
        return code.id

    try:
        return country_code(code)
    except LookupError:
        pass

    if MUNDI_CODE.fullmatch(code.upper()):
        try:
            res = db.get(code)
            return res.name
        except LookupError:
            pass
        country, _, division = code.partition("-")

    elif "/" in code:
        country, _, division = code.partition("/")

    else:
        raise LookupError(code)

    country = country_code(country)
    return _subdivision_code(country, division)


@lru_cache(32_000)
def _subdivision_code(country: str, subdivision: str) -> str:
    """
    Return the mundi code for the given subdivision of a country.
    """
    if subdivision.isdigit():
        res = db.get(numeric_code=subdivision, country_code=country)
        return f"{country}-{res.name}"

    else:
        for lookup in ["short_code", "long_code"]:
            kwargs = {lookup: subdivision, "country_code": country}
            try:
                res = db.get(**kwargs)
                return f"{country}-{res.name}"
            except LookupError:
                pass

        values = db.query(
            country_code=country, name=subdivision, cols=("id", "type", "subtype")
        )

        if len(values) == 1:
            return values.index[0]
        elif len(values) > 1:
            pos = np.argsort([TYPES_HIERARCHY.index(x) for x in values["type"]])
            return values.index[pos[0]]
        else:
            raise LookupError(code)
