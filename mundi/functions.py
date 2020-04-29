import re
from functools import lru_cache

import numpy as np
import sidekick as sk

db = sk.import_later(".db:db", package=__package__)
ISO2 = re.compile(r"[A-Z]{2}")
ISO3 = re.compile(r"[A-Z]{3}")
MUNDI_CODE = re.compile(r"[A-Z]{2}-\w+")
TYPES_HIERARCHY = [
    "region",
    "state",
    "federal district",
    "municipality",
    "meso-region",
    "micro-region",
    "district",
]


def regions(country=None, **kwargs):
    """
    Query the regions/sub-divisions database.
    """
    if country and "country_code" in kwargs:
        raise TypeError("cannot specify country and country_code")
    elif country:
        kwargs["country_code"] = country_code(country)
    return db.query(**kwargs)


def countries(**kwargs):
    """
    Query the country database.
    """
    return regions(type="country", **kwargs)


def region(*args, country=None, **kwargs):
    """
    Query the regions/sub-divisions database.
    """
    if country:
        kwargs["country_code"] = country_code(country)

    if args:
        (ref,) = args
        return db.get(code(ref))
    else:
        return db.get(**kwargs)


@lru_cache(1024)
def country_code(code):
    """
    Return the country code for the given country.

    Similar to the code() function, but only accept valid countries.
    """
    if ISO2.fullmatch(code.upper()):
        try:
            db.get(code)
            return code.upper()
        except LookupError:
            pass
    elif ISO3.fullmatch(code.upper()):
        try:
            return db.get(long_code=code.upper(), type="country").code
        except LookupError:
            pass
    elif code.isdigit():
        return db.get(numeric_code=code, type="country").code
    elif "/" not in code:
        return db.get(name=code, type="country").code
    else:
        raise LookupError(code)


@lru_cache(32_000)
def code(code):
    """
    Return the mundi code for the given region.
    """
    try:
        return country_code(code)
    except LookupError:
        pass

    if MUNDI_CODE.fullmatch(code.upper()):
        try:
            s = db.get(code.upper())
            return code.upper()
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
def _subdivision_code(country, subdivision):
    """
    Return the mundi code for the given subdivision of a country.
    """
    if subdivision.isdigit():
        entry = db.get(numeric_code=subdivision, country_code=country).code
        return country + "-" + entry
    else:
        for lookup in ["code", "long_code"]:
            kwargs = {lookup: subdivision, "country_code": country}
            try:
                return country + "-" + db.get(**kwargs).code
            except LookupError:
                pass

        values = db.query(country_code=country, name=subdivision)
        if len(values) == 1:
            return values.index[0]
        elif len(values) > 1:
            pos = np.argsort([TYPES_HIERARCHY.index(x) for x in values["type"]])
            return values.index[pos[0]]
        else:
            raise LookupError(code)
