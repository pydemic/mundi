from .region import REGION_PLUGINS, Region
from ..db import db


def region_mundi_plugin(db, attr):
    """
    Function that extracts attributes from db for the given region.
    """

    def plugin(region: Region):
        cur = db.raw_sql(f'SELECT id, {attr} FROM {{table}} WHERE id = "{region.id}";')
        try:
            (_, value) = next(iter(cur))
        except IndexError:
            return None
        return value

    plugin.__name__ = attr
    plugin.__qualname__ = attr
    return plugin


REGION_PLUGINS.update(
    name=region_mundi_plugin(db, "name"),
    type=region_mundi_plugin(db, "type"),
    subtype=region_mundi_plugin(db, "subtype"),
    short_code=region_mundi_plugin(db, "short_code"),
    long_code=region_mundi_plugin(db, "long_code"),
    numeric_code=region_mundi_plugin(db, "numeric_code"),
    country_code=region_mundi_plugin(db, "country_code"),
    parent_id=region_mundi_plugin(db, "parent_id"),
    alt_parents=region_mundi_plugin(db, "alt_parents"),
)
