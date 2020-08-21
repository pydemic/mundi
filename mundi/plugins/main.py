from abc import ABC

import pandas as pd
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship, backref

from ..database import Base, Field, Fill, MundiRef
from ..pipeline import Data, DataIOMixin, DataValidationMixin, DataTransformMixin, Plugin


#
# DB Models
#
class Region(Base):
    """
    Basic representation of Mundi regions.
    """

    __tablename__ = "region"

    id = MundiRef(primary_key=True, doc="Unique identifier for each region. Primary key.")
    name = Column(
        String, doc="Human-readable name of region (English version)", nullable=False
    )
    type = Column(String(32), doc="Region main type.", nullable=False)
    subtype = Column(String(32), doc="Optional Region sub-type.")
    short_code = Column(
        String(16), doc="Small identification code (uses ISO alpha-2 for countries)"
    )
    long_code = Column(
        String(32), doc="A longer identification code (uses ISO alpha-3 for countries)"
    )
    numeric_code = Column(
        String(32), doc="An optional numeric identification for region."
    )
    country_id = MundiRef("region.id", doc="Country id for regions within a country.")
    parent_id = MundiRef("region.id", doc="Reference to parent element.")
    income_group = Column(String(16), doc="UN income groups.")
    region = Column(String(16), doc="UN regions.")
    level = Column(
        Integer,
        doc="Level in the mundi hierarchy. Level starts at 0 = XX/World and "
        "increases by one at each additional nesting.",
    )

    # Relationships
    # parent = relationship("Region", remote_side=[id], foreign_keys=[parent_id])
    # country = relationship("Region", remote_side=[id], foreign_keys=[country_id])
    children = relationship(
        "Region", backref=backref("parent", remote_side=[id]), foreign_keys=[parent_id]
    )
    subdivisions = relationship(
        "Region", backref=backref("country", remote_side=[id]), foreign_keys=[country_id]
    )

    def __repr__(self):
        return f"<Region id={self.id!r}, name={self.name!r}>"


class RegionM2M(Base):
    """
    Alternative hierarchy of regions.
    """

    __tablename__ = "region_m2m"

    id = Column(Integer, primary_key=True)
    parent_id = MundiRef("region.id")
    child_id = MundiRef("region.id")
    relation = Column(String(32))

    # Relationships
    parent = relationship(
        "Region",
        backref=backref("parents_alt", remote_side="Region.id"),
        foreign_keys=[parent_id],
    )
    child = relationship(
        "Region",
        backref=backref("children_alt", remote_side="Region.id"),
        foreign_keys=[child_id],
    )


#
# Data Base class
#
class MainData(DataIOMixin, DataValidationMixin, DataTransformMixin, Data, ABC):
    """
    Validates data for the main plugin.
    """

    REGION_DATA_TYPES = {
        "name": "string",
        "type": "string",
        "subtype": "string",
        "short_code": "string",
        "numeric_code": "string",
        "long_code": "string",
        "country_id": "string",
        "parent_id": "string",
        "level": int,
        "region": "string",
        "income_group": "string",
    }
    REGION_M2M_DATA_TYPES = {
        "child_id": "string",
        "parent_id": "string",
        "relation": "string",
    }

    def fill_region(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Fill optional columns with empty values.
        """
        kwargs = {
            "short_code": (pd.NA, "string"),
            "numeric_code": (pd.NA, "string"),
            "long_code": (pd.NA, "string"),
            "parent_id": (pd.NA, "string"),
            "region": (pd.NA, "string"),
            "income_group": (pd.NA, "string"),
            **kwargs,
        }
        return self.assign(data, **kwargs)


#
# Plugin
#
class MainPlugin(Plugin):
    """
    Main data
    """

    __tables__ = {
        "region": Region,
        # 'region_m2m': RegionM2M,
    }
    __data_location__ = "http://github.com/pydemic/mundi-data/{table}.pkl.gz"

    name = Field()
    type = Field()
    subtype = Field()
    short_code = Field()
    long_code = Field()
    numeric_code = Field()
    country_id = Field()
    parent_id = Field()
    income_group = Field()
    region = Field(aggregation=Fill.INHERIT)


def pipeline(path, force=False):
    """
    Execute the main plugin pipeline.
    """

    plugin = MainPlugin()
    plugin.pipeline(path, force=force)
