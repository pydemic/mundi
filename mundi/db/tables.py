from sqlalchemy import Column, String, Integer, Boolean, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, backref

from ..db import Base, MundiRef


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


#
# Main region tables
#
class Region(Base):
    """
    Basic representation of Mundi regions.
    """

    __tablename__ = "region"

    id = MundiRef(primary_key=True, doc="Unique identifier for each region. Primary key.")
    name = Column(
        String, doc="Human-readable name of region (English version).", nullable=False
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
    __table_args__ = (
        PrimaryKeyConstraint("parent_id", "child_id", "relation", name="pk"),
    )

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
