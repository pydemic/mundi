"""
4) Mapper: mapper is responsible to map mundi actions and queries into SQL
queries. It fetches information from the database and expose it in a suitable
form to the user.

5) Plugin: the plugin class coordinates all other classes. If finds data scripts
responsible for creating data chunks, calls the collector to produce the final
data frame and the importer to load data to the database.
"""

from .core import (
    Universe,
    TableInfo,
    Table,
    session,
    connection,
    Column,
    SqlColumn,
    HDF5Column,
    create_tables,
)
from .plugin import Plugin
from .tables import (
    Region,
    RegionM2M,
    Demography,
    HistoricDemography,
    Healthcare,
    AgeDistributionsInfo,
    HistoricAgeDistributions,
    mundi_ref,
)
