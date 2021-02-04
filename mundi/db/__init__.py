"""
4) Mapper: mapper is responsible to map mundi actions and queries into SQL
queries. It fetches information from the database and expose it in a suitable
form to the user.

5) Plugin: the plugin class coordinates all other classes. If finds data scripts
responsible for creating data chunks, calls the collector to produce the final
data frame and the importer to load data to the database.
"""

from .database import (
    Base,
    MundiRef,
    create_tables,
    connection,
    session,
    query,
    get_table,
    values_for,
)
from .tables import Region, RegionM2M, Demography, HistoricDemography, Healthcare
from .plugin import Plugin
