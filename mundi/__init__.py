"""
Centralize data from countries and regions around the World and expose it
as Pandas dataframes.

Mundi has a plug-in system that expands the data sets to many other
specialized uses.
"""
__author__ = "Fábio Mendes"
__version__ = "0.2.3"

from . import db
from . import pandas as _pandas_module
from .constants import *
from .functions import (
    regions,
    countries,
    regions_dataframe,
    countries_dataframe,
    region,
    country_id,
    code,
)
from .logging import log
from .types import Region, RegionSet

db.Plugin.init_plugins()
