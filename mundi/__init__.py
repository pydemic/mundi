"""
Centralize data from countries and regions around the World and expose it
as Pandas dataframes.

Mundi has a plug-in system that expands the data sets to many other
specialized uses.
"""
__author__ = "Fábio Mendes"
__version__ = "0.2.3"

from . import pandas as _pandas_mod
from .functions import regions, countries, region, country_code, code
from .types import Region
from .constants import *
