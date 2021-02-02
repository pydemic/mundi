"""
==================================
The mundi data processing pipeline
==================================

Types in this module are involved in fetching, cleaning, collecting and finally
saving data to destination database. Each step is performed by some specific
class.

A mundi plugin should typically subclass all of those classes and implement
specific logic.

1) Data: basic class responsible for fetching and transforming raw data to a
suitable dataframe. Usually, a plugin will define a base Data subclass that is
subclassed by scripts in the mundi-data repository. The code is organized this
way since typically different countries or regions of the world expose data in
different ways and in different places, so the job of a Data class is to gather
data from some specific location and convert it to a uniform representation
expected by the Mundi plugin.

2) Collector: dataframes produced by Data instances are stored in chunks inside
the build/chunks directory under mundi-data. A chunk usually contains data from an
specific country or region of the world and the complete database must be
build by concatenating all chunks and sometimes doing some additional
post-processing operations (e.g., imputation of missing data, data clean-up,
preparation to a final representation, etc). The final result of this operation
is a dataframe holding information to be inserted into an SQL database. The
results produced by a collector are stored as build/databases pickles into the
mundi-data repository and can be downloaded by users to avoid executing the
expensive operation described in the previous step.

3) Importer: importer task is to load a dataframe from a collector into an SQL
database.
"""
from .collector import Collector
from .data import Data, DataIO, find_prepare_scripts, execute_prepare_script
from .importer import Importer
