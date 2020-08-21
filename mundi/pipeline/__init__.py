"""
==================================
The mundi data processing pipeline
==================================

Types in this module are involved in fetching, cleaning, collecting and finally
saving data to destination database. Each step is performed by some specific
class.

A mundi plugin should typically subclass all of those classes and implement
specific logic

1) Data: basic class responsible for fetching and transforming raw data to a
suitable dataframe. Usually, a plugin will define a base Data subclass that is
subclassed by scripts in the mundi-data repository. The code is organized this
way since typically different countries or regions of the world expose data in
different ways and in different places, so the job of a Data class is to gather
data from some specific location and convert it to a uniform representation
expected by the Mundi plugin.

2) Collector: dataframes produced by Data instances are stored in chunks inside
the build/chunks directory of mundi-data. A chunk usually contains data from an
specific country or region of the world and the complete database must be
build by concatenating all chunks and sometimes doing some additional
post-processing operations (e.g., imputation of missing data, data clean-up,
preparation to a final representation, etc). The final result of this operation
is a dataframe holding information to be inserted into an SQL database. The
results produced by a collector are stored as assets into the mundi-data repository
and can be downloaded by users to avoid an expensive operation in the previous
step.

3) Importer: importer task is to load a dataframe from a collector into an SQL
database.

4) Mapper: mapper is responsible to map mundi actions and queries into SQL
queries. It fetches information from the database and expose it in a suitable
form to the user.

5) Plugin: the plugin class coordinates all other classes. If finds data scripts
responsible for creating data chunks, calls the collector to produce the final
data frame and the importer to load data to the database.
"""
from .collector import Collector
from .data import Data, DataIOMixin, DataValidationMixin, DataTransformMixin
from .importer import Importer
from .mapper import Mapper
from .plugin import Plugin
