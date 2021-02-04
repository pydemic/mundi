Mundi data
==========

Data repository for Mundi. This stores raw data sets, cleaned and processed version of
data available to the builtin Mundi plugins. This repository is separated from the main
repository to avoid cluttering it with huge data files and impose potentially large downloads
to Mundi users.


How to contribute?
------------------

This project is currently under heavy development, hence you best bet is probably to just
contact the developers and see how you can help. In the future we may formalize a process
for new data contributions.


How is this repository organized?
---------------------------------

* **data:** The data folder contains raw data sets in various formats and Python scripts
  that convert those data sets into an uniform and predictable representation as a
  Pandas dataframe. Each Mundi plugin expects the final output to contain some specific
  columns of given types. Sub-folders are associated with specific regions of the World.
  They may contain scripts to process data for specific countries (which are identified by
  their ISO 2-letter code) and will usually contain a folder "XX" representing Planet
  Earth.
* **build:** After all processing steps, the results will be stored as pickled data frames
  in this folder. Files may be directly downloaded from here to initialize a Mundi
  database. Each data frame maps to a SQL table that Mundi uses to query data.
* **chunks:** This stores the intermediate results of data processing and usually creates one
  data frame per region per table in the data sub-folders. The final step in the data
  processing pipeline is to aggregate all those chunks into a single data frame.


Plugins and data sets
---------------------

The base Mundi package contains a few plugins, which are described bellow.

* **main**: Main plugin register regions and contains basic information such as
  identification codes, names, parent region, etc. This is the reference registry
  for all regions in the Mundi database. Other plugins store information that
  reference those region codes.
* **demography**: The demography plugin contains demographic information such as
  total population and age distribution.
* **healthcare**: Contains information about the number of regular hospital beds
   and ICUs in each region.