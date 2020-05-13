=====
Mundi
=====

Mundi is a simple package that provides information about all countries in the world as
as a convenient set of classes and Pandas dataframes. It uses information provided by the
popular pycountry package and supplement it with several other data sources using
plugins.

Warning!
========

Mundi is still in an early stage of development and thus is changing very quickly. New users
should expect some risks in terms of API changes and general breakage. We suggest that if you
want to take that risk, install it from git and keep in touch with the developers (and better yet,
contribute to the project).

Usage
=====

Install Mundi using ``pip install mundi`` or your method of choice. Now, you can just import
it and load the desired information. Mundi exposes collections of entries as dataframes,
and single entries (rows in those dataframes) as Series objects.

>>> import mundi
>>> df = mundi.countries(); df  # DOCTEST: +ELLIPSIS
                    name
id
AD               Andorra
AE  United Arab Emirates
AF           Afghanistan
AG   Antigua and Barbuda
AI              Anguilla
...

The ``mundi.countries()`` function is just an alias to ``mundi.regions(type="country")``.
The more generic ``mundi.region()`` function may be used to query countries and
subdivisions inside a country.

>>> br_states = mundi.regions(country="BR", type="state"); br_states  # DOCTEST: +ELLIPSIS
                      name
id
BR-AC                 Acre
BR-AL              Alagoas
BR-AM             Amazonas
BR-AP                Amapá
BR-BA                Bahia
...

If you want a single country or single region, use the ``mundi.region()`` function,
which returns a Region object, that in many ways behave like a row of a dataframe.

>>> br = mundi.region("BR"); br
Region("BR", name="Brazil")

The library creates a custom ``.mundi`` accessor that exposes additional
methods not present in regular data frames. The most important of those is
the ability to extend the data frame with additional columns available from Mundi
itself or from plugins.

>>> extra = df.mundi[["region", "income_group"]]; extra   # DOCTEST: +ELLIPSIS
                region  income_group
id
AD              europe          high
AE         middle-east          high
AF          south-asia           low
AG       latin-america          high
AI                 NaN           NaN
...

Each region also exhibit those values as attributes

>>> br.region
'latin-america'
>>> br.income_group
'upper-middle'

It is also possible to keep the columns of the original dataframe using
the ellipsis syntax

>>> df = df.mundi[..., "region", "income_group"]; df    # DOCTEST: +ELLIPSIS
                    name         region  income_group
id
AD               Andorra         europe          high
AE  United Arab Emirates    middle-east          high
AF           Afghanistan     south-asia           low
AG   Antigua and Barbuda  latin-america          high
AI              Anguilla            NaN           NaN
...


The ``.mundi`` accessor is also able to select countries over mundi columns,
even if those columns are not in the original dataframe.

>>> countries = mundi.countries()
>>> countries.mundi.filter(income_group="upper-middle")  # DOCTEST: +ELLIPSIS
                       name
id
AD                  Andorra
AE     United Arab Emirates
AG      Antigua and Barbuda
AT                  Austria
AU                Australia
...


Information
===========

The basic data in the mundi package is centered around a table describing many world
regions with the following structure:

+---------------+-------------------------------------------------------------------------------------------+
|    Column     |                                        Description                                        |
+===============+===========================================================================================+
| id (index)    | Dataframe indexes are strings and correspond to the ISO code of a region, when available. |
+---------------+-------------------------------------------------------------------------------------------+
| name          | Region name in English                                                                    |
+---------------+-------------------------------------------------------------------------------------------+
| type          | Type of region. There are too many types to list here, but it will be something like      |
|               | "country", "state", "municipality", etc.                                                  |
+---------------+-------------------------------------------------------------------------------------------+
| subtype       | A sub-division of the given type (e.g. a state can also be a "federal district")          |
+---------------+-------------------------------------------------------------------------------------------+
| short_code    | Short code for region. Those are unique in the same country, but may repeat elsewhere.    |
|               | For Countries, this is the ISO alpha-2 code.                                              |
+---------------+-------------------------------------------------------------------------------------------+
| long_code     | Alternative long version of the code. For countries, this is the ISO alpha-3 code.        |
|               | Other sub-regions may optionally leave this column empty.                                 |
+---------------+-------------------------------------------------------------------------------------------+
| numeric_code  | Numeric code for region, when it exists. ISO assign a numeric code to each country and    |
|               | the official geographical bureau of each country frequently works with numerical codes    |
|               | too. Mundi will try to use those codes whenever possible, or will leave this column empty |
|               | when no numerical convention is available.                                                |
+---------------+-------------------------------------------------------------------------------------------+
| country_code  | Country code for the selected region. If region is a country, this column is empty.       |
+---------------+-------------------------------------------------------------------------------------------+
| parent_id     | The id string for the parent element. Countries are considered to be root elements and    |
|               | therefore do not fill this column. The parent might be an intermediate region between     |
|               | the current row and the corresponding country. A city, for instance, may have a parent    |
|               | state, which have a parent country.                                                       |
+---------------+-------------------------------------------------------------------------------------------+
| alt_parents   | List of ids separated by semi-colons with alternative parents that do not belong to the   |
|               | main hierarchy.                                                                           |
+---------------+-------------------------------------------------------------------------------------------+
| income_group  | Country classification according to UN's income groups.                                   |
+---------------+-------------------------------------------------------------------------------------------+
| region        | Region of the globe according to UN's classification.                                     |
+---------------+-------------------------------------------------------------------------------------------+
