=====
Mundi
=====

Mundi is a simple package that provides information about countries and sub-regions with a convenient
set of classes and Pandas integration. It uses information provided by the
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

Install Mundi using ``pip install mundi`` or your preferred method of choice. Now, import
it and load the desired information. Mundi exposes collections of entries as RegionSet's,
dataframes, and single entries Region objects.

For instance, we can get the list of all countries in the world using the mundi.countries() function.

>>> import mundi
>>> mundi.countries()
RegionSet({'AD', 'AE', 'AF', 'AG', ...})

The resulting object, a region set, is a set-like structure with the list all matched
regions. The :func:`mundi.countries` function accept filtering using keyword arguments,
like the query bellow that fetches all South American countries (XSA is the code for
South America).

>>> countries = mundi.countries(parent_id='XSA')

The resulting RegionSet can be easily queried and converted to dataframes. We can,
for instance, aggregate total population or find the region with the largest or
smallest populations quite easily.

>>> countries.aggregate('population')
453164000
>>> countries.max('population')
Region('BR', name='Brazil')

It is often convenient to tabulate data in a RegionSet to a dataframe.
The ``to_dataframe`` method creates a table with the selected columns,
generating the corresponding dataframe.

>>> (countries.to_dataframe(['name', 'population'])
...     .dropna()
...     .sort_values('population', ascending=False)
... )  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                                 name   population
id
BR                             Brazil  226817000.0
CO                           Colombia   53035000.0
AR                          Argentina   45305000.0
PE                               Peru   34726000.0
VE  Venezuela, Bolivarian Republic of   31022000.0
CL                              Chile   20134000.0
EC                            Ecuador   17646000.0
BO    Bolivia, Plurinational State of   11687000.0
PY                           Paraguay    7219000.0
UY                            Uruguay    3682000.0
GY                             Guyana     975000.0
SR                           Suriname     616000.0
GF                      French Guiana     300000.0

Individual regions can be fetched from a RegionSet by indexing it with
its unique string identifier.

>>> countries["BR"]
Region('BR', name='Brazil')

More generally, they can also be queried with the ``mundi.region()``
function, either by providing an specific country code or a more
complex query.

>>> br = mundi.region(parent_id="XSA", population__gt=200_000_000); br
Region('BR', name='Brazil')

Properties are accessed either as keys or as attributes

>>> br.region == br['region'] == 'latin-america'
True
>>> br.population
226817000


Using dataframes
================

>>> df = mundi.countries_dataframe(); df  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                name
id
AW              Aruba
AF        Afghanistan
AO             Angola
AI           Anguilla
AX      Åland Islands
...
[255 rows x 1 columns]

The ``mundi.country_dataframe()`` function is just an alias to ``mundi.regions_dataframe(type="country")``.
The more generic ``mundi.regions_dataframe()`` function may be used to query countries and
subdivisions inside a country.

>>> br_states = mundi.regions_dataframe(country_id="BR", type="state"); br_states  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                      name
id
BR-AC                 Acre
BR-AL              Alagoas
BR-AM             Amazonas
BR-AP                Amapá
BR-BA                Bahia
...


The library creates a custom ``.mundi`` accessor that exposes additional
methods not present in regular data frames. The most important of those is
the ability to extend the data frame with additional columns available from Mundi
itself or from plugins.

>>> extra = df.mundi[["region", "population"]]; extra   # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                    region  population
id
AW           latin-america    126000.0
AF              south-asia  38929000.0
AO      sub-saharan-africa  32868000.0
AI                    None         NaN
AX                    None         NaN
...
[255 rows x 2 columns]


It is also possible to keep the columns of the original dataframe using
the ellipsis syntax

>>> df = df.mundi[..., ["region", "population"]]; df    # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                 name              region  population
id
AW              Aruba       latin-america    126000.0
AF        Afghanistan          south-asia  38929000.0
AO             Angola  sub-saharan-africa  32868000.0
AI           Anguilla                None         NaN
AX      Åland Islands                None         NaN
...
[255 rows x 3 columns]


The ``.mundi`` accessor is also able to select countries over mundi columns,
even if those columns are not in the original dataframe.

>>> countries = mundi.countries_dataframe()
>>> countries.mundi.filter(region="latin-america")  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                                name
id
AW                              Aruba
AR                          Argentina
AG                Antigua and Barbuda
BS                            Bahamas
BZ                             Belize
BO    Bolivia, Plurinational State of
BR                             Brazil
...


Information
===========

The basic data in the mundi package is centered around a table describing many world
regions with the following structure:

+---------------+-------------------------------------------------------------------------------------------+
|    Column     |                                        Description                                        |
+===============+===========================================================================================+
| id  (index)   | Dataframe indexes are strings and correspond to the ISO code of a region, when available. |
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
| country_id    | Country code for the selected region. If region is a country or continent, this column is |
|               | empty.                                                                                    |
+---------------+-------------------------------------------------------------------------------------------+
| parent_id     | The id string for the parent element. Countries are considered to be root elements and    |
|               | therefore do not fill this column. The parent might be an intermediate region between     |
|               | the current row and the corresponding country. A city, for instance, may have a parent    |
|               | state, which have a parent country.                                                       |
+---------------+-------------------------------------------------------------------------------------------+
| level         | Hierarchical level starting with 0 = world, 1 = continent, 2 = country.                   |
+---------------+-------------------------------------------------------------------------------------------+
| region        | Region of the globe according to UN's classification.                                     |
+---------------+-------------------------------------------------------------------------------------------+
