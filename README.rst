=====
Mundi
=====

Mundi is a simple package that provides information about all countries in the world as
as a convenient set of classes and Pandas dataframes. It uses information provided by the
popular pycountry package and supplement it with several other data sources using
plugins.

The basic data in the mundi package is centered around a dataframe describing many world
regions with the following structure:

+==============+===========================================================================================+
| Column       | Description                                                                               |
+--------------+-------------------------------------------------------------------------------------------+
| id (index)   | Dataframe indexes are strings and correspond to the ISO code of a region, when available. |
+--------------+-------------------------------------------------------------------------------------------+
| name         | Region name in English                                                                    |
+--------------+-------------------------------------------------------------------------------------------+
| type         | Type of region. There are too many types to list here, but it will be something like      |
|              | "country", "state", "municipality", etc.                                                  |
+--------------+-------------------------------------------------------------------------------------------+
| code         | Short code for region. Those are unique in the same country, but may repeat elsewhere.    |
|              | For Countries, this is the ISO alpha-2 code.                                              |
+--------------+-------------------------------------------------------------------------------------------+
| numeric_code | Numeric code for region, when it exists. ISO assign a numeric code to each country and    |
|              | the official geographical bureau of each country frequently works with numerical codes    |
|              | too. Mundi will try to use those codes whenever possible, or will leave this column empty |
|              | when no numerical convention is available.                                                |
+--------------+-------------------------------------------------------------------------------------------+
| long_code    | Alternative long version of the code. For countries, this is the ISO alpha-3 code.        |
|              | Other sub-regions may optionally leave this column empty.                                 |
+--------------+-------------------------------------------------------------------------------------------+
| country_code | Country code for the selected region. If region is a country, this column is empty.       |
+--------------+-------------------------------------------------------------------------------------------+
| parent_id    | The id string for the parent element. Countries are considered to be root elements and    |
|              | therefore do not fill this column. The parent might be an intermediate region between     |
|              | the current row and the corresponding country. A city, for instance, may have a parent    |
|              | state, which have a parent country.                                                       |
+--------------+-------------------------------------------------------------------------------------------+

Usage
=====

Install Mundi using ``pip install mundi`` or your method of choice. Now, you can just import
it and load the desired information. Mundi exposes collections of entries as dataframes,
and single entries (rows in those dataframes) as Series objects.

>>> import mundi
>>> df = mundi.countries(); df
                    name     type code numeric_code long_code country_code parent_id
id
AD               Andorra  country   AD          020       AND         <NA>      <NA>
AE  United Arab Emirates  country   AE          784       ARE         <NA>      <NA>
AF           Afghanistan  country   AF          004       AFG         <NA>      <NA>
AG   Antigua and Barbuda  country   AG          028       ATG         <NA>      <NA>
AI              Anguilla  country   AI          660       AIA         <NA>      <NA>
..                   ...      ...  ...          ...       ...          ...       ...
YE                 Yemen  country   YE          887       YEM         <NA>      <NA>
YT               Mayotte  country   YT          175       MYT         <NA>      <NA>
ZA          South Africa  country   ZA          710       ZAF         <NA>      <NA>
ZM                Zambia  country   ZM          894       ZMB         <NA>      <NA>
ZW              Zimbabwe  country   ZW          716       ZWE         <NA>      <NA>
<BLANKLINE>
[255 rows x 7 columns]

The ``mundi.countries()`` function is just an alias to ``mundi.regions(type="country")``.
The more generic ``mundi.region()`` function may be used to query countries and
subdivisions inside a country.

>>> br_states = mundi.regions(country="BR", type="state"); br_states
                      name   type code numeric_code long_code country_code parent_id
id
BR-AC                 Acre  state   AC           12      <NA>           BR      BR-1
BR-AL              Alagoas  state   AL           27      <NA>           BR      BR-2
BR-AM             Amazonas  state   AM           13      <NA>           BR      BR-1
BR-AP                Amapá  state   AP           16      <NA>           BR      BR-1
BR-BA                Bahia  state   BA           29      <NA>           BR      BR-2
BR-CE                Ceará  state   CE           23      <NA>           BR      BR-2
BR-DF     Distrito Federal  state   DF           53      <NA>           BR      BR-5
BR-ES       Espírito Santo  state   ES           32      <NA>           BR      BR-3
BR-GO                Goiás  state   GO           52      <NA>           BR      BR-5
BR-MA             Maranhão  state   MA           21      <NA>           BR      BR-2
BR-MG         Minas Gerais  state   MG           31      <NA>           BR      BR-3
BR-MS   Mato Grosso do Sul  state   MS           50      <NA>           BR      BR-5
BR-MT          Mato Grosso  state   MT           51      <NA>           BR      BR-5
BR-PA                 Pará  state   PA           15      <NA>           BR      BR-1
BR-PB              Paraíba  state   PB           25      <NA>           BR      BR-2
BR-PE           Pernambuco  state   PE           26      <NA>           BR      BR-2
BR-PI                Piauí  state   PI           22      <NA>           BR      BR-2
BR-PR               Paraná  state   PR           41      <NA>           BR      BR-4
BR-RJ       Rio de Janeiro  state   RJ           33      <NA>           BR      BR-3
BR-RN  Rio Grande do Norte  state   RN           24      <NA>           BR      BR-2
BR-RO             Rondônia  state   RO           11      <NA>           BR      BR-1
BR-RR              Roraima  state   RR           14      <NA>           BR      BR-1
BR-RS    Rio Grande do Sul  state   RS           43      <NA>           BR      BR-4
BR-SC       Santa Catarina  state   SC           42      <NA>           BR      BR-4
BR-SE              Sergipe  state   SE           28      <NA>           BR      BR-2
BR-SP            São Paulo  state   SP           35      <NA>           BR      BR-3
BR-TO            Tocantins  state   TO           17      <NA>           BR      BR-1


If you want a single country or single region, use the function

>>> br = mundi.region("BR")
name             Brazil
type            country
code                 BR
numeric_code        076
long_code           BRA
country_code       <NA>
parent_id          <NA>
Name: BR, dtype: object


The library creates a custom ``.mundi`` accessor that exposes additional
methods not present in regular data frames. The most important of those is
the ability to extend the data frame with additional columns available from Mundi
itself or from plugins.

>>> extra = df.mundi["region", "income_group"]; extra
                region  income_group
id
AD              europe          high
AE         middle-east          high
AF          south-asia           low
AG       latin-america          high
AI                 NaN           NaN
..                 ...           ...
YE         middle-east           low
YT                 NaN           NaN
ZA  sub-saharan-africa  upper-middle
ZM  sub-saharan-africa  lower-middle
ZW  sub-saharan-africa  lower-middle
<BLANKLINE>
[255 rows x 2 columns]

It is also possible to keep the columns of the original dataframe using
the ellipisis syntax

>>> df = df.mundi[..., "region", "income_group"]; df
                    name     type code numeric_code long_code country_code parent_id              region  income_group
id
AD               Andorra  country   AD          020       AND         <NA>      <NA>              europe          high
AE  United Arab Emirates  country   AE          784       ARE         <NA>      <NA>         middle-east          high
AF           Afghanistan  country   AF          004       AFG         <NA>      <NA>          south-asia           low
AG   Antigua and Barbuda  country   AG          028       ATG         <NA>      <NA>       latin-america          high
AI              Anguilla  country   AI          660       AIA         <NA>      <NA>                 NaN           NaN
..                   ...      ...  ...          ...       ...          ...       ...                 ...           ...
YE                 Yemen  country   YE          887       YEM         <NA>      <NA>         middle-east           low
YT               Mayotte  country   YT          175       MYT         <NA>      <NA>                 NaN           NaN
ZA          South Africa  country   ZA          710       ZAF         <NA>      <NA>  sub-saharan-africa  upper-middle
ZM                Zambia  country   ZM          894       ZMB         <NA>      <NA>  sub-saharan-africa  lower-middle
ZW              Zimbabwe  country   ZW          716       ZWE         <NA>      <NA>  sub-saharan-africa  lower-middle
<BLANKLINE>
[255 rows x 9 columns]


The ``.mundi`` accessor is also able to select countries over mundi columns,
even if those columns are not in the original dataframe.

>>> countries = mundi.countries()
>>> countries.mundi.select(income_group="upper-middle")
                       name     type code numeric_code long_code country_code parent_id
id
AD                  Andorra  country   AD          020       AND         <NA>      <NA>
AE     United Arab Emirates  country   AE          784       ARE         <NA>      <NA>
AG      Antigua and Barbuda  country   AG          028       ATG         <NA>      <NA>
AT                  Austria  country   AT          040       AUT         <NA>      <NA>
AU                Australia  country   AU          036       AUS         <NA>      <NA>
..                      ...      ...  ...          ...       ...          ...       ...
TT      Trinidad and Tobago  country   TT          780       TTO         <NA>      <NA>
US            United States  country   US          840       USA         <NA>      <NA>
UY                  Uruguay  country   UY          858       URY         <NA>      <NA>
VG  Virgin Islands, British  country   VG          092       VGB         <NA>      <NA>
VI     Virgin Islands, U.S.  country   VI          850       VIR         <NA>      <NA>
<BLANKLINE>
[76 rows x 7 columns]
