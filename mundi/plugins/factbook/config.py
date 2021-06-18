COUNTRIES = set(
    "AA BL CV GA IZ MD NZ SC TU AC BM CW GB JA MH PA SE TX AE BN DA GG JM MI PE SF TZ "
    "AF BO DJ GH JO MJ PK SG UK AG BR DO GM KE MK PL SH UP AJ BU DR GR KG MN PM SI US "
    "AL BX DX GT KS MO PO SL UV AM CA EC GV KT MP PP SN UY AO CB EE GY KU MR PS SP UZ "
    "AR CE EG HA KV MT PU SU VE AS CF EI HK KZ MU QA SV VI AU CH EN HO LA MV RI SW VM "
    "AX CI ER HR LE MX RM SY VQ BA CK ES HU LG MY RO SZ VT BB CM ET IC LH MZ RP TD WA "
    "BC CN EZ ID LI NH RQ TH WF BE CO FI IN LO NI RS TI WZ BF CQ FJ IR LS NL RW TO YM "
    "BG CS FM IS LU NO SA TP BH CT FR IT MA NP TS BK CU FS IV MC NU SB TT".split())

OCEANS = {
    'ZH',  # Atlantic Ocean
    'ZN',  # Pacific Ocean
    'OO',  # Southern Ocean
    'XQ',  # Arctic Ocean
    'XX',  # World
}

REGIONS = {*COUNTRIES, *OCEANS}