import io
import time
import os
from datetime import datetime
from functools import lru_cache
from urllib.error import HTTPError

import numpy as np
import pandas as pd
import requests

import mundi
from sidekick.functions import retry
from sidekick.cache import ttl_cache

from ...impute import impute_subregions
from ...logging import log
from .common import init_cache

HOURS = 3600
TIMEOUT = 6 * HOURS
EPIDEMIC_CURVES_APIS = {}
MOBILITY_DATA_APIS = {}
init_cache()


def epidemic_curve_api(key):
    return lambda fn: EPIDEMIC_CURVES_APIS.setdefault(key, fn)


def mobility_data_api(key):
    return lambda fn: MOBILITY_DATA_APIS.setdefault(key, fn)


#
# Epidemic curves
#
def epidemic_curve(region, api="auto", extra=False, **kwargs):
    """
    Universal interface to all epidemic curve loaders.

    Always return a dataframe with ["cases", "deaths"] columns for the given
    region. Some API's may offer additional columns such as "recovered", "test"
    etc.
    """
    code = mundi.code(region)
    fn = EPIDEMIC_CURVES_APIS[api]
    data = fn(code, **kwargs)
    return data if extra else data[["cases", "deaths"]]


@epidemic_curve_api("auto")
def auto_api(code, **kwargs):
    """
    Select best API to load according to region code.
    """
    if code == "BR" or code.startswith("BR-"):
        return brasil_io(code, **kwargs)
    elif len(code) == 2:
        return corona_api(code, **kwargs)
    raise ValueError(f"no API can load region with code: {code}")


@epidemic_curve_api("corona-api.com")
@ttl_cache("covid-19", timeout=TIMEOUT)
def corona_api(code) -> pd.DataFrame:
    """
    Load country's cases, deaths and recovered timeline from corona-api.com.
    """

    data = download_corona_api(code)
    data = data["data"]["timeline"]
    df = pd.DataFrame(data).rename({"confirmed": "cases"}, axis=1)

    df = df[["date", "cases", "deaths", "recovered"]]
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates("date", keep="first").set_index("date")
    df = df[df.fillna(0).sum(1) > 0].sort_index()

    # Fill missing data with previous measurement
    start, end = df.index[[0, -1]]
    full_index = pd.to_datetime(np.arange((end - start).days), unit="D", origin=start)
    df = df.reindex(full_index).fillna(method="ffill")

    return df.astype(int)


@retry(10, sleep=0.5)
def download_corona_api(code) -> dict:
    log.info(f"[api/corona-api] Downloading data from corona API ({code})")

    url = "http://corona-api.com/countries/{code}?include=timeline"
    response = requests.get(url.format(code=code))
    size = len(response.content) // 1024

    log.info(f"[api/corona-api] Download ended with {size} kb")
    return response.json()


@epidemic_curve_api("brasil.io")
def brasil_io(code, path=None):
    return brasil_io_cases(path=path).loc[code]


@ttl_cache("covid-19", timeout=TIMEOUT)
def brasil_io_cases(path=None) -> pd.DataFrame:
    """
    Return the complete dataframe of cases and deaths from Brasil.io.
    """

    if path is None:
        df = download_brasil_io_cases()
    else:
        log.info(f"[api/brasil.io] Reading local file {path}")
        df = pd.read_csv(os.path.expanduser(path))

    cases = brasil_io_clean_cases(df)
    log.info("[api/brasil.io] Merging data")
    return (
        pd.concat([cases, impute_subregions(cases, "BR", "date", level=7)])
        .set_index(["id", "date"])
        .sort_index()
    )


@ttl_cache("covid-19", timeout=TIMEOUT)
@retry(10, sleep=0.5)
def download_brasil_io_cases():
    log.info("[api/brasil.io] Downloading data from Brasil.io")

    url = "https://data.brasil.io/dataset/covid19/caso_full.csv.gz"
    try:
        # Brasil.io is now under a Cloudflare CDN and it requires proper
        # User-Agent headers. This means we cannot download data using pandas
        # builtin support for URLs in read_csv, since it does not set those
        # headers accordingly.
        response = requests.get(url, headers={"User-Agent": "python-requests"})
    except HTTPError as e:
        log.warn(f"[api/brasil.io] error downloading: {e}, using Github fallback")
        url = "https://github.com/pydemic/databases/raw/master/caso_full.csv.gz"
        return pd.read_csv(url)
    else:
        return pd.read_csv(io.BytesIO(response.content), compression="gzip")


def brasil_io_clean_cases(data: pd.DataFrame):
    """
    Clean brasil.io data.
    """

    cols = {
        "last_available_confirmed": "cases",
        "confirmed": "cases",
        "last_available_deaths": "deaths",
        "city_ibge_code": "code",
    }

    cases = data.rename(cols, axis=1)
    cases = cases[cases["code"].notna()]
    cases = cases[cases["place_type"] == "city"]

    cases["code"] = cases["code"].apply(lambda x: str(int(x))).astype("string")
    cases["code"] = "BR-" + cases["code"]
    cases["date"] = pd.to_datetime(cases["date"])

    cases = cases[["date", "code", "cases", "deaths"]]
    cases = cases.dropna().reset_index(drop=True)
    cases = cases.rename({"code": "id"}, axis=1)

    return cases


#
# Mobility data
#
@ttl_cache("covid-19", timeout=TIMEOUT)
@retry(10, sleep=0.5)
def google_mobility_data(cli=False):
    """
    Download google mobility data
    """
    url = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"

    log.info(f"Downloading google mobility data {datetime.now().date()}")
    t0 = time.time()
    data = requests.get(url)
    log.info(f"Download finished after {time.time() - t0:0.2} seconds")

    data_cols = ["retail", "grocery", "parks", "transit", "work", "residential"]

    df = pd.read_csv(data.content.decode("utf8")).rename(
        {
            "retail_and_recreation_percent_change_from_baseline": "retail",
            "grocery_and_pharmacy_percent_change_from_baseline": "grocery",
            "parks_percent_change_from_baseline": "parks",
            "transit_stations_percent_change_from_baseline": "transit",
            "workplaces_percent_change_from_baseline": "work",
            "residential_percent_change_from_baseline": "residential",
        },
        axis=1,
    )
    df["date"] = pd.to_datetime(df["date"])
    df[data_cols] = df[data_cols] / 100.0
    return df


def fix_google_mobility_data_region_codes(df):
    data = df[["country_region_code", "sub_region_1", "sub_region_2"]]
    codes = data.apply(subregion_code)
    return df


@lru_cache(1024)
def subregion_code(country, region, subregion):
    region = region or None
    subregion = subregion or None

    # Check arbitrary mapping
    mapping = google_mobility_map_codes()
    try:
        return mapping[country, region, subregion]
    except KeyError:
        pass

    # Fasttrack pure-country codes
    if not region:
        return country

    for name in (subregion, region):
        try:
            region = mundi.region(country_id=country, name=name)
        except LookupError:
            return region.id

    return country + "-" + region


@lru_cache(1)
def google_mobility_map_codes() -> dict:
    data = {}

    # Brazilian states
    for state in mundi.regions_dataframe("BR", type="state"):
        data["BR", f"State of {state}", None] = state.id
    data["BR", "Federal District", None] = "BR-DF"

    return data


if __name__ == "__main__":
    from sidekick.proxy import import_later

    import_later("..cli.api:covid19_api_downloader", package=__package__)()
