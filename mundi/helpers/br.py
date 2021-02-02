import json
from pathlib import Path

import sidekick.api as sk

from ..types import Region


def ibge_city_code(code: str) -> str:
    """
    Normalize IBGE city code to use 7 digits alternatives.
    """
    if code in ibge_city_codes():
        return code
    try:
        return ibge_city_code_mapping()[code]
    except KeyError:
        raise ValueError(f"invalid city code: {code!r}")


def ibge_city(code: str) -> Region:
    """
    Load city from IBGE 6 or 7 digits code.
    """
    if code.startswith("BR-"):
        return Region(code)
    else:
        return Region(f"BR-{ibge_city_code(code)}")


@sk.once
def ibge_city_code_mapping() -> dict:
    """
    Return dictionary mapping 6-digit IBGE city code to the 7-digit counterpart.
    """
    data_path = Path(__file__).parent / "data" / "br-city-codes.json"
    with open(data_path) as fd:
        return json.load(fd)


@sk.once
def ibge_city_codes() -> set:
    """
    Return a set with all valid IBGE city codes.
    """
    data_path = Path(__file__).parent / "data" / "br-city-codes.json"
    with open(data_path) as fd:
        return json.load(fd)
