"""
Run all importers, processing raw data and saving the result to the main
database file.
"""

from .countries import br
from .pycountry import Country, Subdivisions
from .region import RegionGroup
from .wb_summary import WbSummary


def load_regions():
    imp = RegionGroup([Country(), Subdivisions(), br.BrRegions()])
    imp.run()


def load_extra():
    WbSummary().run()


def main():
    load_regions()
    load_extra()


if __name__ == "__main__":
    main()
