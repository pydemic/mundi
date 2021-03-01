from .. import db


class HealthcarePlugin(db.Plugin):
    """
    Demography plugin gather information from various data sources concerning
    basic demographic parameters of a population.

    * population: Total population
    * age_distribution: Age distribution for groups of 5 years
    * age_pyramid: Age distribution disaggregated by sex
    """

    name = "healthcare"
    table_map = {"healthcare": db.Healthcare}
