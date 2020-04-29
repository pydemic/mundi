import click
from mundi import regions


@click.command("mundi")
@click.option("--country_code", "-c", help="Country code")
@click.option("--type", "-t", default="country", help="Region type")
def cli(**kwargs):
    if "type" in kwargs:
        kwargs["type"] = kwargs["type"].lower()
        if kwargs["type"] == "all":
            del kwargs["type"]

    df = regions(**kwargs)
    print(df)


cli()
