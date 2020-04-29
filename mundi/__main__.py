import click
import mundi


@click.command("mundi")
@click.option("--country", "-c", help="Country code")
@click.option("--type", "-t", default="country", help="Region type")
def cli(country=None, **kwargs):
    if country:
        df = mundi.region(country)
    else:
        df = mundi.regions(**kwargs)
    print(df)


cli()
