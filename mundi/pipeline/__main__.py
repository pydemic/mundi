import click
import sidekick.api as sk

from .. import plugins as plugin_modules, Path
from ..database import create_tables

VALID_PLUGINS = ["main", "demography", "healthcare"]


@click.command()
@click.option("--path", "-p", default=None, help="Location of data repository.")
@click.option("--plugin", help="Select an specific plugin.")
@click.option("--force", "-f", is_flag=True, help="Force rebuilding assets.")
def main(path, plugin, force):
    if plugin is None:
        plugins_str = ", ".join(VALID_PLUGINS)
        click.echo(f"Creating standard plugins: {plugins_str}.")
        plugins = VALID_PLUGINS
    elif plugin not in VALID_PLUGINS:
        raise SystemExit(f"invalid plugin: {plugin}")
    else:
        plugins = [plugin]

    path = get_location() if path is None else Path(path)

    for plugin in plugins:
        click.echo(f"Processing plugin: {plugin}")
        mod = getattr(plugin_modules, plugin)
        mod.pipeline(path, force=force)

    click.echo("Creating tables")
    create_tables()


@sk.once
def get_location():
    return Path(".").absolute() / "mundi-data"


if __name__ == "__main__":
    main()
