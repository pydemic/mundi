import importlib
import time
from pathlib import Path

import click


@click.group()
def plugin():
    pass


@plugin.command()
@click.argument("package")
@click.option("--silent", "-s", is_flag=True, help="Do not print debug messages")
def prepare(package, silent):
    """
    Prepare data for mundi plugin.
    """
    from .plugin import utils

    log = lambda *args: None if silent else click.echo(*args)
    t0 = time.time()

    log("Cleaning files...")
    utils.clean_processed_data(package, verbose=not silent)
    log("")

    utils.execute_prepare_scripts(package, verbose=not silent)
    log(f"Finished in {time.time() - t0:.2} seconds.")


@plugin.command()
@click.argument("package")
@click.argument("kind")
@click.option("--silent", "-s", is_flag=True, help="Do not print debug messages")
@click.option("--output", "-o", help="Name of output file")
@click.option(
    "--fix", "-f", multiple=True, help="Name of functions used to post-process data"
)
def compile(package, kind, output, fix, silent):
    """
    Compile prepared data for mundi plugin.
    """
    from .plugin import utils

    log = lambda *args: None if silent else click.echo(*args)
    t0 = time.time()

    log("Collecting processed files...")
    data = utils.collect_processed_data(package, kind)
    try:
        mod = importlib.import_module(package)
    except ImportError as e:
        raise SystemExit(e)

    # Apply fixes
    for name in fix:
        log(f"Applying fix: {name}...")

        # Normalize name
        if "." not in name:
            name = f"mundi.fix.{name}"
        elif name.startswith("."):
            name = package + name

        # Extract optional arguments
        name, _, args = name.partition("(")
        if args:
            args = [args.rstrip(")").split(",")]
        else:
            args = ()

        # Import function and apply
        pre, _, post = name.rpartition(".")
        try:
            fix_mod = importlib.import_module(pre)
            fn = getattr(fix_mod, post)
        except (ImportError, AttributeError):
            raise SystemExit(f"invalid python name: {name}")

        data = fn(package, kind, data, *args)

    # Save output
    if output is None:
        print(data)
    else:
        path = Path(mod.__file__).parent.resolve() / "databases" / output
        utils.save_path(data, path, kind)
        log(f" - Database saved to {path}")

    log(f"Finished in {time.time() - t0:.2} seconds.")
