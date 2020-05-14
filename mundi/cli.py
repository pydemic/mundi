import importlib
import time
from pathlib import Path

import click


@click.group()
def plugin():
    pass


@plugin.command(name="prepare")
@click.argument("package")
@click.option("--silent", "-s", is_flag=True, help="Do not print debug messages")
def prepare_cmd(**kwargs):
    """
    Prepare data for mundi plugin.
    """
    prepare(**kwargs)


def prepare(package, silent=False):
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


@plugin.command(name="compile")
@click.argument("package")
@click.argument("kind")
@click.option("--silent", "-s", is_flag=True, help="Do not print debug messages")
@click.option("--output", "-o", help="Name of output file")
@click.option(
    "--fix", "-f", multiple=True, help="Name of functions used to post-process data"
)
def compile_cmd(**kwargs):
    """
    Compile prepared data for mundi plugin.
    """
    compile(**kwargs)


def compile(package, kind=None, output=None, fix=(), silent=False):
    """
    Compile prepared data for mundi plugin.
    """
    from .plugin import utils

    log = lambda *args: None if silent else click.echo(*args)
    t0 = time.time()

    log(f"Collecting processed files for {package}/{kind}")
    data = utils.collect_processed_data(package, kind)
    try:
        mod = importlib.import_module(package)
    except ImportError as e:
        raise SystemExit(e)

    # Apply fixes
    for name in fix:
        if callable(name):
            fn = name
            log(f"Applying fix: {fn.__name__}...")
            data = fn(package, kind, data)
            continue

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
