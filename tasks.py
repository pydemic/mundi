from invoke import task


@task
def test(ctx):
    ctx.run("pytest tests/ --cov")
    ctx.run("black --check .")
    ctx.run("pycodestyle")


@task
def prepare_data(ctx, fast=False):
    if not fast:
        ctx.run("python -m mundi.plugins.region prepare -v")
    ctx.run("python -m mundi.plugins.region collect -v -t region")
    ctx.run("python -m mundi.plugins.region import -v -t region")
    ctx.run("python -m mundi.plugins.region collect -v -t region_m2m")
    ctx.run("python -m mundi.plugins.region import -v -t region_m2m")

    if not fast:
        ctx.run("python -m mundi.plugins.demography prepare -v")
    ctx.run("python -m mundi.plugins.demography collect -v")
    ctx.run("python -m mundi.plugins.demography import -v")

    if not fast:
        ctx.run("python -m mundi.plugins.healthcare prepare -v")
    ctx.run("python -m mundi.plugins.healthcare collect -v")
    ctx.run("python -m mundi.plugins.healthcare import -v")
