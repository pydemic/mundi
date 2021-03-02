from invoke import task


@task
def test_all(ctx):
    test(ctx)
    doctest(ctx)
    code_style(ctx)


@task
def test(ctx):
    ctx.run("pytest tests/ --cov", pty=True)


@task
def doctest(ctx):
    ctx.run("sphinx-build docs/ build/docs/ -b doctest", pty=True)


@task
def code_style(ctx):
    ctx.run("black --check .", pty=True)
    ctx.run("pycodestyle", pty=True)


@task
def bootstrap(ctx, fast=False):
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
