from invoke import task


@task
def test(ctx):
    ctx.run("pytest tests/ --cov")
    ctx.run("black --check .")
    ctx.run("pycodestyle")


@task
def prepare_data(ctx, clear=False):
    if clear:
        ctx.run("rm data/tmp/* -rfv")
    ctx.run("rm mundi/databases/* -rfv")
    ctx.run("python -m mundi.prepare")
