from invoke import task


@task
def test(ctx):
    ctx.run("pytest tests/ --cov")
    ctx.run("black --check .")
    ctx.run("pycodestyle")


@task
def prepare_data(ctx, fast=False):
    if not fast:
        ctx.run("rm mundi/databases/* -rfv")
        ctx.run("python -m mundi prepare mundi")
        print()

    ctx.run("python -m mundi compile mundi mundi -o db.sqlite -f fix_types")
    print()
    ctx.run("python -m mundi compile mundi un -o db.sqlite -f fix_types")
    print()
    ctx.run("python -m mundi compile mundi un -o un.pkl.gz")
