from invoke import run as invoke_run
from invoke import UnexpectedExit, Result


SRC_DIR = "sparkql"


def run(cmd, pty=None, env=None, *args, **kwargs):
    """
    invoke's default runner, with some customisation.

    Customisations:
    - Ensure PYTHONPATH is present and allows package to be imported.
    - Do not use UnexpectedExit's default message format. stderr & stdout will be output and
    """
    if pty is None:
        pty = True
    if env is None:
        env = {"PYTHONPATH": "./"}

    try:
        invoke_run(cmd, env=env, pty=pty, *args, **kwargs)
    except UnexpectedExit as err:
        result: Result = err.result
        exit(result.return_code)


def test():
    run("pytest -vv -c tests/.pytest.ini")


def reformat():
    run(f"black {SRC_DIR}")
