"""Tasks utils."""

from invoke import run as invoke_run
from invoke import UnexpectedExit, Result


def run(cmd, pty=None, env=None, *args, **kwargs):
    """
    invoke's default runner, with some customisation.

    Customisations:
    - Ensure PYTHONPATH is present and allows package to be imported.
    - Do not use UnexpectedExit's default message format. stderr & stdout will be output and then the task will exit
    """
    if pty is None:
        pty = True
    if env is None:
        env = {"PYTHONPATH": "./"}

    try:
        return invoke_run(cmd, env=env, pty=pty, *args, **kwargs)
    except UnexpectedExit as err:
        result: Result = err.result
        exit(result.return_code)


class ProjectInfo:
    """Information about this project."""

    source_directory: str = "sparkql/"  # top of source code tree
    tests_directory: str = "tests/"
    tasks_directory: str = "tasks/"


PROJECT_INFO = ProjectInfo()
