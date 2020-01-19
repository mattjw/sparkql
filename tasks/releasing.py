"""Tools to support building and publishing a release."""
from typing import Optional

from invoke import Result

from .utils import run, print_heavy


#
# Commands

def prepare_release():
    """
    XXX

    Note the following side-effects:
    - Project version number my increment. `pyproject.toml` is updated.
    - Creation of a new git tag.
    - Creation of a new commit, implementing the version increment.
    """
    print(get_next_version())


#
# Utils

def get_next_version() -> Optional[str]:
    """
    Determine the version number that the next releaes will be, if any.

    Looks through commit messages since the last release (via Commitizen, assuming Conventional Commits).
    If, according to commits, a new release can happen, then determine what the version number will be.

    Returns:
        A string of the form "0.1.1" there is a new release to generate (based on commit messages), or
        `None` if the messages indicate that there is nothing to release.
    """
    result: Result = run("cz bump --dry-run --files-only", warn=True, hide=True)

    print(result.stdout)
