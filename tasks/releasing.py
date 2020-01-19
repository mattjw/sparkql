"""Tools to support building and publishing a release."""
import re
from dataclasses import dataclass
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
    print(get_version_info())


#
# Utils

@dataclass
class VersionInfo:
    current_version: str
    next_version: Optional[str]  # next version, or None if no bump detected
    next_tag: Optional[str]  # next git tag, or None if no bump detected
    increment_type: Optional[str]  # increment type (MAJOR, MINOR, PATCH), or None if no bump detected


def get_version_info() -> VersionInfo:
    """
    Determine the version number that the next releaes will be, if any.

    Looks through commit messages since the last release (via Commitizen, assuming Conventional Commits).
    If, according to commits, a new release can happen, then determine what the version number will be.

    Returns:
        A string of the form "0.1.1" there is a new release to generate (based on commit messages), or
        `None` if the messages indicate that there is nothing to release.
    """
    cz_output = run("cz bump --dry-run --files-only", warn=True, hide=True).stdout
    print(cz_output)

    """bump: version 0.1.1 → 0.1.2
tag to create: v0.1.2
increment detected: PATCH"""

    match = re.search(r"""bump: version ([.\d]+) → ([.\d]+)""", cz_output)
    current_version, next_version = match.groups()

    match = re.search(r"""tag to create: ([.v\d]+)""", cz_output)
    next_tag = match.group(1)

    match = re.search(r"""increment detected: ([A-Z]+)""", cz_output)
    increment_type = match.group(1)

    return VersionInfo(current_version, next_version, next_tag, increment_type)
