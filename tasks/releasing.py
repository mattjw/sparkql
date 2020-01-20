"""Tools to support building and publishing a release."""

import re
from dataclasses import dataclass
from typing import Optional

from invoke import Result
import tomlkit

from .utils import run, PROJECT_INFO


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
    next_ver_info = get_version_info()

    # Abandon if version fields do not match
    if next_ver_info.current_version != get_poetry_version():
        print(
            "Aborting\n"
            "Something's wrong with the project version. poetry and commitizen do not agree:\n"
            f"  poetry is {get_poetry_version()}\n"
            f"  commitizen is {next_ver_info.current_version}")
        exit(1)

    if next_ver_info.current_version == next_ver_info.next_version:
        print("No changes to release")
        exit()

    # Abandon if git tag already exists
    if git_tag_exists(next_ver_info.next_tag):
        print(f"Aborting\ngit tag for next release ({next_ver_info.next_tag}) already exists")
        exit(1)

    # Bump version project TOML
    run(f"poetry version {next_ver_info.next_version}")
    update_toml_commitizen_version(next_ver_info.next_version)


#
# Utils

@dataclass
class NextVersionInfo:
    current_version: str
    next_version: Optional[str]  # next version, or None if no bump detected
    next_tag: Optional[str]  # next git tag, or None if no bump detected
    increment_type: Optional[str]  # increment type (MAJOR, MINOR, PATCH), or None if no bump detected


def get_poetry_version() -> str:
    """Get current version according to poetry."""
    output = run("poetry version", warn=True, hide=True).stdout
    return re.search(r"(\d+\.\d+\.\d+)", output).group(1)


def get_version_info() -> NextVersionInfo:
    """
    Determine the version number that the next releaes will be, if any.

    Looks through commit messages since the last release (via Commitizen, assuming Conventional Commits).
    If, according to commits, a new release can happen, then determine what the version number will be.

    Returns:
        A string of the form "0.1.1" there is a new release to generate (based on commit messages), or
        `None` if the messages indicate that there is nothing to release.
    """
    result: Result = run("cz bump --dry-run --files-only", warn=True)
    cz_output = result.stdout

    if "NO_VERSION_SPECIFIED" in cz_output:
        print("It doesn't look like this project is set up for commitizen")
        exit(1)

    match = re.search(r"""bump: version ([.\d]+) → ([.\d]+)""", cz_output)
    current_version, next_version = match.groups()

    if current_version == next_version:
        return NextVersionInfo(current_version, None, None, None)

    match = re.search(r"""tag to create: ([.v\d]+)""", cz_output)
    next_tag = match.group(1)

    match = re.search(r"""increment detected: ([A-Z]+)""", cz_output)
    increment_type = match.group(1)

    return NextVersionInfo(current_version, next_version, next_tag, increment_type)


def git_tag_exists(tag: str) -> bool:
    """Return True if `tag` exists as a git tag."""
    result: Result = run(f"git describe --tags {tag}", hide=True, warn=True)
    return result.return_code == 0


def update_toml_commitizen_version(ver: str):
    """Update the commitizen version in the project TOML file."""
    with open(PROJECT_INFO.project_toml) as f_in:
        conf = tomlkit.parse(f_in.read())

    conf["tool"]["commitizen"]["version"] = ver

    with open(PROJECT_INFO.project_toml, "w") as f_out:
        f_out.write(tomlkit.dumps(conf))
