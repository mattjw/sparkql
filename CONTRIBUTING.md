# Contributor's guide

## Dependency management

Dependencies are managed with `poetry`. Make sure you have poetry
[installed on your system](https://python-poetry.org/docs/#installation),
and then install package dependencies:

```bash
poetry install
```

## Code verification

A short-hand to run all verification tasks is as follows:

```bash
poetry run verify-all
```

To only run the tests:

```bash
poetry run test
```

To only run the type checks:

```bash
poetry run typecheck 
```

To only run the linting checks:

```bash
poetry run lint 
```

## Code style

All code formatting is handled by the
[`black`](https://black.readthedocs.io/) auto-formatter. You can
auto-format your code changes with:

```bash
poetry run reformat
```

CI will reject changes that do not comply with `black`.

Additional lint checks are handled by a few linting tools, including
`pylint`, `pycodestyle`, and `pydocstyle`.

Type hints are strongly encouraged, and are checked during CI.

## Versioning and tracking changes

This library uses [Semantic Versioning](https://semver.org/) (semver).

To track changes to the library (major, minor, patch), this library
uses an abridged version of
[Conventional Commits](https://woile.github.io/commitizen/tutorials/writing_commits/#conventional-commits):

- The change is a PATCH change if the message begins with `fix`.
- The change is a MINOR change if the message begins with `feat`.
- The change is a MAJOR change if the message includes `BREAKING CHANGE`.
- If the commit history does not match any of the above, no new
  release will happen.

commitizen is used to evaluate the type of change. There is no requirement
to use commitzen to prepare your commit messages.

## Creating a new release

If you have accumulated releasable changes that you would like to
publish, the following instructions explain how to make a new release
by hand.

First, ensure you are on an up-to-date `master` branch. 

You can check whether there are any releasable changes with:

```bash
poetry run find-releasable-changes
``` 

The following command will look through the commit history to detect
whether any releasable changes have been made. If this is the case,
it will bump the project's version number (MAJOR/MINOR/PATCH, as
automatically identified from the commit history) and then create a
new commit and tag for the release. 

```bash
poetry run prepare-release
```

Finally, ensure you have PyPI crendentials available and then
enact the release:

```bash
export PYPI_USERNAME="MY_USERNAME"
export PYPI_PASSWORD="MY_PASSWORD_OR_TOKEN"
poetry publish --build --username "${PYPI_USERNAME}" --password "${PYPI_TOKEN}" --no-interaction
```

Don't forget to push the git tag:

```bash
git push origin <TAG>
```

## Library design philosophy

- The API should favour strong type inference. Enables IDE completions
  and strong type checking.
