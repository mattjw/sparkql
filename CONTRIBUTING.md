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
poetry run tests
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

## Versioning strategy

This library uses [Semantic Versioning](https://semver.org/).

## Library design principles

- The API should favour strong type inference. Enables IDE completions
  and strong type checking.
