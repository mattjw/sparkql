from .utils import run

SRC_DIR = "sparkql"


def test():
    run("pytest -vv -c tests/.pytest.ini")


def reformat():
    run(f"black {SRC_DIR}")
