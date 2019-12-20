from .utils import run, PROJECT_INFO


def test():
    run(f"pytest -vv -c {PROJECT_INFO.tests_directory}/.pytest.ini")
