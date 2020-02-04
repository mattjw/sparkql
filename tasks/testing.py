from .utils import run, PROJECT_INFO, print_heavy


#
# Commands


def test():
    print_heavy(f"\n🔍 TESTING 🔎\n")
    run(f"pytest -vv -c {PROJECT_INFO.tests_directory}/.pytest.ini")
