from tasks.utils import run, PROJECT_INFO, print_heavy, prepare_reports_dir


#
# Commands


def test():
    print_heavy(f"\n🔍 TESTING 🔎\n")
    prepare_reports_dir()
    run(
        f"""
        export COVERAGE_FILE={PROJECT_INFO.reports_directory / ".coverage"};
        pytest -vv --cov={PROJECT_INFO.source_directory} -c {PROJECT_INFO.tests_directory}/.pytest.ini"""
    )
    run(
        f"""
        export COVERAGE_FILE={PROJECT_INFO.reports_directory / ".coverage"};
        poetry run coverage html -d {PROJECT_INFO.reports_directory / "htmlcov"}"""
    )
    run(
        f"""
            export COVERAGE_FILE={PROJECT_INFO.reports_directory / ".coverage"};
            poetry run coverage xml -o {PROJECT_INFO.reports_directory / "coverage.xml"}"""
    )
