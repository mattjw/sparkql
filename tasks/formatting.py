from .utils import run, PROJECT_INFO


def reformat():
    run(
        f"black {PROJECT_INFO.source_directory} {PROJECT_INFO.tasks_directory} "
        f"{PROJECT_INFO.tests_directory}"
    )
