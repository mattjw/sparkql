from .utils import run, PROJECT_INFO


def reformat():
    run(f"black {PROJECT_INFO.source_directory}")
