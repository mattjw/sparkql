from .utils import run, PROJECT_INFO


def typecheck():
    """Typecheck code."""
    # run(f"pytype {PROJECT_INFO.source_directory}")
    run(f"pyre --source-directory {PROJECT_INFO.source_directory} --search-path ")
