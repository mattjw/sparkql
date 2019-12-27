from .utils import run, PROJECT_INFO


def typecheck():
    """Typecheck code."""
    run(f"typecheckcmd {PROJECT_INFO.source_directory}")
