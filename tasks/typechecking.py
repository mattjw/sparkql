from .utils import run, PROJECT_INFO


def typecheck():
    """Typecheck code."""
    run(f"pytype --config .pytype.cfg")
