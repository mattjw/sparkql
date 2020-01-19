from .utils import run


def typecheck():
    """Typecheck code."""
    run(f"pytype --config .pytype.cfg")
