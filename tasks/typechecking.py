from .utils import run


#
# Commands


def typecheck():
    """Typecheck code."""
    run(f"pytype --config .pytype.cfg")
