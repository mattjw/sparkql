from .utils import run, print_heavy


#
# Commands


def typecheck():
    """Typecheck code."""
    print_heavy(f"\n🔍 TYPECHECKING 🔎\n")
    run(f"pytype --config .pytype.cfg")
