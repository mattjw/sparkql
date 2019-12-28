from .formatting import reformat
from .testing import test
from .linting import lint
from .typechecking import typecheck


def verify_all():
    """Run all checks."""
    test()
    lint()
    typecheck()
