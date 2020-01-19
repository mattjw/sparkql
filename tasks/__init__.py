from .formatting import reformat
from .testing import test
from .linting import lint
from .releasing import prepare_release
from .typechecking import typecheck


def verify_all():
    """Run all checks."""
    lint()
    typecheck()
    test()
