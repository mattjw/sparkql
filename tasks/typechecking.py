from .utils import run, PROJECT_INFO


def typecheck():
    """Typecheck code."""
    run(f"pytype --config .pytype.cfg")
    # run(f"pyre --source-directory {PROJECT_INFO.source_directory} --search-path $(poetry env info -p)", echo=True)
