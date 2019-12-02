from pyspark.sql.types import StructType


def pretty_schema(struct: StructType) -> str:
    """
    Returns a pretty stringified `struct`.

    This is similar to the StructType's default string formatter, but with the
    addition of white space, new lines, and indentation.
    """
    return ""
