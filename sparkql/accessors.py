"""Accessor functions in order to access field paths, field names, and related field attributes."""

# pylint: disable=protected-access

from typing import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as sql_funcs
from pyspark.sql.types import StructField

from sparkql.fields.base import BaseField


def path_seq(field: BaseField) -> Sequence[str]:
    """Items on the path to a field."""
    fields = [field]
    while fields[0]._parent is not None:
        fields.insert(0, fields[0]._parent)

    assert all(
        field._resolve_field_name() is not None for field in fields
    ), f"Encountered an unset name while traversing path. Path is: {_pretty_path(fields)}"

    return [f._field_name for f in fields]


def path_str(field: BaseField) -> str:
    """Return dot-delimited path to field `field`."""
    return ".".join(path_seq(field))


def path_col(field: BaseField) -> Column:
    """Return Spark column pointing to field `field`."""
    fields_seq = path_seq(field)
    col: Column = sql_funcs.col(fields_seq[0])  # pylint: disable=no-member
    for col_field_name in fields_seq[1:]:
        col = col[col_field_name]
    return col


def name(field: BaseField) -> str:
    """Return field name of field `field`."""
    return field._field_name


def struct_field(field: BaseField) -> StructField:
    """Return the equivalent PySpark StructField of field `field`."""
    return field._spark_struct_field


def _pretty_path(path: Sequence[BaseField]):
    """Build pretty string of path, for debug and/or error purposes."""
    return "< " + " -> ".join(f"'{field._resolve_field_name()}' ({type(field).__name__})" for field in path) + " >"
