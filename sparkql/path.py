"""Obtain path to a field within a possibly nested hierarchy."""

# pylint: disable=protected-access

from typing import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as sql_funcs

from .fields.base import BaseField


def path_seq(field: BaseField) -> Sequence[str]:
    """Items on the path to a field."""
    fields = [field]
    while fields[0]._parent is not None:
        if fields[0].field_name is None:
            raise ValueError("Encountered an unset name while traversing tree")
        fields.insert(0, fields[0]._parent)
    return [f.field_name for f in fields]


def path_str(field: BaseField) -> str:
    """Return dot-delimited path to field `field`."""
    return ".".join(path_seq(field))


def path_col(field: BaseField) -> Column:
    """Return Spark column pointing to field `field`."""
    fields_seq = path_seq(field)
    col: Column = sql_funcs.col(fields_seq[0])  # pylint: disable=no-member
    for field_name in fields_seq[1:]:
        col = col[field_name]
    return col
