"""Schema management operations."""

from typing import Union, Type

from pyspark.sql.types import StructType

from .fields.struct import Struct


def schema(schema_object: Union[Struct, Type[Struct]]) -> StructType:
    """Spark schema for a struct object."""
    return schema_object._struct_metadata.spark_struct  # pylint: disable=protected-access
