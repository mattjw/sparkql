"""Schema management operations."""

from typing import Union, Type

from pyspark.sql.types import StructType

from .fields.struct import StructObject


def schema(schema_object: Union[StructObject, Type[StructObject]]) -> StructType:
    """Spark schema for a struct object."""
    return schema_object._struct_object_meta.spark_struct  # pylint: disable=protected-access
