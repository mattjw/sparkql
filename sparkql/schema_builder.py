"""Schema management operations."""

from typing import Union, Type

from pyspark.sql.types import StructType

from .fields.struct import StructObject


def schema(object: Union[StructObject, Type[StructObject]]) -> StructType:
    """Spark schema for a struct object."""
    return object._struct_object_meta.spark_struct
