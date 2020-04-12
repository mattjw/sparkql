"""Support for combining Spark schemas."""

from collections import OrderedDict
from copy import copy
from typing import Dict

from pyspark.sql.types import StructType, StructField, ArrayType, DataType


def merge_schemas(struct_a: StructType, struct_b: StructType) -> StructType:
    """
    Merge two schemas.

    meerge schemas

    Args:
        struct_a: TODO
        struct_b: TODO

    Returns:
        TODO
    """
    return _SchemaMerger.merge_struct_types(struct_a, struct_b)


class _SchemaMerger:
    """
    Merge two schemas.

    See ``merge_schemas`` for detailed behaviour description.
    """

    @classmethod
    def append_to_fields(cls, fields: Dict[str, StructField], field: StructField) -> None:
        """
        Add `field` to `fields`; modifies `fields` as a side-effect.

        Args:
            fields: Mapping of field name to field.
            field: Field to be added.
        """
        if field.name not in fields:
            fields[field.name] = field
        else:
            fields[field.name] = cls.merge_fields(fields[field.name], field)

    @classmethod
    def merge_fields(cls, field_a: StructField, field_b: StructField) -> StructField:
        assert field_a.name == field_b.name
        if field_a.nullable != field_b.nullable:
            raise ValueError("XXX merging cannot change nullable XXX")

        if type(field_a.dataType) is not type(field_a.dataType):
            raise ValueError("XXX merging cannot change class of data type XXX")

        return StructField(
            name=field_a.name, dataType=cls.merge_types(field_a.dataType, field_b.dataType), nullable=field_a.nullable
        )

    #
    # Merge by type

    @classmethod
    def merge_types(cls, type_a: DataType, type_b: DataType) -> DataType:
        """Merge two arbitrary types; delegates to corresponding methods."""
        assert type(type_a) is type(type_b)

        if isinstance(type_a, StructType):
            assert isinstance(type_b, StructType)
            return cls.merge_struct_types(type_a, type_b)
        elif isinstance(type_a, ArrayType):
            assert isinstance(type_b, ArrayType)
            return cls.merge_array_types(type_a, type_b)

        assert type_a == type_b
        return copy(type_a)

    @classmethod
    def merge_struct_types(cls, struct_type_a: StructType, struct_type_b: StructType) -> StructType:
        assert all(isinstance(obj, StructType) for obj in [struct_type_a, struct_type_b])

        fields: Dict[str, StructField] = OrderedDict()
        for field in struct_type_a.fields:
            cls.append_to_fields(fields, field)
        for field in struct_type_b.fields:
            cls.append_to_fields(fields, field)

        return StructType(list(fields.values()))

    @classmethod
    def merge_array_types(cls, array_type_a: ArrayType, array_type_b: ArrayType) -> ArrayType:
        assert all(isinstance(obj, ArrayType) for obj in [array_type_a, array_type_b])

        if array_type_a.containsNull != array_type_b.containsNull:
            raise ValueError("XXX cannot change containsNull array type XXX")

        return ArrayType(elementType=None, containsNull=array_type_a.containsNull)
