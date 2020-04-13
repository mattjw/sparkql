"""Support for combining Spark schemas."""

from collections import OrderedDict
from copy import copy
from typing import Dict, Union, Optional, overload


from pyspark.sql.types import StructType, StructField, ArrayType, AtomicType


MergeableSparkDataType = Union[StructType, ArrayType, AtomicType]


@overload
def merge_schemas(type_a: StructType, type_b: StructType) -> StructType:  # noqa: D103 (pydocstyle missing docstring)
    ...  # pragma: no cover


@overload
def merge_schemas(type_a: ArrayType, type_b: ArrayType) -> ArrayType:  # noqa: D103 (pydocstyle missing docstring)
    ...  # pragma: no cover


@overload
def merge_schemas(type_a: AtomicType, type_b: AtomicType) -> AtomicType:  # noqa: D103 (pydocstyle missing docstring)
    ...  # pragma: no cover


def merge_schemas(type_a: MergeableSparkDataType, type_b: MergeableSparkDataType) -> MergeableSparkDataType:
    """
    Merge two schemas (or any Spark types) and return the merged schema.

    When merging `StructType`s, nested schemas are merged recursively. Fields shared between the two schemas must be
    compatible; specifically,
    - fields containing atomic types must contain the same type,
    - fields must have the same nullability,
    - arrays must have the same containsNull.

    Args:
        type_a: A type to be merged.
        type_b: A type to be merged.

    Returns:
        The merger of `type_a` with `type_b`.
    """
    return _SchemaMerger.merge_types(type_a, type_b, parent_field_name=None)


class _SchemaMerger:
    """
    Merge two schemas.

    See `merge_schemas` for detailed behaviour description.
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
            raise ValueError(
                _validation_error_message(
                    "Fields must have matching nullability constraints. "
                    f"nullable of field A is {field_a.nullable}. "
                    f"nullable of field B is {field_b.nullable}",
                    parent_field_name=field_a.name,
                )
            )

        return StructField(
            name=field_a.name,
            dataType=cls.merge_types(field_a.dataType, field_b.dataType, parent_field_name=field_a.name),
            nullable=field_a.nullable,
        )

    #
    # Merge by type

    @classmethod
    def merge_types(
        cls, type_a: MergeableSparkDataType, type_b: MergeableSparkDataType, parent_field_name: Optional[str]
    ) -> MergeableSparkDataType:
        """
        Merge two arbitrary types; delegates to corresponding methods.

        `parent_field_name` is the name of the field to which the types belong.
        It is used to generate an intuitive message if validation fails.
        """
        if type(type_a) is not type(type_b):
            raise ValueError(
                _validation_error_message(
                    "Types must match. "
                    f"Type of A is {type_a.__class__.__name__}. Type of B is {type_b.__class__.__name__}",
                    parent_field_name=parent_field_name,
                )
            )

        if isinstance(type_a, StructType):
            assert isinstance(type_b, StructType)
            return cls.merge_struct_types(type_a, type_b)

        if isinstance(type_a, ArrayType):
            assert isinstance(type_b, ArrayType)
            return cls.merge_array_types(type_a, type_b, parent_field_name=parent_field_name)

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
    def merge_array_types(cls, array_type_a: ArrayType, array_type_b: ArrayType, parent_field_name) -> ArrayType:
        assert all(isinstance(obj, ArrayType) for obj in [array_type_a, array_type_b])

        if array_type_a.containsNull != array_type_b.containsNull:
            raise ValueError(
                _validation_error_message(
                    "Arrays must have matching containsNull constraints. "
                    f"containsNull of array A is {array_type_a.containsNull}. "
                    f"containsNull of array B is {array_type_b.containsNull}",
                    parent_field_name=parent_field_name,
                )
            )

        return ArrayType(
            elementType=cls.merge_types(
                array_type_a.elementType, array_type_b.elementType, parent_field_name=parent_field_name
            ),
            containsNull=array_type_a.containsNull,
        )


def _validation_error_message(message: str, parent_field_name: Optional[str]) -> str:
    if parent_field_name is not None:
        return f"Cannot merge due to incompatibility in field '{parent_field_name}': {message}"
    return f"Cannot merge due to incompatibility: {message}"
