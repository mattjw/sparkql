"""Array field."""

import copy
from typing import Optional, Generic, TypeVar

from pyspark.sql.types import ArrayType, StructField

from .base import BaseField


ArrayElementType = TypeVar("ArrayElementType", bound=BaseField)


class Array(Generic[ArrayElementType], BaseField):
    """
    Array field; shadows ArrayType in the Spark API.

    A spark schema generated with this array field will behave as follows:
    - Nullability of the `StructField` is given by `Array.nullable`, as per normal.
    - `containsNull` of the `ArrayType` is given by the nullability of the element contained
      within this field. In other words, `ArrayType.containsNull` is given by
      `Array.element.nullable`.

    Attributes:
        etype: Data type info for the element of this array. Should be an instance of a `BaseField`.
    """

    etype: ArrayElementType  # pytype: disable=not-supported-yet

    def __init__(self, element: ArrayElementType, nullable: bool = True, name: Optional[str] = None):
        super().__init__(nullable, name)

        if not isinstance(element, BaseField):
            raise ValueError(f"Array element must be a field. Found type: {type(element)}")

        self.etype = element
        if element._resolve_field_name() is not None:
            raise ValueError(
                "When using a field as the element field of an array, the field shoud not have a name. "
                f"The field's name resolved to: {element._resolve_field_name()}"
            )
            # None of the naming mechanics of this array's element type will be used.
            # The name of the element type will not be used for anything

    #
    # Field path chaining

    def _replace_parent(self, parent: Optional["Struct"] = None) -> "Struct":  # pytype: disable=invalid-annotation
        """Return a copy of this array with the parent attribute set."""
        field = copy.copy(self)
        field._parent_struct = self.etype._replace_parent(parent=parent)  # pylint: disable=protected-access
        return field

    #
    # Field name management

    def _set_contextual_name(self, value: str):
        self._INTERNAL_name_contextual__INTERNAL = value
        # set child to same name as parent; i.e., propagate contextual name downwards:
        self.etype._set_contextual_name(value)  # pylint: disable=protected-access

    #
    # Spark type management

    @property
    def _spark_type_class(self):
        return ArrayType

    @property
    def spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""
        # containsNull => is used to indicate if elements in a ArrayType value can have null values
        return StructField(
            name=self.field_name,
            dataType=ArrayType(
                # Note that we do not care about the element's field name here:
                elementType=self.etype.spark_struct_field.dataType,
                containsNull=self.etype._is_nullable,  # pylint: disable=protected-access
            ),
            nullable=self._is_nullable,
        )
