"""Array field."""
import copy
from typing import Optional, Generic, TypeVar

from pyspark.sql.types import ArrayType, StructField

from .base import BaseField


T = TypeVar('T', bound=BaseField)


class ArrayField(Generic[T], BaseField):
    """
    Array field; shadows ArrayType in the Spark API.

    A spark schema generated with this array field will behave as follows:
    - Nullability of the `StructField` is given by `ArrayField.nullable`, as per normal.
    - `containsNull` of the `ArrayType` is given by the nullability of the element contained
      within this field. In other words, `ArrayType.containsNull` is given by
      `ArrayField.element.nullable`.

    Attributes:

    """

    ArrayElem: T

    def __init__(self, element: T, nullable: bool = True, name: Optional[str] = None):
        super().__init__(nullable, name)
        self.ArrayElem = element

        if element._name_explicit is not None:
            raise ValueError("The element type of array should not have an explicit name")
            # None of the naming mechanics of this array's element type will be used.
            # The name of the element type will not be used for anything

    #
    # Field path chaining

    def replace_parent(self, parent: Optional["StructObject"] = None) -> "StructObject":
        field = copy.copy(self)
        field._parent_struct_object = self.ArrayElem.replace_parent(parent=parent)
        return field

    #
    # Field name management

    @BaseField._contextual_name.setter
    def _contextual_name(self, value: str):
        self._name_contextual = value  # TODO: this should go through parent
        self.ArrayElem._name_contextual = value  # TODO: set child to same name as parent

    #
    # Spark type management

    @property
    def _spark_type_class(self):
        return ArrayType

    @property
    def spark_struct_field(self) -> StructField:
        # containsNull => is used to indicate if elements in a ArrayType value can have null values
        return StructField(
            name=self.field_name,
            dataType=ArrayType(
                # Note that we do not care about the element's field name here:
                elementType=self.ArrayElem.spark_struct_field.dataType,
                containsNull=self.ArrayElem.is_nullable
            ),
            nullable=self.is_nullable,
        )
