"""Base field and abstract fields."""

from abc import ABC, abstractmethod
from typing import Optional, Type
import copy

from pyspark.sql import types as sql_type
from pyspark.sql.types import StructField, DataType

from ..exceptions import FieldNameError, FieldParentError

# pytype: disable=invalid-annotation


class BaseField(ABC):
    """Root of the field hierarchy; shadows DataType in the Spark API."""

    # Name management logic:
    # - Explicit name (`_name_explicit`): Set via constructor.
    # - Contextual name (`_name_contextual`): Inferred for the field as it is used in a struct object.
    #   This will always get set, although not immediately. The struct object that will contain this field
    #   is responsible for setting the contextual name.
    # The explicit name, if provided, will override the contextual name.

    _nullable: bool = True
    _name_explicit: Optional[str] = None
    _name_contextual: Optional[str] = None
    _parent_struct_object: Optional["StructObject"] = None

    def __init__(self, nullable: bool = True, name: Optional[str] = None):
        """
        Constructor for a base field.

        Args:
            nullable: Is this field nullable.
            name: Field name. If None, field name will be identified via ivar context resolution.
        """
        self._nullable = nullable
        self._name_explicit = name

    #
    # Nullability

    @property
    def is_nullable(self) -> bool:
        """The nullability status of this field."""
        return self._nullable

    #
    # Field path chaining

    @property
    def _parent(self) -> Optional["StructObject"]:
        return self._parent_struct_object

    def replace_parent(self, parent: Optional["StructObject"] = None) -> "BaseField":
        """Return a copy of this Field with the parent attribute set."""
        field = copy.copy(self)
        if self._parent_struct_object is not None:
            raise FieldParentError("Attempted to set parent field that has already been set")
        field._parent_struct_object = parent  # pylint: disable=protected-access
        return field

    #
    # Field name management

    @property
    def _contextual_name(self) -> Optional[str]:
        return self._name_contextual

    @_contextual_name.setter
    def _contextual_name(self, value: str):
        if self._name_contextual is not None:
            raise FieldNameError(
                "Attempted to override a name that has already been set: "
                f"'{value}' replacing '{self._name_contextual}'"
            )
        self._name_contextual = value

    @property
    def field_name(self) -> str:
        """The name for this field."""
        name = self._resolve_field_name()
        if name is None:
            raise FieldNameError(
                "No field name found among: explicit name = {}, inferred name = {}".format(
                    self._name_explicit, self._name_contextual
                )
            )
        return name

    def _resolve_field_name(self) -> Optional[str]:
        """Resolve name for this field, or None if no concrete name set."""
        if self._name_explicit is not None:
            return self._name_explicit
        if self._name_contextual is not None:
            return self._name_contextual
        return None

    #
    # Spark type management

    @property
    @abstractmethod
    def _spark_type_class(self) -> Type[DataType]:
        """The class of the Spark type corresponding to this field."""

    @property
    @abstractmethod
    def spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""

    #
    # Misc.

    def __str__(self):
        """String formatted object."""
        return (
            f"<{type(self).__name__} \n"
            f"  spark type = {self._spark_type_class.__name__} \n"
            f"  nullable = {self.is_nullable} \n"
            f"  name = {self._resolve_field_name()} <- {[self._name_explicit, self._name_contextual]} \n"
            f"  parent = {self._parent}"
            ">"
        )


class AtomicField(BaseField):
    """
    Atomic field type.

    In the Spark API types hierarchy:

    ```
    DataType
     |- AtomicType
     |- ...
    ```
    """

    @property
    @abstractmethod
    def _spark_type_class(self) -> Type[DataType]:
        """The class of the Spark type corresponding to this field."""

    @property
    def spark_data_type(self) -> sql_type.DataType:
        """Corresponding Spark datatype for this class."""
        return self._spark_type_class()

    @property
    def spark_struct_field(self) -> StructField:
        """The StructField for this object."""
        return StructField(name=self.field_name, dataType=self.spark_data_type, nullable=self.is_nullable)


class NumericField(AtomicField):
    """
    Numeric field type.

    In the Spark API types hierarchy:

    ```
    DataType
     |- AtomicType
         |- NumericType
     |- ...
    ```
    """

    @property
    @abstractmethod
    def _spark_type_class(self) -> Type[DataType]:
        """The class of the Spark type corresponding to this field."""


class IntegralField(NumericField):
    """
    Integral field type.

    In the Spark API types hierarchy:

    ```
    DataType
     |- AtomicType
         |- NumericType
             |- IntegralType
     |- ...
    ```
    """

    @property
    @abstractmethod
    def _spark_type_class(self) -> Type[DataType]:
        """The class of the Spark type corresponding to this field."""


class FractionalField(NumericField):
    """
    Integral field type.

    In the Spark API types hierarchy:

    ```
    DataType
     |- AtomicType
         |- NumericType
             |- FractionalType
     |- ...
    ```
    """

    @property
    @abstractmethod
    def _spark_type_class(self) -> Type[DataType]:
        """The class of the Spark type corresponding to this field."""
