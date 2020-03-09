"""Base field and abstract fields."""

from abc import ABC, abstractmethod
from typing import Optional, Type, Any, Tuple
import copy

from pyspark.sql import types as sql_type
from pyspark.sql.types import StructField, DataType

from sparkql.exceptions import FieldNameError, FieldParentError

# pytype: disable=invalid-annotation


def _validate_value_type_for_field(accepted_types: Tuple[Type, ...], value: Any):
    """Raise TypeError if `value` is not compatible with types; None values are always permitted."""
    if value is not None and not isinstance(value, accepted_types):
        pretty_types = " ,".join("'" + type.__name__ + "'" for type in accepted_types)
        raise TypeError(f"Value '{value}' has invalid type '{type(value).__name__}'. Allowed types are: {pretty_types}")


class BaseField(ABC):
    """Root of the field hierarchy; shadows DataType in the Spark API."""

    # Name management logic:
    # - Explicit name (`__name_explicit`): Set via constructor.
    # - Contextual name (`__name_contextual`): Inferred for the field as it is used in a struct object.
    #   This will always get set, although not immediately. The struct object that will contain this field
    #   is responsible for setting the contextual name.
    # The explicit name, if provided, will override the contextual name.

    __nullable: bool = True
    __name_explicit: Optional[str] = None
    __name_contextual: Optional[str] = None
    _parent_struct: Optional["Struct"] = None  # pytype: disable=name-error

    def __init__(self, nullable: bool = True, name: Optional[str] = None):
        """
        Constructor for a base field.

        Args:
            nullable: Is this field nullable.
            name: Field name. If None, field name will be identified via ivar context resolution.
        """
        self.__nullable = nullable
        self.__name_explicit = name

    #
    # Nullability

    @property
    def _is_nullable(self) -> bool:
        """The nullability status of this field."""
        return self.__nullable

    #
    # Field path chaining

    @property
    def _parent(self) -> Optional["Struct"]:  # pytype: disable=name-error
        return self._parent_struct

    def _replace_parent(self, parent: Optional["Struct"] = None) -> "BaseField":  # pytype: disable=name-error
        """Return a copy of this Field with the parent attribute set."""
        field = copy.copy(self)
        if self._parent_struct is not None:
            raise FieldParentError("Attempted to set parent field that has already been set")
        field._parent_struct = parent  # pylint: disable=protected-access
        return field

    #
    # Field name management

    @property
    def _contextual_name(self) -> Optional[str]:
        return self.__name_contextual

    def _set_contextual_name(self, value: str):
        # Intentionally not using an implicit setter here
        if self.__name_contextual is not None:
            raise FieldNameError(
                "Attempted to override a name that has already been set: "
                f"'{value}' replacing '{self.__name_contextual}'"
            )
        self.__name_contextual = value

    @property
    def _field_name(self) -> str:
        """The name for this field."""
        name = self._resolve_field_name()
        if name is None:
            raise FieldNameError(
                "No field name found among: explicit name = {}, inferred name = {}".format(
                    self.__name_explicit, self.__name_contextual
                )
            )
        return name

    def _resolve_field_name(self) -> Optional[str]:
        """
        Resolve name for this field, or None if no concrete name set.

        Should only be used by this class and its subclasses.
        """
        if self.__name_explicit is not None:
            return self.__name_explicit
        if self.__name_contextual is not None:
            return self.__name_contextual
        return None

    #
    # Spark type management

    @property
    @abstractmethod
    def _spark_type_class(self) -> Type[DataType]:
        """The class of the Spark type corresponding to this field."""

    @property
    @abstractmethod
    def _spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""

    @abstractmethod
    def _validate_on_value(self, value: Any) -> None:
        """
        Raises an error if `value` is not compatible with this field.

        Incompatibility may be due to incorrect nullability or incorrect type.
        """
        # for acceptable type declrations according to pytype, see pytype source code:
        #   types.py:1183
        #   _acceptable_types = {...}
        if not self._is_nullable and value is None:
            raise ValueError(f"Non-nullable field cannot have None value")

    #
    # Misc.

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        # Subclasses should call this as part of their equality checks
        return (
            isinstance(other, BaseField)
            and self._is_nullable == other._is_nullable
            and self._resolve_field_name() == other._resolve_field_name()  # may be None == None
            and self._spark_type_class == other._spark_type_class
        )

    def __str__(self):
        """Returns the name of this field."""
        # stringifying a field as its field adds some convenience for cases where we need the field
        # name
        return self._resolve_field_name()

    def _info(self):
        """String formatted object with a more complete summary of this field, primarily for debugging."""
        return (
            f"<{type(self).__name__}\n"
            f"  spark type = {self._spark_type_class.__name__}\n"
            f"  nullable = {self._is_nullable}\n"
            f"  name = {self._resolve_field_name()} <- {[self.__name_explicit, self.__name_contextual]}\n"
            f"  parent = {self._parent}\n"
            ">"
        )

    def _short_info(self):
        """Short info string for use in error messages."""
        return f"<{type(self).__name__}: nullable={self._is_nullable}>"


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
    def _spark_data_type(self) -> sql_type.DataType:
        """Corresponding Spark datatype for this class."""
        return self._spark_type_class()

    @property
    def _spark_struct_field(self) -> StructField:
        """The StructField for this object."""
        return StructField(name=self._field_name, dataType=self._spark_data_type, nullable=self._is_nullable)

    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        return (
            super().__eq__(other) and isinstance(other, AtomicField) and self._spark_data_type == other._spark_data_type
        )

    @abstractmethod
    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)


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

    @abstractmethod
    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)


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

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        _validate_value_type_for_field((int,), value)


class FractionalField(NumericField):
    """
    Fractional field type.

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

    @abstractmethod
    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
