"""Base field and abstract fields."""

from abc import ABC, abstractmethod
from typing import Optional, Type, Any, Tuple, Sequence, Dict, TYPE_CHECKING, cast
import copy

from pyspark.sql import types as sql_type, Column
from pyspark.sql import functions as sql_funcs
from pyspark.sql.types import StructField, DataType
from sparkql.exceptions import FieldNameError, FieldParentError, FieldValueValidationError

if TYPE_CHECKING:
    from sparkql import Struct


def _validate_value_type_for_field(accepted_types: Tuple[Type[Any], ...], value: Any) -> None:
    """Raise error if `value` is not compatible with types; None values are always permitted."""
    if value is not None and not isinstance(value, accepted_types):
        pretty_types = " ,".join("'" + accepted_type.__name__ + "'" for accepted_type in accepted_types)
        raise FieldValueValidationError(
            f"Value '{value}' has invalid type '{value.__class__.__name__}'. Allowed types are: {pretty_types}"
        )


class BaseField(ABC):
    """Root of the field hierarchy; shadows DataType in the Spark API."""

    # Name management logic:
    # - Explicit name (`__name_explicit`): Set via constructor.
    # - Contextual name (`__name_contextual`): Inferred for the field as it is used in a struct object.
    #   This will always get set, although not immediately. The struct object that will contain this field
    #   is responsible for setting the contextual name.
    # The explicit name, if provided, will override the contextual name.

    # Placeholders for private instance variables received via the constructor. (Represented here
    # for convenience only; these will be overwritten.)
    __nullable: bool = True
    __name_explicit: Optional[str] = None
    __name_contextual: Optional[str] = None
    __metadata: Dict[str, Any] = {}  # must be overwritten in constructor

    # Placeholder for "protected" style variables. (Again, represented only for convenience.)
    _parent_struct: Optional["Struct"] = None

    def __init__(self, nullable: bool = True, name: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None):
        """
        Constructor for a base field.

        Args:
            nullable: Is this field nullable.
            name: Field name. If None, field name will be identified via ivar context resolution.
            metadata:
                Metadata for this field. Metadata is a native feature of Spark and PySpark, allowing a field to be
                annotated. If None, then metadata will be treated as an empty dictionary.
        """
        self.__nullable = nullable
        self.__name_explicit = name
        self.__metadata = {} if metadata is None else dict(metadata)

    #
    # Nullability

    @property
    def _is_nullable(self) -> bool:
        """The nullability status of this field."""
        return self.__nullable

    @property
    def _metadata(self) -> Dict[str, Any]:
        """The metadata of this field."""
        return self.__metadata

    #
    # Field path chaining

    @property
    def _parent(self) -> Optional["Struct"]:
        return self._parent_struct

    def _replace_parent(self, parent: Optional["Struct"] = None) -> "BaseField":
        """Return a copy of this Field with the parent attribute set."""
        field = copy.copy(self)
        if self._parent_struct is not None:
            raise FieldParentError("Attempted to set parent field that has already been set")
        field._parent_struct = parent  # pylint: disable=protected-access
        return field

    def _replace_explicit_name(self, name: Optional[str] = None) -> "BaseField":
        """
        Return a copy of this field, with the explicit name set.

        Should only be used for internal mechanics of handling name resolution during
        path chaining.
        """
        field: BaseField = copy.copy(self)
        if self.__name_explicit is not None:
            raise FieldNameError("Attempted to set an explicit name that has already been set")
        field.__name_explicit = name  # pylint: disable=protected-access,unused-private-member
        return field

    #
    # Field name management

    @property
    def _explicit_name(self) -> Optional[str]:
        return self.__name_explicit

    @property
    def _contextual_name(self) -> Optional[str]:
        return self.__name_contextual

    def _set_contextual_name(self, value: str) -> None:
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
            # pylint: disable=consider-using-f-string
            raise FieldNameError(
                "No field name found among: explicit name = {}, inferred name = {}".format(
                    self.__name_explicit, self.__name_contextual
                )
            )
        return name

    def _resolve_field_name(self, default: Optional[str] = None) -> Optional[str]:
        """
        Resolve name for this field, or None if no concrete name set.

        Should only be used by this class and its subclasses.
        """
        if self.__name_explicit is not None:
            return self.__name_explicit
        if self.__name_contextual is not None:
            return self.__name_contextual
        return default

    #
    # Public interface to accessing field names and paths

    @property
    def SEQ(self) -> Sequence[str]:
        """
        The sequence of items that constitute the path to this field.

        The result is context-specific and depends on the path to this field through nested structs (if any).
        """
        fields = [self]
        parent = self._parent  # pylint: disable=protected-access
        while parent is not None:  # pylint: disable=protected-access
            fields.insert(0, parent)
            parent = parent._parent  # pylint: disable=protected-access

        assert all(
            field._resolve_field_name() is not None for field in fields  # pylint: disable=protected-access
        ), f"Encountered an unset name while traversing path. Path is: {_pretty_path(fields)}"

        return [f._field_name for f in fields]  # pylint: disable=protected-access

    @property
    def COL(self) -> Column:
        """
        The Spark column pointing to this field.

        The result is context-specific and depends on the path to this field through nested structs (if any).
        """
        fields_seq = self.SEQ
        col: Column = sql_funcs.col(fields_seq[0])  # pylint: disable=no-member
        for col_field_name in fields_seq[1:]:
            col = col[col_field_name]
        return col

    @property
    def PATH(self) -> str:
        """
        The dot-delimited path to this field.

        The result is context-specific and depends on the path to this field through nested structs (if any).
        """
        return ".".join(self.SEQ)

    @property
    def NAME(self) -> str:
        """The name of this field."""
        return self._field_name

    @property
    def METADATA(self) -> Dict[str, Any]:
        """The metadata for this field."""
        return self._metadata

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
        if not self._is_nullable and value is None:
            msg = "Non-nullable field cannot have None value"
            if self._resolve_field_name() is not None:
                msg += f" (field name = '{self._resolve_field_name()}')"
            raise FieldValueValidationError(msg)

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
            and self._metadata == other._metadata  # may be None == None
        )

    def __str__(self) -> str:
        """Returns the name of this field."""
        # stringifying a field as its field adds some convenience for cases where we need the field
        # name
        return cast(str, self._resolve_field_name(""))

    def _info(self) -> str:
        """String formatted object with a more complete summary of this field, primarily for debugging."""
        return (
            f"<{self.__class__.__name__}\n"
            f"  spark type = {self._spark_type_class.__name__}\n"
            f"  nullable = {self._is_nullable}\n"
            f"  name = {self._resolve_field_name()} <- {[self.__name_explicit, self.__name_contextual]}\n"
            f"  parent = {self._parent}\n"
            f"  metadata = {self._metadata}\n"
            ">"
        )

    def _short_info(self) -> str:
        """Short info string for use in error messages."""
        nullable = "Nullable " if self._is_nullable else ""

        # Good candidate for python pattern matching once <3.10 support no longer required
        num_metadata_items = len(self.__metadata)
        if num_metadata_items == 0:
            metadata = ""
        elif num_metadata_items == 1:
            metadata = f" [with {num_metadata_items} metadata item]"
        else:
            metadata = f" [with {num_metadata_items} metadata items]"

        return f"<{nullable}{self.__class__.__name__}{metadata}: {self._resolve_field_name()}>"

    def __hash__(self) -> int:
        return hash((self._is_nullable, self._resolve_field_name(""), self._spark_type_class))

    def __repr__(self) -> str:
        return self._short_info()


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

    __hash__ = BaseField.__hash__

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
        return StructField(
            name=self._field_name,
            dataType=self._spark_data_type,
            nullable=self._is_nullable,
            metadata=self._metadata,
        )

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


def _pretty_path(path: Sequence[BaseField]) -> str:
    """Build pretty string of path, for debug and/or error purposes."""
    # pylint: disable=protected-access
    return "< " + " -> ".join(f"'{field._resolve_field_name()}' ({type(field).__name__})" for field in path) + " >"
