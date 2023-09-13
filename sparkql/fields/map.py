"""Map field."""

import copy
from typing import Optional, Generic, TypeVar, Any, TYPE_CHECKING, Type

from pyspark.sql.types import MapType, StructField

from sparkql.exceptions import FieldValueValidationError
from sparkql.fields.base import BaseField

if TYPE_CHECKING:
    from sparkql import Struct


KeyType = TypeVar("KeyType", bound=BaseField)  # pylint: disable=invalid-name
ValueType = TypeVar("ValueType", bound=BaseField)  # pylint: disable=invalid-name


class Map(Generic[KeyType, ValueType], BaseField):
    """
    Map field; shadows MapType in the Spark API.

    A spark schema generated with this map field will behave as follows:
    - Nullability of the `StructField` is given by `Map.nullable`, as per normal.
    - `containsNull` of the `MapType` is given by the nullability of the value contained
      within this field. In other words, `MapType.containsNull` is given by
      `Map.keyValue.nullable`.

    Attributes:
        keyType: Data type info for the key of this map. Should be an instance of a `BaseField`.
        valueType: Data type info for the map of this map. Should be an instance of a `BaseField`.
    """

    __hash__ = BaseField.__hash__

    keyType: KeyType  # pylint: disable=invalid-name
    valueType: ValueType  # pylint: disable=invalid-name

    def __init__(self, key_type: KeyType, value_type: ValueType, nullable: bool = True, name: Optional[str] = None):
        super().__init__(nullable=nullable, name=name)

        if not isinstance(key_type, BaseField):
            raise ValueError(f"Map key type must be a field. Found type: {type(key_type).__name__}")

        if not isinstance(value_type, BaseField):
            raise ValueError(f"Map value type must be a field. Found type: {type(value_type).__name__}")

        if key_type._resolve_field_name() is not None:
            # we require that the map's keyType should be a "vanilla" field. it should not have been given
            # any name (explicit nor contextual)
            raise ValueError(
                "When using a field as the key field of an map, the field should not have a name. "
                f"The field's name resolved to: {key_type._resolve_field_name()}"
            )
        if value_type._resolve_field_name() is not None:
            # we require that the map's value type should be a "vanilla" field. it should not have been given
            # any name (explicit nor contextual)
            raise ValueError(
                "When using a field as the value field of an map, the field should not have a name. "
                f"The field's name resolved to: {value_type._resolve_field_name()}"
            )

            # hand down this map's explicit name to its child element
            # this is to ensure correct naming in path chaining (see `self._replace_parent` and `path_seq`)
        self.keyType = key_type._replace_explicit_name(name=self._explicit_name)  # pylint: disable=invalid-name
        self.valueType = value_type._replace_explicit_name(name=self._explicit_name)  # pylint: disable=invalid-name

    #
    # Field path chaining

    def _replace_parent(  # pytype: disable=name-error
        self, parent: Optional["Struct"] = None  # pytype: disable=invalid-annotation,name-error
    ) -> "Struct":  # pytype: disable=invalid-annotation,name-error
        """Return a copy of this map with the parent attribute set."""
        self_copy = copy.copy(self)
        self_copy._parent_struct = parent  # pylint: disable=protected-access
        self_copy.keyType = self.keyType._replace_parent(parent=parent)  # pylint: disable=protected-access
        self_copy.valueType = self.valueType._replace_parent(parent=parent)  # pylint: disable=protected-access
        return self_copy

    #
    # Field name management

    def _set_contextual_name(self, value: str) -> None:
        super()._set_contextual_name(value)
        # set child to same name as parent; i.e., propagate contextual name downwards:
        self.keyType._set_contextual_name(value)  # pylint: disable=protected-access
        self.valueType._set_contextual_name(value)  # pylint: disable=protected-access

    #
    # Pass through to the element, for users who don't want to use the `.value_type` field

    def __getattribute__(self, attr_name: str) -> Any:
        """Custom get attribute behaviour."""
        if attr_name.startswith("_"):
            return super().__getattribute__(attr_name)

        try:
            attr_value = super().__getattribute__(attr_name)
        except AttributeError:
            attr_value = None

        if attr_value is not None:
            return attr_value

        return getattr(super().__getattribute__("valueType"), attr_name)

    @property
    def _spark_type_class(self) -> Type[MapType]:
        return MapType

    @property
    def _spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""
        # containsNull => is used to indicate if elements in a MapType value can have null values
        return StructField(
            name=self._field_name,
            dataType=MapType(
                # Note that we do not care about the key's and value's field name here:
                keyType=self.keyType._spark_struct_field.dataType,  # pylint: disable=protected-access
                valueType=self.valueType._spark_struct_field.dataType,  # pylint: disable=protected-access
                valueContainsNull=self.valueType._is_nullable,  # pylint: disable=protected-access
            ),
            nullable=self._is_nullable,
        )

    def _validate_on_value(self, value: Any) -> None:
        super()._validate_on_value(value)
        if value is None:
            # super() will have already validate none vs nullability. if None, then it's safe to be none
            return

        if not isinstance(value, dict):
            raise FieldValueValidationError(
                f"Value for a Map must not be a '{type(value).__name__}' with value '{value}'. Please use a dict!"
            )
        for item_value in value.values():
            value_field = self.valueType
            if not value_field._is_nullable and item_value is None:  # pylint: disable=protected-access
                # to improve readability for errors, we preemptively validate the non-nullability of the map
                # value here
                msg = "Encountered None value in Map, but the value field of this map is specified as non-nullable"
                if self._resolve_field_name() is not None:
                    msg += f" (map field name = '{self._resolve_field_name()}')"
                raise FieldValueValidationError(msg)
            self.valueType._validate_on_value(item_value)  # pylint: disable=protected-access

    #
    # Misc

    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        return (
            super().__eq__(other)
            and isinstance(other, Map)
            and self.valueType == other.valueType
            and self.keyType == other.keyType
        )
