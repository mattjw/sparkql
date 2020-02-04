"""Struct."""
from collections import OrderedDict
from dataclasses import dataclass
from typing import ClassVar, Optional, Mapping, Iterable, Type, Any, Generator, Tuple

from pyspark.sql import types as sql_types
from pyspark.sql.types import DataType, StructField

from ..exceptions import InvalidStructError
from .base import BaseField


class Struct(BaseField):
    """A struct; shadows StructType in the Spark API."""

    _struct_meta: ClassVar[Optional["StructInnerHandler"]] = None

    #
    # Handle Spark representations for a Struct object

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return sql_types.StructType

    @property
    def _spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""
        return StructField(name=self._field_name, dataType=self._struct_meta.spark_struct, nullable=self._is_nullable)

    #
    # Hook in to sub-class creation. Ensure fields are pre-processed when a sub-class is declared

    @classmethod
    def __init_subclass__(cls, **options):  # pylint: disable=unused-argument
        """Hook in to the subclassing of this base class; process fields when sub-classing occurs."""
        super().__init_subclass__()  # pytype: disable=attribute-error

        # Do not re-extract
        if cls._struct_meta is not None:
            return

        # Ensure a subclass does not break any base class functionality
        for child_prop, child_val in cls.__dict__.items():
            if (child_prop in Struct.__dict__) and (isinstance(child_val, BaseField)):
                raise InvalidStructError(f"Field should not override inherited class properties: {child_prop}")

        # Extract fields
        cls._struct_meta = StructInnerHandler(cls)

    #
    # Handle dot chaining for full path ref to nested fields

    def __getattribute__(self, attr_name):
        """
        Customise how field attributes are handled.

        Augment the attribute reference chain to ensure that a field's parent is set.
        """
        attr_value = super().__getattribute__(attr_name)

        if attr_name == "_struct_meta":
            return attr_value

        resolved_field = self._struct_meta.resolve_field(struct_object=self, attr_name=attr_name, attr_value=attr_value)
        if resolved_field is not None:
            return resolved_field

        return attr_value

    #
    # Other methods

    def _info(self):
        """String formatted object with a more complete summary of this field, primarily for debugging."""
        return (
            f"<{type(self).__name__} \n"
            f"  spark type = {self._spark_type_class.__name__} \n"
            f"  nullable = {self._is_nullable} \n"
            f"  name = {self._resolve_field_name()} <- {[self.__name_explicit, self.__name_contextual]} \n"
            f"  parent = {self._parent} \n"
            f"  metadata = {self._struct_meta}"
            ">"
        )

    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        return (
            super().__eq__(other)
            and isinstance(other, Struct)
            and self._struct_meta.fields == other._struct_meta.fields
            and list(self._struct_meta.fields.keys()) == list(other._struct_meta.fields.keys())
        )


@dataclass
class StructInnerHandler:
    """
    Management and retrieval of a Struct's fields (including Includes handling), and other magic.

    Definitions...

    - Root Struct: The Struct being managed by this Handler.
    - Native fields: Fields explicitly declared in the Struct.
    - Include class: The inner class that can be (optionally) specified within a Struct class. Found at
      `Struct.Includes`.
    - Include Struct: One of (possibly many) Structs specified in the root struct's Includes.
    - Includes fields: Fields pulled in from an Include Struct.
    """

    INCLUDES_CLASS_NAME: ClassVar[str] = "Includes"

    struct_class: Type[Struct]

    def __post_init__(self):
        # pylint: disable=attribute-defined-outside-init

        # native field name -> field
        self._native_fields = OrderedDict(self._yield_native_fields())
        for field_name, field in self._native_fields.items():
            field._set_contextual_name(field_name)  # pylint: disable=protected-access

        # includes field name -> Struct field
        self._include_structs = OrderedDict(self._yield_included_structs())

        # build canonical list of fields (combined native and includes)
        # field name -> field
        self._fields = OrderedDict(self._native_fields)

        for included_struct in self._include_structs.values():
            incl_fields = included_struct._struct_meta.fields  # pylint: disable=protected-access
            for incl_field_name, incl_field in incl_fields.items():
                if incl_field_name not in self._fields:
                    self._fields[incl_field_name] = incl_field  # populate `_fields`
                    setattr(self.struct_class, incl_field_name, incl_field)  # add included attrib to the struct class
                    # important bit here. we modify the struct class so that the includes fields are available
                    # from the struct, as well as (alternatively) via the Includes
                elif self._fields[incl_field_name] != incl_field:
                    raise InvalidStructError(
                        "Attempting to replace a field with an Includes field of different type. "
                        f"Incompatible field name: {incl_field_name}"
                    )

        # build spark struct
        self._spark_struct = StructInnerHandler._build_spark_struct(self._fields.values())

    @property
    def fields(self) -> Mapping[str, BaseField]:
        """
        All fields for the Struct, including both native fields and imported from `Includes` Structs.

        Returns:
            Mapping from field name to field.
        """
        return self._fields

    @property
    def spark_struct(self) -> sql_types.StructType:
        """Complete Spark StructType for the Struct; incorporates Includes."""
        return self._spark_struct

    @staticmethod
    def _build_spark_struct(fields: Iterable[BaseField]) -> sql_types.StructType:
        """Build a Spark struct (StructType) for a list of fields."""
        return sql_types.StructType([field._spark_struct_field for field in fields])  # pylint: disable=protected-access

    #
    # Extraction and processing of the Struct class

    def _yield_native_fields(self) -> Generator[Tuple[str, Struct], None, None]:
        """
        Get the native fields specified for this Struct class, if any.

        Yields:
            A `(str, BaseField)` pair for each field found in this class. Each pair consists of the attribute name and
            the field.
        """
        for attr_name, attr_value in self.struct_class.__dict__.items():
            if not isinstance(attr_value, BaseField):
                continue
            if attr_name.startswith("_"):
                raise InvalidStructError(
                    f"Fields must not begin with an underscore. Found: {attr_name} = {type(attr_value)}"
                )
            yield (attr_name, attr_value)

    def _get_includes_class(self) -> Optional[Type]:
        """Retrieve the `Includes` inner class, or None if none is provided."""
        if not hasattr(self.struct_class, StructInnerHandler.INCLUDES_CLASS_NAME):  # pytype: disable=wrong-arg-types
            return None
        includes_class = getattr(
            self.struct_class, StructInnerHandler.INCLUDES_CLASS_NAME  # pytype: disable=wrong-arg-types
        )

        if not isinstance(includes_class, type):
            raise InvalidStructError(
                "The 'Includes' property of a Struct must only be used as an inner class. "
                f"Found type {type(includes_class)}"
            )
        return includes_class

    def _yield_included_structs(self) -> Generator[Tuple[str, Struct], None, None]:
        """
        Get the Structs specified in the Includes, if any.

        Yields:
            A `(str, Struct)` pair for each Struct field found in the class's Includes.
            Each pair consists of the attribute name and the Struct.
            If Includes is not provided or Includes is empty, no yield.
        """
        if self._get_includes_class() is None:
            return
        for attr_name, attr_value in self._get_includes_class().__dict__.items():
            if attr_name.startswith("_"):
                continue
            if not isinstance(attr_value, Struct):
                raise InvalidStructError(
                    f"Encountered non-struct property in 'Includes' inner class: {attr_name} = {type(attr_value)}"
                )
            yield (attr_name, attr_value)

    #
    # Resolving of class attributes
    # pylint: disable=no-self-use
    def resolve_field(self, struct_object: "Struct", attr_name: str, attr_value: Any) -> Optional[BaseField]:
        """
        Attempt to resolve a `getattribute` call on the Struct, returning a BaseField if applicable.

        This should be used to hook into the Struct's `getattribute` behaviour to customise resolution of
        fields. Will also check if the `getattribute` call is attempting to resolve a field. If not, returns None.

        Args:
            struct_object: The instance of the Struct upon which `getattribute` was called.
            attr_name: The name of the `struct_object` attribute to be resolved.
            attr_value: The attribute of the `struct_object`.
        """
        if attr_name.startswith("_"):
            return None

        if isinstance(attr_value, BaseField):
            new_field: BaseField = attr_value._replace_parent(parent=struct_object)  # pylint: disable=protected-access
            return new_field

        return None
