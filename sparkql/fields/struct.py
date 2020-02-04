"""Struct."""
from collections import OrderedDict
from dataclasses import dataclass
from inspect import isclass
from typing import ClassVar, Optional, Mapping, Iterable, Type, Any, Generator, Tuple, Sequence

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
    Management and retrieval of a Struct's fields (including Meta inner class handling), and other magic.

    Definitions...

    - Root Struct: The sub-class of Struct being managed by this Handler.
    - Native fields: Fields explicitly declared in the Struct.
    - Meta class: The inner class that can be (optionally) specified within a Struct sub-class. Found at
      `Struct.Meta`.
    - Include Struct: One of (possibly many) Structs specified in the root struct's `Meta.includes`.
    - Includes fields: Fields pulled in from `Meta.includes`.
    """

    META_INNER_CLASS_NAME: ClassVar[str] = "Meta"
    INCLUDES_FIELD_NAME: ClassVar[str] = "includes"

    struct_class: Type[Struct]

    def __post_init__(self):
        # pylint: disable=attribute-defined-outside-init

        # native field name -> field
        self._native_fields = OrderedDict(self._yield_native_fields())
        for field_name, field in self._native_fields.items():
            field._set_contextual_name(field_name)  # pylint: disable=protected-access

        # list of Structs whose fields are incorporated into the Root Struct
        self._include_structs = list(self._yield_included_structs())

        # build canonical list of fields (combined native and includes)
        # field name -> field
        self._fields = OrderedDict(self._native_fields)

        for included_struct in self._include_structs:
            incl_fields = included_struct._struct_meta.fields  # pylint: disable=protected-access
            for incl_field_name, incl_field in incl_fields.items():
                if incl_field_name not in self._fields:
                    self._fields[incl_field_name] = incl_field  # populate `_fields`
                    # Modify the root struct so that includes fields are available:
                    setattr(self.struct_class, incl_field_name, incl_field)
                elif self._fields[incl_field_name] != incl_field:
                    raise InvalidStructError(
                        "Attempting to replace a field with an 'includes' field of different type. "
                        f"Incompatible field name: {incl_field_name}"
                    )

        # build spark struct
        self._spark_struct = StructInnerHandler._build_spark_struct(self._fields.values())

    @property
    def fields(self) -> Mapping[str, BaseField]:
        """
        All fields for the Struct, including both native fields and imported from Include Structs.

        Returns:
            Mapping from field name to field.
        """
        return self._fields

    @property
    def spark_struct(self) -> sql_types.StructType:
        """Complete Spark StructType for the Struct; incorporates Include Structs."""
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

    def _get_inner_meta_class(self) -> Optional[Type]:
        """Retrieve the `Meta` inner class, or None if none is provided."""
        if not hasattr(self.struct_class, self.META_INNER_CLASS_NAME):
            return None
        inner_meta_class = getattr(self.struct_class, self.META_INNER_CLASS_NAME)

        if not isinstance(inner_meta_class, type):
            raise InvalidStructError(
                f"The '{StructInnerHandler.META_INNER_CLASS_NAME}' property of a Struct must only be used as an "
                f"inner class. Found type {type(inner_meta_class)}"
            )
        return inner_meta_class

    def _yield_included_structs(self) -> Generator[Struct, None, None]:
        """
        Get the Structs specified in the `Meta.includes`, if any.

        A class specified in `Meta.includes` is converted to an instance of the class before returning.

        Yields:
            A Struct object. Note that this is an instance of the Struct, not the class.
            If `Meta` or `Meta.includes` are not provided, no yield.
        """
        if self._get_inner_meta_class() is None:
            return

        include_struct_classes = getattr(self._get_inner_meta_class(), StructInnerHandler.INCLUDES_FIELD_NAME, None)
        if include_struct_classes is None:
            return

        for index, include_struct_class in enumerate(include_struct_classes):
            if not isclass(include_struct_class):
                raise InvalidStructError(
                    "Encountered non-class item in 'includes' list of 'Meta' inner class. "
                    f"Item at index {index} is {include_struct_class}"
                )

            include_struct = include_struct_class()
            if not isinstance(include_struct, Struct):
                raise InvalidStructError(
                    "Encountered item in 'includes' list of 'Meta' inner class that is not a Struct or Struct "
                    f"subclass. Item at index {index} is {include_struct_class}"
                )

            yield include_struct

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
