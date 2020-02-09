"""Struct."""

from collections import OrderedDict
from dataclasses import dataclass
from inspect import isclass
from typing import ClassVar, Optional, Mapping, Type, Any, Generator, Tuple, MutableMapping

from pyspark.sql import types as sql_types
from pyspark.sql.types import DataType, StructField

from ..exceptions import InvalidStructError, StructImplementationError
from .base import BaseField


class Struct(BaseField):
    """A struct; shadows StructType in the Spark API."""

    _struct_metadata: ClassVar[Optional["_StructInnerMetadata"]] = None

    #
    # Handle Spark representations for a Struct object

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return sql_types.StructType

    @property
    def _spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""
        return StructField(
            name=self._field_name, dataType=self._struct_metadata.spark_struct, nullable=self._is_nullable
        )

    #
    # Hook in to sub-class creation. Ensure fields are pre-processed when a sub-class is declared

    @classmethod
    def __init_subclass__(cls, **options):  # pylint: disable=unused-argument
        """Hook in to the subclassing of this base class; process fields when sub-classing occurs."""
        super().__init_subclass__(**options)  # pytype: disable=attribute-error

        # Ensure a subclass does not break any base class functionality
        for child_prop, child_val in cls.__dict__.items():
            if (child_prop in Struct.__dict__) and (isinstance(child_val, BaseField)):
                raise InvalidStructError(f"Field should not override inherited class properties: {child_prop}")

        # Extract internal metadata for this class, including parsing the fields
        cls._struct_metadata = _StructInnerMetadata.from_struct_class(cls)

        # Validate the "implements" (if any)
        _Validator(cls).validate()

    #
    # Handle dot chaining for full path ref to nested fields

    def __getattribute__(self, attr_name):
        """
        Customise how field attributes are handled.

        Augment the attribute reference chain to ensure that a field's parent is set.
        """
        attr_value = super().__getattribute__(attr_name)

        if attr_name == "_struct_metadata":
            return attr_value

        resolved_field = self._struct_metadata.resolve_field(
            struct_object=self, attr_name=attr_name, attr_value=attr_value
        )
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
            f"  metadata = {self._struct_metadata}"
            ">"
        )

    def __eq__(self, other: Any) -> bool:
        """True if `self` equals `other`."""
        return (
            super().__eq__(other)
            and isinstance(other, Struct)
            and self._struct_metadata.fields == other._struct_metadata.fields
            and list(self._struct_metadata.fields.keys()) == list(other._struct_metadata.fields.keys())
        )


@dataclass(frozen=True)
class _StructInnerMetadata:
    """Inner metadata object for Struct classes; hides complexity of field handling."""

    fields: Mapping[str, BaseField]
    # ^ All fields for the Struct, including both native fields and imported from includes

    @staticmethod
    def from_struct_class(struct_class: Type["Struct"]) -> "_StructInnerMetadata":
        """Build instance from a struct class."""
        return _StructInnerMetadata(fields=_FieldsExtractor(struct_class).extract())

    @property
    def spark_struct(self) -> sql_types.StructType:
        """Complete Spark StructType for the sparkql Struct."""
        return sql_types.StructType(
            [field._spark_struct_field for field in self.fields.values()]  # pylint: disable=protected-access
        )

    # Resolving of class attributes
    @staticmethod
    def resolve_field(struct_object: "Struct", attr_name: str, attr_value: Any) -> Optional[BaseField]:
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


#
# Supporting functionality

_META_INNER_CLASS_NAME = "Meta"


@dataclass
class _FieldsExtractor:
    """
    Extracts a Struct's fields from its class, also handling inner Meta, super class, and so on.

    Definitions...

    - Root Struct: The sub-class of Struct being managed by this Handler.
    - Native fields: Fields explicitly declared in the Struct.
    - Meta class: The inner class that can be (optionally) specified within a Struct sub-class. Found at
      `Struct.Meta`.
    - Include Struct: One of (possibly many) Structs specified in the root struct's `Meta.includes`.
    - Includes fields: Fields pulled in from `Meta.includes`.
    """

    INCLUDES_FIELD_NAME: ClassVar[str] = "includes"

    struct_class: Type[Struct]

    def extract(self) -> Mapping[str, BaseField]:
        """Extract the fields."""
        # pylint: disable=attribute-defined-outside-init

        # Extract and hand native fields
        # native field name -> field
        self._native_fields = OrderedDict(self._yield_native_fields())
        for field_name, field in self._native_fields.items():
            field._set_contextual_name(field_name)  # pylint: disable=protected-access

        # Start with super class fields, if any
        self._fields = OrderedDict(self._get_super_class_fields())

        # Add native fields
        self._fields = self._safe_combine_fields(self._fields, self._native_fields)

        # Add includes fields (if any)
        for included_struct in self._yield_included_structs():
            incl_fields = included_struct._struct_metadata.fields  # pylint: disable=protected-access
            for incl_field_name, incl_field in incl_fields.items():
                if incl_field_name not in self._fields:
                    self._fields[incl_field_name] = incl_field  # populate `_fields`
                    # Modify the root struct so that includes fields are available:
                    setattr(self.struct_class, incl_field_name, incl_field)
                elif self._fields[incl_field_name] != incl_field:
                    raise InvalidStructError(
                        "Attempting to replace a field with an 'includes' field of different type. "
                        f"From includes class: {type(included_struct)}. Incompatible field name: {incl_field_name}"
                    )

        return self._fields

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

    def _yield_included_structs(self) -> Generator[Struct, None, None]:
        """
        Get the Structs specified in the `Meta.includes`, if any.

        A class specified in `Meta.includes` is converted to an instance of the class before returning.

        Yields:
            A Struct object. Note that this is an instance of the Struct, not the class.
            If `Meta` or `Meta.includes` are not provided, no yield.
        """
        for struct in _yield_structs_from_meta(
            self.struct_class, self.INCLUDES_FIELD_NAME  # pytype: disable=wrong-arg-types
        ):
            yield struct

    #
    # Handle inheritance from super class

    def _get_super_class(self) -> Optional[Type]:
        """Obtain super class; or None if somehow no super class."""
        root_class = self.struct_class
        if len(root_class.__mro__) <= 1:
            return None
        return root_class.__mro__[1]

    def _get_super_struct_metadata(self) -> Optional[_StructInnerMetadata]:
        """Obtain super class's inner metadata; or None if none found."""
        super_class = self._get_super_class()
        if super_class is None or not hasattr(super_class, "_struct_metadata"):
            return None
        if super_class is Struct:
            # we know that the Struct class (i.e., the base for all custom Structs)
            return None

        super_struct_metadata = getattr(super_class, "_struct_metadata")
        if not isinstance(super_struct_metadata, _StructInnerMetadata):
            raise InvalidStructError(
                "Inner struct metadata of super class was not of correct type. "
                f"Encountered {type(super_struct_metadata)}"
            )

        return super_struct_metadata

    def _get_super_class_fields(self) -> Mapping[str, BaseField]:
        """Returns fields (mapping of attr_name -> field) from the super class; or empty if none found."""
        super_metadata = self._get_super_struct_metadata()
        if super_metadata is None:
            return OrderedDict()
        return OrderedDict(super_metadata.fields)

    #
    # Helpers

    @staticmethod
    def _safe_combine_fields(
        fields_a: Mapping[str, BaseField], fields_b: Mapping[str, BaseField]
    ) -> MutableMapping[str, BaseField]:
        """
        Add fields from `fields_b` into `fields_a`, returning a new copy.

        Only new fields are added. Checks that duplicate fields are identical.
        """
        combined = OrderedDict(fields_a)
        for key, value_b in fields_b.items():
            if key not in combined:
                combined[key] = value_b
            elif combined[key] != value_b:
                raise InvalidStructError(
                    f"Attempting to replace field '{key}' with field of different type: "
                    f"{type(fields_a[key])} vs {type(fields_b[key])}"
                )
        return combined


@dataclass
class _Validator:
    """
    Validate the "implements" requirement of a struct, if any provided.

    Assumes that the inner metadata of the class has been extracted.
    """

    IMPLEMENTS_FIELD_NAME: ClassVar[str] = "implements"

    struct_class: Type[Struct]

    def validate(self):
        """
        Validate that the meets "implements" requirements, if specified.

        Raises:
            StructImplementationError:
                If class does not correctly implement any required structs.
        """
        root_struct_metadata: _StructInnerMetadata = self.struct_class._struct_metadata  # pylint: disable=protected-access
        if root_struct_metadata is None:  # pylint: disable=protected-access
            raise ValueError(f"Struct class {self.struct_class} has not had its inner metadata extracted")

        for required_struct in self._yield_implements_structs():

            req_fields = (
                required_struct._struct_metadata.fields  # pylint: disable=protected-access  # pytype: disable=attribute-error
            )
            for req_field_name, req_field in req_fields.items():
                if req_field_name not in root_struct_metadata.fields:  # pytype: disable=attribute-error
                    raise StructImplementationError(
                        f"Struct '{self.struct_class.__name__}' does not implement field '{req_field_name}' "
                        f"required by struct '{type(required_struct).__name__}'"
                    )
                if req_field != root_struct_metadata.fields[req_field_name]:  # pytype: disable=attribute-error
                    # pylint: disable=protected-access
                    raise StructImplementationError(
                        f"Struct '{self.struct_class.__name__}' "  # pytype: disable=attribute-error
                        f"implements field '{req_field_name}' "
                        f"(required by struct '{type(required_struct).__name__}') but field is not compatible. "
                        f"Required {req_field._short_info()} "
                        f"but found {root_struct_metadata.fields[req_field_name]._short_info()}"
                    )

    def _yield_implements_structs(self) -> Generator[Struct, None, None]:
        """Get the Structs specified in the `Meta.implements`, if any."""
        for struct in _yield_structs_from_meta(
            self.struct_class, self.IMPLEMENTS_FIELD_NAME  # pytype: disable=wrong-arg-types
        ):
            yield struct


def _get_inner_meta_class(struct_class: Type[Struct]) -> Optional[Type]:
    """Retrieve the `Meta` inner class of a Struct class, or None if none is provided."""
    if not hasattr(struct_class, _META_INNER_CLASS_NAME):  # pytype: disable=wrong-arg-types
        return None
    inner_meta_class = getattr(struct_class, _META_INNER_CLASS_NAME)

    if not isinstance(inner_meta_class, type):
        raise InvalidStructError(
            f"The '{_META_INNER_CLASS_NAME}' "
            "property of a Struct must only be used as an "
            f"inner class. Found type {type(inner_meta_class)}"
        )
    return inner_meta_class


def _yield_structs_from_meta(source_struct_class: Type[Struct], attribute_name: str) -> Generator[Struct, None, None]:
    """
    Get a list of structs located at an attribute of the Meta inner class, if any.

    A class specified in the list of Structs is converted to an instance of the class before returning.

    Yields:
        A Struct object. Note that this is an instance of the Struct, not the class.
        If `Meta` or the attribute `attribute_name` of `Meta` are not present, no yield.
    """
    inner_meta_class = _get_inner_meta_class(source_struct_class)
    if inner_meta_class is None:
        return

    struct_classes = getattr(inner_meta_class, attribute_name, None)
    if struct_classes is None:
        return

    for index, struct_class in enumerate(struct_classes):
        if not isclass(struct_class):
            raise InvalidStructError(
                f"Encountered non-class item in '{attribute_name}' list of 'Meta' inner class. "
                f"Item at index {index}: '{struct_class}' with type {type(struct_class)}"
            )

        struct_instance = struct_class()
        if not isinstance(struct_instance, Struct):
            raise InvalidStructError(
                "Encountered item in 'includes' list of 'Meta' inner class that is not a Struct or Struct "
                f"subclass. Item at index {index} is {struct_class}"
            )

        yield struct_instance
