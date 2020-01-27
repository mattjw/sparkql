"""Struct."""

from collections import OrderedDict
from dataclasses import dataclass
from typing import ClassVar, Sequence, Optional, Mapping, Iterable, Type, Any

from pyspark.sql import types as sql_types
from pyspark.sql.types import DataType, StructField

from ..exceptions import InvalidStructError
from .base import BaseField


@dataclass(frozen=True)
class StructClassMeta:
    """Metadata associated with a struct object; part of the underlying machinery of sparkql."""

    fields: Mapping[str, BaseField]
    spark_struct: sql_types.StructType
    includes: Optional[Sequence["Struct"]] = None  # ^ TO-DO  https://github.com/mattjw/sparkql/issues/17
    interfaces: Optional[Sequence["Struct"]] = None  # ^ TO-DO  https://github.com/mattjw/sparkql/issues/16


class Struct(BaseField):
    """A struct; shadows StructType in the Spark API."""

    _struct_meta: ClassVar[Optional[StructClassMeta]] = None

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
    def __extract_fields(cls) -> Mapping[str, BaseField]:
        fields = OrderedDict((key, value) for key, value in cls.__dict__.items() if isinstance(value, BaseField))
        for field_name, field in fields.items():
            field._set_contextual_name(field_name)  # pylint: disable=protected-access
        return fields

    @staticmethod
    def __build_spark_struct(fields: Iterable[BaseField]) -> sql_types.StructType:
        """Build a Spark struct (StructType) for a list of fields."""
        return sql_types.StructType([field._spark_struct_field for field in fields])  # pylint: disable=protected-access

    @classmethod
    def __init_subclass__(cls, **options):  # pylint: disable=unused-argument
        """Hook in to the subclassing of this base class; process fields when sub-classing occurs."""
        super().__init_subclass__()  # pytype: disable=attribute-error

        # Do not re-extract
        if cls._struct_meta is not None:
            return

        # Ensure a subclass does not break base class functionality
        for child_prop, child_val in cls.__dict__.items():
            if (child_prop in Struct.__dict__) and (isinstance(child_val, BaseField)):
                raise InvalidStructError(f"Field should note override inherited class properties: {child_prop}")

        # Extract fields
        fields = cls.__extract_fields()
        cls._struct_meta = StructClassMeta(fields=fields, spark_struct=Struct.__build_spark_struct(fields.values()))

    #
    # Handle dot chaining for full path ref to nested fields

    def __getattribute__(self, name):
        """
        Customise how field attributes are handled.

        Augment the attribute reference chain to ensure that a field's parent is set.
        """
        prop = super().__getattribute__(name)
        if not name.startswith("_") and isinstance(prop, BaseField):
            return prop._replace_parent(parent=self)
        return prop

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
