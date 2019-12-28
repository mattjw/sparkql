"""Struct object."""

from collections import OrderedDict
from dataclasses import dataclass
from typing import ClassVar, Sequence, Optional, Mapping, Iterable, Type

from pyspark.sql import types as sql_types
from pyspark.sql.types import DataType, StructField

from ..exceptions import InvalidStructObjectError
from .base import BaseField


@dataclass(frozen=True)
class StructObjectClassMeta:
    """Metadata associated with a struct object; part of the underlying machinery of sparkql."""

    fields: Mapping[str, BaseField]
    spark_struct: sql_types.StructType
    includes: Optional[Sequence["StructObject"]] = None  # ^ TO-DO  https://github.com/mattjw/sparkql/issues/17
    interfaces: Optional[Sequence["StructObject"]] = None  # ^ TO-DO  https://github.com/mattjw/sparkql/issues/16


class StructObject(BaseField):
    """A struct object; shadows StructType in the Spark API."""

    _struct_object_meta: ClassVar[Optional[StructObjectClassMeta]] = None

    #
    # Handle Spark representations for a StructObject object

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return sql_types.StructType

    @property
    def spark_struct_field(self) -> StructField:
        """The Spark StructField for this field."""
        return StructField(
            name=self.field_name, dataType=self._struct_object_meta.spark_struct, nullable=self.is_nullable
        )

    #
    # Hook in to sub-class creation. Ensure fields are pre-processed when a sub-class is declared

    @classmethod
    def __extract_fields(cls) -> Mapping[str, BaseField]:
        fields = OrderedDict((key, value) for key, value in cls.__dict__.items() if isinstance(value, BaseField))
        for field_name, field in fields.items():
            field._contextual_name = field_name  # pylint: disable=protected-access
        return fields

    @staticmethod
    def __build_spark_struct(fields: Iterable[BaseField]) -> sql_types.StructType:
        """Build a Spark struct (StructType) for a list of fields."""
        return sql_types.StructType([field.spark_struct_field for field in fields])

    @classmethod
    def __init_subclass__(cls, **options):  # pylint: disable=unused-argument
        """Hook in to the subclassing of this base class; process fields when sub-classing occurs."""
        super().__init_subclass__()  # pytype: disable=attribute-error

        # Do not re-extract
        if cls._struct_object_meta is not None:
            return

        # Ensure a subclass does not break base class functionality
        for child_prop, child_val in cls.__dict__.items():
            if (child_prop in StructObject.__dict__) and (isinstance(child_val, BaseField)):
                raise InvalidStructObjectError(f"Field should note override inherited class properties: {child_prop}")

        # Extract fields
        fields = cls.__extract_fields()
        cls._struct_object_meta = StructObjectClassMeta(
            fields=fields, spark_struct=StructObject.__build_spark_struct(fields.values())
        )

    #
    # Handle dot chaining for full path ref to nested fields

    def __getattribute__(self, name):
        """
        Customise how field attributes are handled.

        Augment the attribute reference chain to ensure that a field's parent is set.
        """
        prop = super().__getattribute__(name)
        if not name.startswith("_") and isinstance(prop, BaseField):
            return prop.replace_parent(parent=self)
        return prop

    #
    # Other methods

    def __str__(self) -> str:
        """Nicely printable string representation."""
        return (
            f"<{type(self).__name__} \n"
            f"  spark type = {self._spark_type_class.__name__} \n"
            f"  nullable = {self.is_nullable} \n"
            f"  name = {self._resolve_field_name()} <- {[self._name_explicit, self._name_contextual]} \n"
            f"  parent = {self._parent} \n"
            f"  metadata = {self._struct_object_meta}"
            ">"
        )
