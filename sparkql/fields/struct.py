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
    fields: Mapping[str, BaseField]
    spark_struct: sql_types.StructType
    includes: Optional[Sequence["StructObject"]] = None  # TO-DO
    interfaces: Optional[Sequence["StructObject"]] = None  # TO-DO


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
        return StructField(
            name=self.field_name,
            dataType=self._struct_object_meta.spark_struct,
            nullable=self.is_nullable,
        )

    #
    # Hook in to sub-class creation. Ensure fields are pre-processed when a sub-class is declared

    @classmethod
    def __extract_fields(cls) -> Mapping[str, BaseField]:
        fields = OrderedDict(
            (key, value) for key, value in cls.__dict__.items() if isinstance(value, BaseField)
        )
        for field_name, field in fields.items():
            field._contextual_name = field_name
        return fields

    @staticmethod
    def __build_spark_struct(fields: Iterable[BaseField]) -> sql_types.StructType:
        """Build a Spark struct (StructType) for a list of fields."""
        return sql_types.StructType([field.spark_struct_field for field in fields])

    def __init_subclass__(cls, **options):
        super().__init_subclass__()

        # Do not re-extract
        if cls._struct_object_meta is not None:
            return

        # Ensure a subclass does not break base class functionality
        print("parent   ", StructObject.__dict__)
        print("child    ", cls.__dict__)  # FIXME

        for child_prop, child_val in cls.__dict__.items():
            if (child_prop in StructObject.__dict__) and (isinstance(child_val, BaseField)):
                raise InvalidStructObjectError(
                    f"Field should note override inherited class properties: {child_prop}"
                )

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

    def __str__(self):
        return (
            f"<{type(self).__name__} \n"
            f"  spark type = {self._spark_type_class.__name__} \n"
            f"  nullable = {self.is_nullable} \n"
            f"  name = {self._resolve_field_name()} <- {[self._name_explicit, self._name_contextual]} \n"
            f"  parent = {self._parent} \n"
            f"  metadata = {self._struct_object_meta}"
            ">"
        )
