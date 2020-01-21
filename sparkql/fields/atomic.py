"""Concrete atomic fields."""

from typing import Type

from pyspark.sql.types import (
    ByteType,
    IntegerType,
    LongType,
    ShortType,
    DecimalType,
    DoubleType,
    FloatType,
    StringType,
    BinaryType,
    BooleanType,
    DateType,
    TimestampType,
    DataType,
)

from .base import AtomicField, IntegralField, FractionalField


#
# DataType -> AtomicType -> NumericType -> IntegralType


class Byte(IntegralField):
    """Field for Spark's ByteType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return ByteType


class Integer(IntegralField):
    """Field for Spark's IntegerType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return IntegerType


class Long(IntegralField):
    """Field for Spark's LongType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return LongType


class Short(IntegralField):
    """Field for Spark's ShortType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return ShortType


#
# DataType -> AtomicType -> NumericType -> FractionalType


class Decimal(FractionalField):
    """Field for Spark's DecimalType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DecimalType


class Double(FractionalField):
    """Field for Spark's DoubleType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DoubleType


class Float(FractionalField):
    """Field for Spark's FloatType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return FloatType


#
# DataType -> AtomicType -> non-numeric types


class String(AtomicField):
    """Field for Spark's StringType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return StringType


class Binary(AtomicField):
    """Field for Spark's BinaryType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return BinaryType


class Boolean(AtomicField):
    """Field for Spark's BooleanType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return BooleanType


class Date(AtomicField):
    """Field for Spark's DateType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DateType


class Timestamp(AtomicField):
    """Field for Spark's TimestampType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return TimestampType
