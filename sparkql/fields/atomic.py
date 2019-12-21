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


class ByteField(IntegralField):
    """Field for Spark's ByteType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return ByteType


class IntegerField(IntegralField):
    """Field for Spark's IntegerType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return IntegerType


class LongField(IntegralField):
    """Field for Spark's LongType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return LongType


class ShortField(IntegralField):
    """Field for Spark's ShortType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return ShortType


#
# DataType -> AtomicType -> NumericType -> FractionalType


class DecimalField(FractionalField):
    """Field for Spark's DecimalType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DecimalType


class DoubleField(FractionalField):
    """Field for Spark's DoubleType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DoubleType


class FloatField(FractionalField):
    """Field for Spark's FloatType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return FloatType


#
# DataType -> AtomicType -> non-numeric types


class StringField(AtomicField):
    """Field for Spark's StringType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return StringType


class BinaryField(AtomicField):
    """Field for Spark's BinaryType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return BinaryType


class BooleanField(AtomicField):
    """Field for Spark's BooleanType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return BooleanType


class DateField(AtomicField):
    """Field for Spark's DateType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DateType


class TimestampField(AtomicField):
    """Field for Spark's TimestampType."""

    @property
    def _spark_type_class(self) -> Type[DataType]:
        return TimestampType
