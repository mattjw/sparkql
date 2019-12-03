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
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return ByteType


class IntegerField(IntegralField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return IntegerType


class LongField(IntegralField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return LongType


class ShortField(IntegralField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return ShortType


#
# DataType -> AtomicType -> NumericType -> FractionalType


class DecimalField(FractionalField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DecimalType


class DoubleField(FractionalField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DoubleType


class FloatField(FractionalField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return FloatType


#
# DataType -> AtomicType -> non-numeric types


class StringField(AtomicField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return StringType


class BinaryField(AtomicField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return BinaryType


class BooleanField(AtomicField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return BooleanType


class DateField(AtomicField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return DateType


class TimestampField(AtomicField):
    @property
    def _spark_type_class(self) -> Type[DataType]:
        return TimestampType
