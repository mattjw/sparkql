import re

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

from sparkql.exceptions import InvalidDataFrameError
from sparkql.fields.struct import ValidationResult
from sparkql import Struct, String, Float, Array


class AnElement(Struct):
    value = Float()


class ComplexStruct(Struct):
    string_field = String()
    values = Array(AnElement())


class TestDataFrameValidation:
    @staticmethod
    def test_compliant_data_frame(spark_session: SparkSession):
        # given
        class SimpleStruct(Struct):
            string_field = String()

        dframe = spark_session.createDataFrame([{"string_field": "abc"}])

        # when
        validation_result = SimpleStruct.validate_data_frame(dframe)

        # expect
        assert validation_result == ValidationResult(
            True,
            "StructType(List(\n    StructField(string_field,StringType,true)))",
            "StructType(List(\n    StructField(string_field,StringType,true)))",
            "",
        )

    @staticmethod
    def test_should_raise_when_invalid(spark_session: SparkSession):
        # given
        class SimpleStruct(Struct):
            string_field = String()

        dframe = spark_session.createDataFrame([{"other_field": "abc"}])

        # when, then
        err_message = """Struct schema...

StructType(List(
    StructField(string_field,StringType,true)))

DataFrame schema...

StructType(List(
    StructField(other_field,StringType,true)))

Diff of struct -> data frame...

  StructType(List(
-     StructField(other_field,StringType,true)))
+     StructField(string_field,StringType,true)))"""
        with pytest.raises(InvalidDataFrameError, match=re.escape(err_message)):
            SimpleStruct.validate_data_frame(dframe).raise_on_invalid()

    @staticmethod
    def test_should_not_raise_when_valid(spark_session: SparkSession):
        # given
        class SimpleStruct(Struct):
            string_field = String()

        dframe = spark_session.createDataFrame([{"string_field": "abc"}])

        # when, then
        SimpleStruct.validate_data_frame(dframe).raise_on_invalid()

    @staticmethod
    def test_mismatched_data_frame_with_nested_data(spark_session: SparkSession):
        # given (above), and
        dframe = spark_session.createDataFrame(
            [{"other_string_field": "my string", "values": [{"value": 3.4}]}],
            schema=StructType(
                [
                    StructField("other_string_field", StringType()),
                    StructField("values", ArrayType(StructType([StructField("value", FloatType())]))),
                ]
            ),
        )

        # when
        validation_result = ComplexStruct.validate_data_frame(dframe)

        # expect
        assert validation_result == ValidationResult(
            False,
            """StructType(List(
    StructField(string_field,StringType,true),
    StructField(values,
        ArrayType(StructType(List(
            StructField(value,FloatType,true))),true),
        true)))""",
            """StructType(List(
    StructField(other_string_field,StringType,true),
    StructField(values,
        ArrayType(StructType(List(
            StructField(value,FloatType,true))),true),
        true)))""",
            """Struct schema...

StructType(List(
    StructField(string_field,StringType,true),
    StructField(values,
        ArrayType(StructType(List(
            StructField(value,FloatType,true))),true),
        true)))

DataFrame schema...

StructType(List(
    StructField(other_string_field,StringType,true),
    StructField(values,
        ArrayType(StructType(List(
            StructField(value,FloatType,true))),true),
        true)))

Diff of struct -> data frame...

  StructType(List(
-     StructField(other_string_field,StringType,true),
+     StructField(string_field,StringType,true),
      StructField(values,
          ArrayType(StructType(List(
              StructField(value,FloatType,true))),true),
          true)))""",
        )
