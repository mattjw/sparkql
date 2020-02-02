"""
Suite of tests for the Struct.Includes feature.

Partner to `test_struct.py`.
"""

from pyspark.sql.types import StructType, StructField, StringType

from sparkql.exceptions import InvalidStructError
from sparkql import Struct, String, schema, Integer

import pytest


class TestStructIncludes:
    @staticmethod
    def test_should_combine_disjoint_includes():
        # given
        class AnObject(Struct):
            field_a = String()

        class AnotherObject(Struct):
            field_b = String()

        # when
        class CompositeObject(Struct):
            class Includes:
                an_object = AnObject()
                another_object = AnotherObject()

            native_field = String()

        composite_schema = schema(CompositeObject)

        # expect
        assert composite_schema == StructType(
            [
                StructField("native_field", StringType()),
                StructField("field_a", StringType()),
                StructField("field_b", StringType()),
            ]
        )

    @staticmethod
    def test_should_combine_overlapping_includes():
        # given
        class AnObject(Struct):
            field_z = String()

        class AnotherObject(Struct):
            field_z = String()

        # when
        class CompositeObject(Struct):
            class Includes:
                an_object = AnObject()
                another_object = AnotherObject()

        composite_schema = schema(CompositeObject)

        # expect
        assert composite_schema == StructType([StructField("field_z", StringType())])

    @staticmethod
    def test_should_reject_incompatible_includes():
        # given
        class AnObject(Struct):
            field_z = String()

        class AnotherObject(Struct):
            field_z = Integer()

        # when, expect
        with pytest.raises(
            InvalidStructError, match="Attempting to replace a field with an Includes field of different type"
        ):

            class CompositeObject(Struct):
                class Includes:
                    an_object = AnObject()
                    another_object = AnotherObject()
