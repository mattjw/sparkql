"""
Suite of tests for the Struct.Includes feature.

Partner to `test_struct.py`.
"""

from pyspark.sql.types import StructType, StructField, StringType

from sparkql.exceptions import InvalidStructError
from sparkql import Struct, String, schema, Integer, path_str

import pytest


#
# Test path and field handling


class SiblingStruct(Struct):
    sibling_field = String()


class RootStruct(Struct):
    class Includes:
        sibling = SiblingStruct()


class TestStructIncludes:
    @staticmethod
    def test_path_from_includes_field_in_includes():
        # given (see test cases), when
        field_path = path_str(RootStruct.Includes.sibling.sibling_field)

        # then
        assert field_path == "sibling_field"

    @staticmethod
    def test_path_from_includes_field_in_root_class():
        # given (see test cases), when
        field_path = path_str(RootStruct.sibling_field)

        # then
        assert field_path == "sibling_field"

    @staticmethod
    def test_field_from_includes_field_in_includes():
        # given (see test cases), when
        field = RootStruct.Includes.sibling.sibling_field

        # then
        assert field == String(name="sibling_field")

    @staticmethod
    def test_field_from_includes_field_in_root_class():
        # given (see test cases), when
        field = RootStruct.sibling_field

        # then
        assert field == String(name="sibling_field")


#
# Other tests


class TestStructIncludesSchemaBuilding:
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
