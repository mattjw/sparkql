"""
Suite of tests for the Struct.Includes feature.

Partner to `test_struct.py`.
"""

from pyspark.sql.types import StructType, StructField, StringType

from sparkql.exceptions import InvalidStructError
from sparkql import Struct, String, schema, Integer, path_str, Float

import pytest


#
# Test path and field handling


class CousinStruct(Struct):
    cousin_field = String()


class SiblingAStruct(Struct):
    class Includes:
        cousin = CousinStruct()

    sibling_a_field = String()


class SiblingBStruct(Struct):
    sibling_b_field = Float()


class RootStruct(Struct):
    class Includes:
        sibling_a = SiblingAStruct()
        sibling_b = SiblingBStruct()


class TestStructIncludes:
    @staticmethod
    def test_should_give_correct_path_when_referring_to_field_via_source_class():
        assert path_str(RootStruct.sibling_a_field) == "sibling_a_field"
        assert path_str(RootStruct.sibling_b_field) == "sibling_b_field"
        assert path_str(RootStruct.cousin_field) == "cousin_field"

    @staticmethod
    def test_should_give_correct_field_object_when_referring_to_field_via_source_class():
        assert RootStruct.sibling_a_field == String(name="sibling_a_field")
        assert RootStruct.sibling_b_field == Float(name="sibling_b_field")
        assert RootStruct.cousin_field == String(name="cousin_field")


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
