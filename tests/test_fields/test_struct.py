import pytest
from pyspark.sql.types import StructType, StructField, StringType

from sparkql.exceptions import InvalidStructError
from sparkql import Struct, String, schema


class TestStruct:
    @staticmethod
    def test_should_not_permit_field_overrides_of_internal_properties():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                _spark_type_class = String()

    @staticmethod
    def test_should_allow_override_of_functions():
        # given, when, then
        class Article(Struct):
            @property
            def _spark_type_class(self):
                return None


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
            field_c = String()
        composite_schema = schema(CompositeObject)

        # expect
        assert composite_schema == StructType([
            StructField("field_a", StringType()),
            StructField("field_b", StringType()),
            StructField("field_c", StringType()),
        ])
