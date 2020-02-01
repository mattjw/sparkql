"""
Suite of tests for the Struct.Includes feature.

Partner to `test_struct.py`.
"""

from pyspark.sql.types import StructType, StructField, StringType

from sparkql import Struct, String, schema


import pytest
@pytest.mark.only  # FIXME
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
        from sparkql import pretty_schema  # FIXME
        print(pretty_schema(composite_schema))  # FIXME
        assert composite_schema == StructType([
            StructField("native_field", StringType()),
            StructField("field_a", StringType()),
            StructField("field_b", StringType()),
        ])
