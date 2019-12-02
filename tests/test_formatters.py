import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sparkql import pretty_schema


class TestPrettySchema:

    TEST_CASES = [
        (
            StructType([
                StructField("str_a", StringType()),
                StructField("str_b", StringType()),
                StructField("object_a", StructType([
                    StructField("int_a", IntegerType())
                ])),
            ]), """StructType(List(
    StructField(str_a,StringType,true),
    StructField(str_b,StringType,true),
    StructField(object_a,StructType(List(
            StructField(int_a,IntegerType,false)
        )), true
    ),
))"""
        )
    ]

    @staticmethod
    @pytest.mark.parametrize("struct,pretty_struct", TEST_CASES)
    def test_should_stringify_struct(struct, pretty_struct):
        assert pretty_schema(struct) == pretty_struct
