import re

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType

from sparkql import pretty_schema


class TestPrettySchema:

    TEST_CASES = [
        pytest.param(
            StructType(
                [
                    StructField("str_a#etc", StringType()),
                    StructField("str b", StringType()),
                    StructField(
                        "object_a",
                        StructType([StructField("int_a", IntegerType()), StructField("int_b", IntegerType())]),
                    ),
                    StructField(
                        "array_a",
                        ArrayType(StructType([StructField("long_a", LongType()), StructField("long_b", LongType())])),
                    ),
                ]
            ),
            """StructType(List(
    StructField(str_a#etc,StringType,true),
    StructField(str b,StringType,true),
    StructField(object_a,
        StructType(List(
            StructField(int_a,IntegerType,true),
            StructField(int_b,IntegerType,true))),
        true),
    StructField(array_a,
        ArrayType(StructType(List(
            StructField(long_a,LongType,true),
            StructField(long_b,LongType,true))),true),
        true)))""",
            id="mixed nested structs and arrays",
        ),
        pytest.param(
            StructType([StructField("a", ArrayType(StringType()))]),
            """StructType(List(
    StructField(a,
        ArrayType(StringType,true),
        true)))""",
            id="simple array",
        ),
        pytest.param(
            StructType([StructField("a", ArrayType(StringType(), containsNull=False))]),
            """StructType(List(
    StructField(a,
        ArrayType(StringType,false),
        true)))""",
            id="array without nulls",
        ),
        pytest.param(StructType([]), "StructType(List())", id="empty schema"),
    ]

    @staticmethod
    @pytest.mark.parametrize("struct,expected_pretty_schema", TEST_CASES)
    def test_should_stringify_struct(struct, expected_pretty_schema):
        # given

        # when
        prettified = pretty_schema(struct)

        # then
        assert prettified == expected_pretty_schema

    @staticmethod
    @pytest.mark.parametrize("struct,_", TEST_CASES)
    def test_prettified_should_be_equivalent_to_stringified(struct, _):
        # check that the default spark representation is functionality
        # equivalent to the prettified string. remove superficial formatting

        # given
        spark_builtin_stringified = str(struct)

        # when
        prettified = pretty_schema(struct)
        deprettified = re.sub(r"\n(\s*)", "", prettified)

        # then
        assert spark_builtin_stringified == deprettified
