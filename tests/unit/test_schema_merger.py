import re

import pytest
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType

from sparkql import merge_schemas


class TestMergeSchemas:
    @staticmethod
    @pytest.mark.parametrize(
        "schema_a, schema_b, expected_schema",
        [
            pytest.param(StructType(), StructType(), StructType(), id="empty-schemas"),
            pytest.param(
                StructType([StructField("a_string", StringType())]),
                StructType([StructField("another_string", StringType())]),
                StructType([StructField("a_string", StringType()), StructField("another_string", StringType())]),
                id="disjoint-schemas",
            ),
            pytest.param(
                StructType([StructField("a_string", StringType())]),
                StructType([StructField("a_string", StringType()), StructField("another_string", StringType())]),
                StructType([StructField("a_string", StringType()), StructField("another_string", StringType())]),
                id="partially-overlapping-schemas",
            ),
            pytest.param(
                StructType([StructField("an_array", ArrayType(StructType([StructField("a_field", StringType())])))]),
                StructType(
                    [StructField("an_array", ArrayType(StructType([StructField("another_field", StringType())])))]
                ),
                StructType(
                    [
                        StructField(
                            "an_array",
                            ArrayType(
                                StructType(
                                    [StructField("a_field", StringType()), StructField("another_field", StringType())]
                                )
                            ),
                        )
                    ]
                ),
                id="arrays-of-disjoint-structs",
            ),
        ],
    )
    def should_successfully_merge_with(schema_a: StructType, schema_b: StructType, expected_schema: StructType):
        # given ^

        # when
        merged_schema = merge_schemas(schema_a, schema_b)

        # then
        assert merged_schema.jsonValue() == expected_schema.jsonValue()

        # ...expect distinct objects
        assert merged_schema is not schema_a
        assert merged_schema is not schema_b

    @staticmethod
    @pytest.mark.parametrize(
        "schema_a, schema_b, expected_error",
        [
            pytest.param(
                StructType([StructField("an_array", ArrayType(StringType()))]),
                StructType([StructField("an_array", ArrayType(FloatType()))]),
                pytest.raises(
                    ValueError,
                    match=re.escape("Cannot merge due to incompatibility in field 'an_array': Types must match. Type of A is StringType. Type of B is FloatType")),
                id="arrays-of-different-type",
            ),
            pytest.param(
                StructType([StructField("an_array", ArrayType(StringType(), containsNull=True))]),
                StructType([StructField("an_array", ArrayType(StringType(), containsNull=False))]),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Cannot merge due to incompatibility in field 'an_array': Arrays must have matching containsNull constraints. containsNull of array A is True. containsNull of array B is False")),
                id="arrays-of-different-containsNull",
            ),
            pytest.param(
                StructType([StructField("a_field", StringType(), nullable=True)]),
                StructType([StructField("a_field", StringType(), nullable=False)]),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Cannot merge due to incompatibility in field 'a_field': Fields must have matching nullability constraints. nullable of field A is True. nullable of field B is False")),
                id="fields-of-different-nullable",
            ),
            pytest.param(
                StructType([StructField("a_field", FloatType())]),
                StructType([StructField("a_field", StringType())]),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Cannot merge due to incompatibility in field 'a_field': Types must match. Type of A is FloatType. Type of B is StringType")),
                id="fields-of-different-type",
            ),
        ],
    )
    def should_fail_to_merge_with(schema_a: StructType, schema_b: StructType, expected_error):
        # given ^

        # when, then
        with expected_error:
            merge_schemas(schema_a, schema_b)
