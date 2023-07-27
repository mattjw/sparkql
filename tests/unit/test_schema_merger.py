import re

import pytest
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, DataType

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
            pytest.param(ArrayType(StringType()), ArrayType(StringType()), ArrayType(StringType()), id="root-arrays"),
        ],
    )
    def should_successfully_merge_struct_types_with(
        schema_a: StructType, schema_b: StructType, expected_schema: StructType
    ):
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
        "schema_a, schema_b, expected_schema",
        [
            pytest.param(
                ArrayType(StringType()), ArrayType(StringType()), ArrayType(StringType()), id="empty-root-arrays"
            )
        ],
    )
    def should_successfully_merge_array_types_with(
        schema_a: ArrayType, schema_b: ArrayType, expected_schema: ArrayType
    ):
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
                    match=re.escape(
                        "Cannot merge due to incompatibility in field 'an_array': Types must match. Type of A is StringType. Type of B is FloatType"
                    ),
                ),
                id="arrays-of-different-type",
            ),
            pytest.param(
                StructType([StructField("an_array", ArrayType(StringType(), containsNull=True))]),
                StructType([StructField("an_array", ArrayType(StringType(), containsNull=False))]),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Cannot merge due to incompatibility in field 'an_array': Arrays must have matching containsNull constraints. containsNull of array A is True. containsNull of array B is False"
                    ),
                ),
                id="arrays-of-different-containsNull",
            ),
            pytest.param(
                StructType([StructField("a_field", StringType(), nullable=True)]),
                StructType([StructField("a_field", StringType(), nullable=False)]),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Cannot merge due to incompatibility in field 'a_field': Fields must have matching nullability constraints. nullable of field A is True. nullable of field B is False"
                    ),
                ),
                id="fields-of-different-nullable",
            ),
            pytest.param(
                StructType([StructField("a_field", FloatType())]),
                StructType([StructField("a_field", StringType())]),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Cannot merge due to incompatibility in field 'a_field': Types must match. Type of A is FloatType. Type of B is StringType"
                    ),
                ),
                id="fields-of-different-type",
            ),
        ],
    )
    def should_fail_to_merge_array_types_with(schema_a: StructType, schema_b: StructType, expected_error):
        # given ^

        # when, then
        with expected_error:
            merge_schemas(schema_a, schema_b)

    @staticmethod
    @pytest.mark.parametrize(
        "schema_a, schema_b, expected_error",
        [
            pytest.param(
                ArrayType(StringType()),
                ArrayType(FloatType()),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Cannot merge due to incompatibility: Types must match. Type of A is StringType. Type of B is FloatType"
                    ),
                ),
                id="root-arrays-of-different-element-types",
            )
        ],
    )
    def should_fail_to_merge_struct_types_with(schema_a: ArrayType, schema_b: ArrayType, expected_error):
        # given ^

        # when, then
        with expected_error:
            merge_schemas(schema_a, schema_b)

    @staticmethod
    @pytest.mark.parametrize(
        "schema_a, schema_b, expected_error",
        [
            pytest.param(
                StructType([StructField("a_field", StringType(), metadata={"key": "value"})]),
                StructType([StructField("a_field", StringType(), metadata={"key": "another_value"})]),
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Cannot merge due to a conflict in field metadata. Metadata of field A is {'key': 'value'}. Metadata of field B is {'key': 'another_value'}. "
                    ),
                ),
                id="fields-with-duplicate-metadata-keys",
            )
        ],
    )
    def should_fail_to_merge_when_metadata_keys_are_not_unique(schema_a: ArrayType, schema_b: ArrayType, expected_error):
        # given ^

        # when, then
        with expected_error:
            merge_schemas(schema_a, schema_b)

    @staticmethod
    @pytest.mark.parametrize(
        "schema_a, schema_b, expected_schema",
        [
            pytest.param(
                StructType([StructField("a_field", StringType(), metadata={"key": "value", "another_key": "value"})]),
                StructType([StructField("a_field", StringType(), metadata={"key": "value"})]),
                StructType([StructField("a_field", StringType(), metadata={"key": "value", "another_key": "value"})]),
                id="fields-with-duplicate-metadata-keys-but-same-value",
            ),
        ],
    )
    def should_merge_when_shared_metadata_keys_have_same_value(schema_a: StructType, schema_b: StructType, expected_schema: StructType):
        # given ^

        merged_schema = merge_schemas(schema_a, schema_b)

        # then
        assert merged_schema.jsonValue() == expected_schema.jsonValue()

        # ...expect distinct objects
        assert merged_schema is not schema_a
        assert merged_schema is not schema_b
