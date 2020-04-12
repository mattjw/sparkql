import pytest
from pyspark.sql.types import StructType

from sparkql import merge_schemas


class MergeSchemas:
    @staticmethod
    @pytest.mark.parametrize(
        "schema_a, schema_b, expected_schema",
        [
            pytest.param(
                StructType(),
                StructType(),
                StructType(),
                id="empty-schemas"
            )
        ]
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
