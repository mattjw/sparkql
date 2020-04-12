from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from sparkql import merge_schemas


schema_a = StructType([
    StructField("message", StringType()),
    StructField("author", ArrayType(
        StructType([
            StructField("name", StringType())
        ])
    ))
])

schema_b = StructType([
    StructField("author", ArrayType(
        StructType([
            StructField("address", StringType())
        ])
    ))
])

merged_schema = merge_schemas(schema_a, schema_b)


pretty_merged_schema = """
StructType(List(
    StructField(message,StringType,true),
    StructField(author,
        ArrayType(StructType(List(
            StructField(name,StringType,true),
            StructField(address,StringType,true))),true),
        true)))
"""
