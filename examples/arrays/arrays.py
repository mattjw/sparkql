from sparkql import Struct, Array, String


#
# Simple example of the nullability of an array field vs
# nullability of elements in an array

class Article(Struct):
    title = String(nullable=False)
    tags = Array(String(), nullable=False)
    comments = Array(String(nullable=False))


#
# Here's what the schema looks like

prettified_schema = """
StructType(List(
    StructField(title,StringType,false),
    StructField(tags,
        ArrayType(StringType,true),
        false),
    StructField(comments,
        ArrayType(StringType,false),
        true)))
"""
