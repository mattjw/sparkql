from sparkql import Struct, Map, String


#
# Simple example of the nullability of a map value field vs
# nullability of values in a map

class Article(Struct):
    title = String(nullable=False)
    tags = Map(String(), String(), nullable=False)
    comments = Map(String(), String(nullable=False))


#
# Here's what the schema looks like

prettified_schema = """
StructType([
    StructField('title', StringType(), False), 
    StructField('tags', MapType(), False), 
    StructField('comments', MapType(), True)])
"""
