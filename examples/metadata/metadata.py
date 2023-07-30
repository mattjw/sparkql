from sparkql import Struct, Array, String


#
# Example of attaching metadata to a field

class Article(Struct):
    title = String(nullable=False, metadata={'description': 'Title of the article'})
    tags = Array(String(), nullable=False, metadata={'enum': ['spark', 'sparkql']})
    comments = Array(String(nullable=False), metadata={'max_length': 100})


#
# Here's what the schema looks like

prettified_schema = """
StructType([
    StructField('title', StringType(), False, {'description': 'Title of the article'}), 
    StructField('tags', 
        ArrayType(StringType(), True), 
        False, 
        {'enum': ['spark', 'sparkql']}), 
    StructField('comments', 
        ArrayType(StringType(), False), 
        True, 
        {'max_length': 100})])
"""
