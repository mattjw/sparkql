from textwrap import indent

from pyspark.sql.types import StructType, StructField, ArrayType, DataType


# only time to start a new line (ever at all):
#   within StructType handler
#     this will make a newline, to begin list of fields
#     and put new line between each field
#     and add newline at the end
#
# depth := literally just defined as how many StructType deep we are
# there is no other reason do go deeper than moving within a new StructType
# the root StructType has depth = 0


class SparkSchemaPrettifier:
    PREFIX = "    "

    #
    # Entry point

    @classmethod
    def pretty_schema(cls, struct: StructType) -> str:
        return cls._pretty_struct_type(struct, depth=0)

    #
    # Recursive handlers

    @classmethod
    def _pretty_data_type(cls, dtype: DataType, depth: int) -> str:
        """Handle an instance of a data type."""
        if isinstance(dtype, StructType):
            # off load to the struct field
            # this is the only time we increment the depth
            return cls._pretty_struct_type(dtype, depth + 1)
        if isinstance(dtype, ArrayType):
            # recurse
            return f"ArrayType({cls._pretty_data_type(dtype.elementType, depth)})"
        # for all other data types, give the type name
        return dtype.__class__.__name__

    @classmethod
    def _pretty_struct_type(cls, struct: StructType, depth: int) -> str:
        """Handle the case where the data type is a struct type"""
        assert isinstance(struct, StructType)
        if not struct.fields:
            formatted = "StructType(List())"
        else:
            delim = "\n" + (cls.PREFIX * (depth + 1))
            lines = []
            for field in struct.fields:
                lines.append(f"{delim}{cls._pretty_struct_field(field, depth)}")
            formatted = "StructType(List({}))".format(",".join(lines))
        return formatted

    @classmethod
    def _pretty_struct_field(cls, field: StructField, depth: int) -> str:
        """Helper for `_pretty_struct_type`, that formats a field."""
        assert isinstance(field, StructField)
        formatted = "StructField({},{},{})".format(
            field.name,
            cls._pretty_data_type(field.dataType, depth),
            str(field.nullable).lower())
        return formatted


def pretty_schema(struct: StructType) -> str:
    """
    Returns a pretty stringified `struct`.

    This attempts to mimic StructType's default string formatter, but with the
    addition of white space, new lines, and indentation.
    """
    return SparkSchemaPrettifier.pretty_schema(struct)
