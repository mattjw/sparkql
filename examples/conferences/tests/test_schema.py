from sparkql import schema, pretty_schema
from ..conferences import Conference, prettified_schema


def test_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(schema(Conference))

    # then
    assert generated_schema == prettified_schema.strip()
