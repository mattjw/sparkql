from sparkql import pretty_schema
from .. import plain_schema


def test_plain_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(plain_schema.CONFERENCE_SCHEMA)

    # then
    assert generated_schema == plain_schema.prettified_schema.strip()


def test_plain_munged_data():
    # given

    # when
    actual_rows = [row.asDict(True) for row in plain_schema.df.collect()]

    # then
    assert actual_rows == plain_schema.expected_rows


def test_schemas_are_equivalent():
    # given


    # when
    actual_rows = [row.asDict(True) for row in df.collect()]

    # then
    assert actual_rows == expected_rows
