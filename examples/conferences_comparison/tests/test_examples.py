from sparkql import pretty_schema, schema
from examples.conferences_comparison import plain_schema, sparkql_schema


def test_plain_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(plain_schema.CONFERENCE_SCHEMA)

    # then
    assert generated_schema == plain_schema.prettified_schema.strip()


def test_plain_munged_data():
    # given

    # when
    actual_rows = [row.asDict(True) for row in plain_schema.dframe.collect()]

    # then
    assert actual_rows == plain_schema.expected_rows


def test_sparkql_stringified_schema():
    # given

    # when
    generated_schema = pretty_schema(schema(sparkql_schema.Conference))

    # then
    assert generated_schema == sparkql_schema.prettified_schema.strip()


def test_sparkql_munged_data():
    # given

    # when
    actual_rows = [row.asDict(True) for row in sparkql_schema.dframe.collect()]

    # then
    assert actual_rows == sparkql_schema.expected_rows


def test_schemas_are_equivalent():
    assert schema(sparkql_schema.Conference) == plain_schema.CONFERENCE_SCHEMA
