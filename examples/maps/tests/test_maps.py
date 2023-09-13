from sparkql import pretty_schema, schema
from .. import maps

class TestExampleMaps:

    @staticmethod
    def test_sparkql_stringified_schema():
        # given

        # when
        generated_schema = pretty_schema(schema(maps.Article))

        # then
        assert generated_schema == maps.prettified_schema.strip()
