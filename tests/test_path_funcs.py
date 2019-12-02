from sparkql import StringField, StructObject
from sparkql import path


class TestFieldNames:

    @staticmethod
    def test_should_return_correct_list_for_nested_schema():
        # given
        class User(StructObject):
            name = StringField()

        class Article(StructObject):
            author = User()

        # when
        path_field_names = path.field_names(Article.author.name)

        # then
        assert path_field_names == ["author", "name"]
