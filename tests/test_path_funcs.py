from sparkql import StringField, StructObject, ArrayField
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

    @staticmethod
    def test_should_return_correct_list_for_array():
        # given
        class User(StructObject):
            full_name = StringField()

        class Article(StructObject):
            authors = ArrayField(User())

        # when
        path_field_names = path.field_names(Article.authors.ArrayElem.full_name)

        # then
        assert path_field_names == ["authors", "full_name"]
