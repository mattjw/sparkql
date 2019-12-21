from sparkql import StringField, StructObject, ArrayField
from sparkql import path


class User(StructObject):
    full_name = StringField()


class Article(StructObject):
    author = User()


class Message(StructObject):
    sender = User()
    recipients = ArrayField(User())


class TestPathSeq:
    @staticmethod
    def test_should_return_correct_list_for_nested_schema():
        # given (see above)

        # when
        path_field_names = path.path_seq(Article.author.full_name)

        # then
        assert path_field_names == ["author", "full_name"]

    @staticmethod
    def test_should_return_correct_list_for_array():
        # given (see above)

        # when
        path_field_names = path.path_seq(Message.recipients.etype.full_name)

        # then
        assert path_field_names == ["recipients", "full_name"]


class TestPathStr:
    @staticmethod
    def test_should_return_correct_str_for_nested_schema():
        # given (see above)

        # when
        path_field_names = path.path_str(Article.author.full_name)

        # then
        assert path_field_names == "author.full_name"

    @staticmethod
    def test_should_return_correct_str_for_array():
        # given (see above)

        # when
        path_field_names = path.path_str(Message.recipients.etype.full_name)

        # then
        assert path_field_names == "recipients.full_name"


class TestPathCol:
    @staticmethod
    def test_should_return_correct_column_for_nested_schemas():
        # given (see above)

        # when
        column = path.path_col(Article.author.full_name)

        # then
        assert column == ["author", "name"]
