from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as sql_funcs

from sparkql import String, Struct, Array
from sparkql import path


class User(Struct):
    full_name = String()


class Article(Struct):
    author = User()


class Message(Struct):
    sender = User()
    recipients = Array(User())


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
    def test_should_return_correct_column_for_nested_schemas(spark_session: SparkSession):
        # spark_session: Testing of `path_col` has implicit JVM Spark dependency

        # given (see above)

        # when
        col_ref = path.path_col(Article.author.full_name)

        # then
        assert str(col_ref) == str(sql_funcs.col("author")["full_name"])
