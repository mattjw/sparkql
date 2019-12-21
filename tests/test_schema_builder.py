from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, ArrayType

from sparkql import StringField, ArrayField, FloatField, StructObject, TimestampField, schema


class TestSparkSchema:
    @staticmethod
    def test_should_structise_flat_object():
        # given
        class User(StructObject):
            id = StringField(nullable=False)
            age = FloatField()
            full_name = StringField(name="name")

        # when
        struct = schema(User)

        # then
        assert struct == StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField("age", FloatType()),
                StructField("name", StringType()),
            ]
        )

    @staticmethod
    def test_should_structise_deep_object():
        # given
        class User(StructObject):
            id = StringField(nullable=False)
            age = FloatField()
            full_name = StringField(name="name")

        class Article(StructObject):
            author = User(name="article_author", nullable=False)
            title = StringField(nullable=False)
            date = TimestampField()

        # when
        struct = schema(Article)

        # then
        assert struct == StructType(
            [
                StructField(
                    "article_author",
                    nullable=False,
                    dataType=StructType(
                        [
                            StructField("id", StringType(), nullable=False),
                            StructField("age", FloatType()),
                            StructField("name", StringType()),
                        ]
                    ),
                ),
                StructField("title", StringType(), nullable=False),
                StructField("date", TimestampType()),
            ]
        )

    @staticmethod
    def test_should_structise_object_containing_array_of_objects():
        # given
        class Tag(StructObject):
            id = StringField(nullable=False)
            name = StringField()

        class Article(StructObject):
            id = StringField(nullable=False)
            tags = ArrayField(Tag(nullable=True))

        # when
        struct = schema(Article)

        # then
        assert struct == StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField(
                    "tags",
                    ArrayType(
                        containsNull=True,
                        elementType=StructType(
                            [StructField("id", StringType(), nullable=False), StructField("name", StringType())]
                        ),
                    ),
                    nullable=True,
                ),
            ]
        )
