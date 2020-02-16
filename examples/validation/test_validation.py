import pytest
from pyspark.sql import SparkSession

from sparkql.exceptions import InvalidDataFrameError
from sparkql import Struct, String


def test_validation_example(spark_session: SparkSession):
    dframe = spark_session.createDataFrame([{"title": "abc"}])

    class Article(Struct):
        title = String()
        body = String()

    validation_result = Article.validate_data_frame(dframe)

    assert not validation_result.is_valid
    assert validation_result.report == """Struct schema...

StructType(List(
    StructField(title,StringType,true),
    StructField(body,StringType,true)))

Data frame schema...

StructType(List(
    StructField(title,StringType,true)))

Diff of struct -> data frame...

  StructType(List(
-     StructField(title,StringType,true)))
+     StructField(title,StringType,true),
+     StructField(body,StringType,true)))"""

    with pytest.raises(InvalidDataFrameError):
        Article.validate_data_frame(dframe).raise_on_invalid()
