"""Array field."""

from pyspark.sql.types import ArrayType

from .base import BaseField


class ArrayField(BaseField):
    """Array field; shadows ArrayType in the Spark API."""

    @property
    def _spark_type_class(self):
        return ArrayType
