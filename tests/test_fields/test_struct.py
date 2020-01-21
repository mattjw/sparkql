import pytest

from sparkql.exceptions import InvalidStructObjectError
from sparkql import Struct, String


class TestStructObject:
    @staticmethod
    def test_should_not_permit_field_overrides_of_internal_properties():
        # given, when, then
        with pytest.raises(InvalidStructObjectError):

            class Article(Struct):
                _spark_type_class = String()

    @staticmethod
    def test_should_allow_override_of_functions():
        # given, when, then
        class Article(Struct):
            @property
            def _spark_type_class(self):
                return None
