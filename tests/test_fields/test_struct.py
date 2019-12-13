import pytest

from sparkql.exceptions import InvalidStructObjectError
from sparkql import StructObject, StringField


class TestStructObject:

    @staticmethod
    def test_should_not_permit_field_overrides_of_internal_properties():
        # given

        # when, then
        with pytest.raises(InvalidStructObjectError):
            class Article(StructObject):
                _spark_type_class = StringField()

    @staticmethod
    def test_should_allow_override_of_functions():
        # given, when, then
        class Article(StructObject):
            @property
            def _spark_type_class(self):
                return None
