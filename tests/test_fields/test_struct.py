import pytest

from sparkql.fields.struct import _Validator
from sparkql.exceptions import InvalidStructError
from sparkql import Struct, String


class TestStruct:
    @staticmethod
    def test_should_not_permit_field_overrides_of_internal_properties():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                _spark_type_class = String()

    @staticmethod
    def test_should_not_permit_fields_starting_with_underscore():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                _name = String()

    @staticmethod
    def test_should_not_inner_meta_that_is_not_a_class():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                Meta = String()

    @staticmethod
    def test_should_allow_override_of_functions():
        # given, when, then
        class Article(Struct):
            @property
            def _spark_type_class(self):
                return None

    @staticmethod
    def test_should_handle_non_field_attribute_on_struct_instance():
        # given
        class Article(Struct):
            other_const = "HELLO"

        # when
        value = Article().other_const

        # then
        assert value == "HELLO"


class TestValidator:
    @staticmethod
    def should_reject_class_missing_metadata():
        # given
        class Empty:
            _struct_metadata = None

        validator = _Validator(Empty)

        # when, then
        with pytest.raises(ValueError, match="has not had its inner metadata extracted"):
            validator.validate()
