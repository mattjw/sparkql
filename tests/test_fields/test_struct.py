import re

import pytest

from sparkql.fields.struct import _Validator, _FieldsExtractor
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
    def test_should_not_permit_field_overrides_of_reserved_properties():
        # given, when, then
        with pytest.raises(InvalidStructError):

            class Article(Struct):
                validate_data_frame = String()

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
        class Empty(Struct):
            pass

        Empty._struct_metadata = None
        validator = _Validator(Empty)

        # when, then
        with pytest.raises(ValueError, match="has not had its inner metadata extracted"):
            validator.validate()

    @staticmethod
    def should_ensure_struct_class_inherits_from_struct():
        # given
        class Empty:
            pass

        # when, then
        with pytest.raises(ValueError, match="'struct_class' must inherit from Struct"):
            _Validator(Empty)

    @staticmethod
    def should_ensure_struct_class_is_not_struct():
        # given, when, then
        with pytest.raises(ValueError, match="'struct_class' must not be Struct"):
            _Validator(Struct)


class TestStructValidateOnValue:
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_error",
        [
            pytest.param(
                [],
                pytest.raises(ValueError, match=re.escape("Value for a struct must be a mapping, not 'list'")),
                id="value-is-wrong-type",
            ),
            pytest.param(
                {},
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Dict has incorrect number of fields. \nStruct requires 1 fields: text \nDict has 0 fields: "
                    ),
                ),
                id="not-enough-fields",
            ),
            pytest.param(
                {"wrong_name": "hello"},
                pytest.raises(
                    ValueError, match=re.escape("Dict fields do not match struct fields. \nStruct fields: text \nDict fields: wrong_name")
                ),
                id="incorrect-field-name",
            ),
            pytest.param(
                {"text": 3},
                pytest.raises(
                    TypeError, match=re.escape("Invalid type <class 'int'>. Allowed types are: 'str'")
                ),
                id="incorrect-field-type",
            ),
        ],
    )
    def should_reject_invalid_values(value, expected_error):
        # given
        class SimpleObject(Struct):
            text = String()

        struct = SimpleObject()

        # when
        with expected_error:
            struct._validate_on_value(value)


class TestFieldExtractor:
    @staticmethod
    def should_ensure_struct_class_inherits_from_struct():
        # given
        class Empty:
            pass

        # when, then
        with pytest.raises(ValueError, match="'struct_class' must inherit from Struct"):
            _FieldsExtractor(Empty)

    @staticmethod
    def should_ensure_struct_class_is_not_struct():
        # given, when, then
        with pytest.raises(ValueError, match="'struct_class' must not be Struct"):
            _FieldsExtractor(Struct)
