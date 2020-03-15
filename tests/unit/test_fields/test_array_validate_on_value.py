import re

import pytest

from sparkql import Array, Float, Struct, String
from sparkql.exceptions import FieldValueValidationError, StructInstantiationArgumentTypeError
from tests.utilities import does_not_raise


@pytest.mark.only
class TestArrayFieldValidateOnValue:
    ARRAY_FIELD = Array(Float())

    @staticmethod
    def should_reject_non_sequence():
        # given
        value = {}

        # when, then
        with pytest.raises(
            FieldValueValidationError, match=re.escape("Value for an array must be a sequence, not 'dict'")
        ):
            TestArrayFieldValidateOnValue.ARRAY_FIELD._validate_on_value(value)

    @staticmethod
    def should_reject_invalid_element_value():
        # given
        value = [3.5, "string"]

        # when, then
        with pytest.raises(
            FieldValueValidationError,
            match=re.escape("Value 'string' has invalid type 'str'. Allowed types are: 'float'"),
        ):
            TestArrayFieldValidateOnValue.ARRAY_FIELD._validate_on_value(value)

    @staticmethod
    @pytest.mark.parametrize(
        "kwargs, expected_error",
        [
            pytest.param({"text_sequence": None}, does_not_raise(), id="allow-none-in-nullable"),
            pytest.param(
                {"text_sequence": "this is a string value"},
                pytest.raises(IOError, match="xxx"),
                id="reject-non-sequence-string-in-array",
            ),
            pytest.param(
                {"float_sequence": 5.5},
                pytest.raises(
                    IOError, match="yy"
                ),
                id="reject-non-sequence-float-in-array",
            ),
            pytest.param(
                {"non_nullable_float_sequence": [None]},
                pytest.raises(
                    IOError, match="zz"
                ),
                id="reject-null-element-in-array-of-of-non-nullable-elements",
            ),
        ],
    )
    def test_arrays_should_be_handled_correctly(kwargs, expected_error):
        # given
        class AnObject(Struct):
            text_sequence = Array(String())
            float_sequence = Array(Float())
            non_nullable_float_sequence = Array(Float(nullable=False))

        # when, then
        with expected_error:
            AnObject.make_dict(**kwargs)
