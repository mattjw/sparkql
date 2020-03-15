import re

import pytest

from sparkql import Array, Float, String
from sparkql.exceptions import FieldValueValidationError
from tests.utilities import does_not_raise


class TestArrayFieldValidateOnValue:
    @staticmethod
    def should_reject_non_sequence():
        # given
        array_of_floats_field = Array(Float())
        value = {}

        # when, then
        with pytest.raises(
            FieldValueValidationError, match=re.escape("Value for an array must be a sequence, not 'dict'")
        ):
            array_of_floats_field._validate_on_value(value)

    @staticmethod
    def should_reject_invalid_element_value():
        # given
        array_of_floats_field = Array(Float())
        value = [3.5, "string"]

        # when, then
        with pytest.raises(
            FieldValueValidationError,
            match=re.escape("Value 'string' has invalid type 'str'. Allowed types are: 'float'"),
        ):
            array_of_floats_field._validate_on_value(value)

    @staticmethod
    @pytest.mark.parametrize(
        "array_field, value, expected_error",
        [
            pytest.param(
                Array(String(), name="name_of_array_field"),
                None,
                does_not_raise(),
                id="array-of-nullable-elemets-should-accept-none",
            ),
            pytest.param(
                Array(String(), name="name_of_array_field"),
                "this is a string value",
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape(
                        "Value for an array must not be a string. Found value 'this is a string value'. Did you mean "
                        "to use a list of strings?")),
                id="string-array-should-reject-non-sequence",
            ),
            pytest.param(
                Array(Float(), name="name_of_array_field"),
                3.5,
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape("Value for an array must be a sequence, not 'float'")),
                id="float-array-should-reject-non-sequence",
            ),
            pytest.param(
                Array(Float(nullable=False), name="name_of_array_field"),
                [None],
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape(
                        "Encountered None value in array, but the element field of this array is specified as "
                        "non-nullable (array field name = 'name_of_array_field')")
                ),
                id="float-array-of-non-nullable-elements-should-reject-null-element",
            ),
        ],
    )
    def test_arrays_should_be_handled_correctly(array_field: Array, value, expected_error):
        # given, when, then
        with expected_error:
            array_field._validate_on_value(value)
