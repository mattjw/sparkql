import re

import pytest

from sparkql import Array, Float
from sparkql.exceptions import FieldValueValidationError


class TestArrayFieldValidateOnValue:
    FIELD = Array(Float())

    @staticmethod
    def should_reject_non_sequence():
        # given
        value = {}

        # when, then
        with pytest.raises(
            FieldValueValidationError, match=re.escape("Value for an array must be a sequence, not 'dict'")
        ):
            TestArrayFieldValidateOnValue.FIELD._validate_on_value(value)

    @staticmethod
    def should_reject_invalid_element_value():
        # given
        value = [3.5, "string"]

        # when, then
        with pytest.raises(
            FieldValueValidationError,
            match=re.escape("Value 'string' has invalid type 'str'. Allowed types are: 'float'"),
        ):
            TestArrayFieldValidateOnValue.FIELD._validate_on_value(value)
