import re

import pytest

from sparkql import Map, Float, String
from sparkql.exceptions import FieldValueValidationError
from tests.utilities import does_not_raise


class TestMapFieldValidateOnValue:
    @staticmethod
    @pytest.mark.parametrize(
        "map_field, value, expected_error_message",
        [
            pytest.param(
                Map(String(), Float()),
                "text",
                "Value for a Map must not be a 'str' with value 'text'. Please use a dict!",
                id="map-is-not-a-dict",
            ),
            pytest.param(
                Map(String(), Float()),
                {"key": "3.5"},
                "Value '3.5' has invalid type 'str'. Allowed types are: 'float'",
                id="value-of-wrong-type",
            ),
            pytest.param(
                Map(String(), Float(nullable=False)),
                {"key": None},
                "Encountered None value in Map, but the value field of this map is specified as non-nullable",
                id="null-value-in-unnamed-map-that-accepts-nonnull-only",
            ),
        ],
    )
    def test_raise_validation_error_when(map_field: Map, value, expected_error_message: str):
        # given, when, then
        with pytest.raises(FieldValueValidationError, match=re.escape(expected_error_message)):
            map_field._validate_on_value(value)

    @staticmethod
    @pytest.mark.parametrize(
        "map_field, value, expected_error",
        [
            pytest.param(
                Map(String(), String(), name="name_of_map_field"),
                None,
                does_not_raise(),
                id="map-of-nullable-values-should-accept-none",
            ),
            pytest.param(
                Map(String(), String(), name="name_of_map_field"),
                {"key": "value"},
                does_not_raise(),
                id="map-of-proper-values-should-accept-none",
            ),
            pytest.param(
                Map(String(), String(), name="name_of_map_field"),
                "this is a string value",
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape(
                        "Value for a Map must not be a 'str' with value 'this is a string value'. Please use a dict!"
                    ),
                ),
                id="string-map-should-reject-non-dict",
            ),
            pytest.param(
                Map(String(), Float(), name="name_of_array_field"),
                3.5,
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape("Value for a Map must not be a 'float' with value " "'3.5'. Please use a dict!"),
                ),
                id="float-array-should-reject-non-sequence",
            ),
            pytest.param(
                Map(String(), Float(nullable=False), name="name_of_map_field"),
                {"key": None},
                pytest.raises(
                    FieldValueValidationError,
                    match=re.escape(
                        "Encountered None value in Map, but the value field of this map is specified as "
                        "non-nullable (map field name = 'name_of_map_field')"
                    ),
                ),
                id="float-map-of-non-nullable-elements-should-reject-null-value",
            ),
        ],
    )
    def test_maps_should_be_handled_correctly(map_field: Map, value, expected_error):
        # given, when, then
        with expected_error:
            map_field._validate_on_value(value)
