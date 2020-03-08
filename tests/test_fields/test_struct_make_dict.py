"""
Suite of tests for Struct make_dict.

Partner to `test_struct.py`.
"""
from collections import OrderedDict
from typing import Mapping, Any

import pytest

from sparkql.exceptions import StructInstantiationArgumentsError, StructInstantiationTypeError
from sparkql import Struct, String, Float


def assert_ordered_dicts_equal(dict_a: Mapping[Any, Any], dict_b: Mapping[Any, Any]):
    assert dict_a == dict_b
    assert OrderedDict(dict_a) == OrderedDict(dict_b)


class TestStructMakeDict:
    @staticmethod
    def should_take_keyword_arg_and_resolve_property_name_to_explicit_name():
        # given
        class AnObject(Struct):
            text = String(name="explicit_text_field_name")

        # when
        dic = AnObject.make_dict(text="text_value")

        # then
        assert_ordered_dicts_equal(dic, {"explicit_text_field_name": "text_value"})

    @staticmethod
    def should_take_positional_arg():
        # given
        class AnObject(Struct):
            text = String()

        # when
        dic = AnObject.make_dict("text_value")

        # then
        assert_ordered_dicts_equal(dic, {"text": "text_value"})

    @staticmethod
    def should_obey_schema_ordering():
        # given
        class AnObject(Struct):
            text_field = String(name="explicit_text_name")
            numeric_field = Float(name="explicit_numeric_name")

        # when
        dic = AnObject.make_dict(numeric_field=7, text_field="text_value")

        # then
        assert_ordered_dicts_equal(dic, {"explicit_text_name": "text_value", "explicit_numeric_name": 7})

    @staticmethod
    @pytest.mark.parametrize(
        "args,kwargs,expected_error_message",
        [
            pytest.param(
                [],
                {"numeric": 7},
                "Some struct properties were not specified: text \n"
                "Properties required by this struct are: text, numeric",
                id="value-unspecified",
            ),
            pytest.param(
                ["value"],
                {"text": "value", "numeric": 7},
                "There were struct properties with multiple values. Repeated properties: text \n"
                "Properties required by this struct are: text, numeric",
                id="surplus-mixed-args",
            ),
            pytest.param(
                ["value", 7, 3],
                {},
                "There were 1 surplus positional arguments. Surplus values: 3 \n"
                "Properties required by this struct are: text, numeric",
                id="surplus-positional-args",
            ),
            pytest.param(
                [],
                {"text": "value", "numeric": 7, "mystery_argument": "value"},
                "There were surplus keyword arguments: mystery_argument \n"
                "Properties required by this struct are: text, numeric",
                id="surplus-keyword-args",
            ),
        ],
    )
    def should_raise_on_encountering_invalid_args(args, kwargs, expected_error_message):
        # given
        class AnObject(Struct):
            text = String(name="alt_name")
            numeric = Float()

        # when, then
        with pytest.raises(StructInstantiationArgumentsError, match=expected_error_message):
            AnObject.make_dict(*args, **kwargs)

    @staticmethod
    @pytest.mark.parametrize(
        "args,kwargs,expected_error_message",
        [
            pytest.param(
                [],
                {"text": None, "numeric": 7},
                "None in non-nullable field 'text' is not permitted",
                id="none-in-nullable",
            )
        ],
    )
    def should_raise_on_encountering_invalid_arg_type(args, kwargs, expected_error_message):
        # given
        class AnObject(Struct):
            text = String(nullable=False)
            numeric = Float()

        # when, then
        with pytest.raises(StructInstantiationTypeError, match=expected_error_message):
            AnObject.make_dict(*args, **kwargs)
