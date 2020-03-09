"""
Suite of tests for Struct make_dict.

Partner to `test_struct.py`.
"""

import re
from collections import OrderedDict
from typing import Mapping, Any

import pytest

from sparkql.exceptions import StructInstantiationArgumentsError
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
        dic = AnObject.make_dict(numeric_field=7.0, text_field="text_value")

        # then
        assert_ordered_dicts_equal(dic, {"explicit_text_name": "text_value", "explicit_numeric_name": 7})

    @staticmethod
    def should_fail_when_defaulting_a_non_nullable_to_null():
        # given
        args = ["text value"]
        kwargs = {}

        class AnObject(Struct):
            text = String(name="alt_name")
            numeric = Float(nullable=False)

        # when, then
        with pytest.raises(
            TypeError, match=re.escape("Non-nullable field cannot have None value (field name = 'numeric')")
        ):
            AnObject.make_dict(*args, **kwargs)

    @staticmethod
    def should_default_a_nullable_to_null():
        # given
        args = []
        kwargs = {"numeric": 3.4}

        class AnObject(Struct):
            text = String(name="alt_name")
            numeric = Float(nullable=False)

        # when
        dic = AnObject.make_dict(*args, **kwargs)

        # then
        assert_ordered_dicts_equal(dic, {"alt_name": None, "numeric": 3.4})

    @staticmethod
    @pytest.mark.parametrize(
        "args,kwargs,expected_error_message",
        [
            pytest.param(
                ["value"],
                {"text": "value", "numeric": 7.0},
                "There were struct properties with multiple values. Repeated properties: text \n"
                "Properties required by this struct are: text, numeric",
                id="surplus-mixed-args",
            ),
            pytest.param(
                ["value", 7.0, 3.0],
                {},
                "There were 1 surplus positional arguments. Values for surplus args: 3.0 \n"
                "Properties required by this struct are: text, numeric",
                id="surplus-positional-args",
            ),
            pytest.param(
                [],
                {"text": "value", "numeric": 7.0, "mystery_argument": "value"},
                "There were surplus keyword arguments: mystery_argument \n"
                "Properties required by this struct are: text, numeric",
                id="surplus-keyword-args",
            ),
            pytest.param(
                [],
                {"mystery_argument": "value"},
                "There were surplus keyword arguments: mystery_argument \n"
                "Properties required by this struct are: text, numeric\n"
                "Omitted struct properties were defaulted to null: text, numeric",
                id="all-fields-defaulted-is-ok-but-surplus-keyword-args-is-bad",
            ),
        ],
    )
    def should_raise_on_encountering_invalid_args(args, kwargs, expected_error_message):
        # given
        class AnObject(Struct):
            text = String(name="alt_name")
            numeric = Float()

        # when, then
        with pytest.raises(StructInstantiationArgumentsError, match=re.escape(expected_error_message)):
            AnObject.make_dict(*args, **kwargs)

    @staticmethod
    @pytest.mark.parametrize(
        "args,kwargs,expected_error_message",
        [
            pytest.param(
                [], {"text": None, "numeric": 7}, "Non-nullable field cannot have None value", id="none-in-nullable"
            )
        ],
    )
    def should_raise_on_encountering_invalid_arg_type(args, kwargs, expected_error_message):
        # given
        class AnObject(Struct):
            text = String(nullable=False)
            numeric = Float()

        # when, then
        with pytest.raises(TypeError, match=expected_error_message):
            AnObject.make_dict(*args, **kwargs)
