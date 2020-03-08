"""
Suite of tests for Struct make_dict.

Partner to `test_struct.py`.
"""

import pytest

from sparkql import Struct, String, Float


class AnObject(Struct):
    text = String(name="explicit_text_name")
    numeric = Float(name="explicit_numeric_name")
    more_text = String()


@pytest.mark.only
class TestStructMakeDict:

    @staticmethod
    def should_make_dict_from_flat_schema():
        # given, above

        # when
        dic = AnObject.make_dict(
            "text_value",
            more_text="more_text_value",
            numeric=123,
        )

        # then
        assert list(dic.items()) == list({
            "explicit_text_name": "text_value",
            "explicit_numeric_name": 123,
            "more_text": "more_text_value",
        }.items())
        # TODO ordering matters
