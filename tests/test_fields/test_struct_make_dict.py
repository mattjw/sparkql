"""
Suite of tests for Struct make_dict.

Partner to `test_struct.py`.
"""
from sparkql import Struct, String


class TestStructMakeDict:

    @staticmethod
    def should_make_dict_from_flat_schema():
        # given
        class AnObject(Struct):
            text = String(name="alternative_text_name")

        # when
        dic = AnObject.make_dict(text="value")

        # then
        assert dic == {"alternative_text_name": "value"}
