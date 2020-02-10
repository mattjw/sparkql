import pytest

from sparkql.exceptions import FieldParentError
from sparkql import Float, Struct


class TestBaseField:
    @staticmethod
    def should_give_correct_info_string():
        # given
        float_field = Float()

        # when
        info_str = float_field._info()

        # then
        assert (
            info_str
            == "<Float \n  spark type = FloatType \n  nullable = True \n  name = None <- [None, None] \n  parent = None>"
        )

    @staticmethod
    def should_reject_setting_a_set_parent():
        # given
        struct = Struct()
        float_field = Float()._replace_parent(struct)

        another_struct = Struct()

        # when, then
        with pytest.raises(FieldParentError):
            float_field._replace_parent(another_struct)

    @staticmethod
    def should_get_contextual_field_name():
        # given
        float_field = Float()
        float_field._set_contextual_name("contextual_name")

        # when
        contextual_name = float_field._contextual_name

        # then
        assert contextual_name == "contextual_name"
