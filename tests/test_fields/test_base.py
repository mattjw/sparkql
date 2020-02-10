from sparkql import Float


class TestBaseField:

    @staticmethod
    def should_give_correct_info_string():
        # given
        string_field = Float()

        # when
        info_str = string_field._info()

        # then
        assert info_str == "<Float \n  spark type = FloatType \n  nullable = True \n  name = None <- [None, None] \n  parent = None>"
