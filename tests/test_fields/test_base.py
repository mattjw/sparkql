from sparkql import Float


class TestBaseField:

    @staticmethod
    def should_give_correct_info_string():
        # given
        string_field = Float()

        # when
        info_str = string_field._info()

        # then
        print(info_str)
        assert info_str == """<Float
  spark type = FloatType
  nullable = True
  name = None <- [None, None]
  parent = None>"""
