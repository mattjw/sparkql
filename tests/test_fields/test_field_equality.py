import pytest

from sparkql import Struct, String


class ObjectA(Struct):
    string_field = String()


class ObjectB(Struct):
    string_field = String()


class ObjectC(Struct):
    string_field = String()
    extra_field = String()


@pytest.mark.only
class TestEquality:
    @staticmethod
    def test_two_atomics_are_equal():
        field_a = String()
        field_b = String()
        assert field_a == field_b

    @staticmethod
    def test_two_structs_are_equal():
        assert ObjectA() == ObjectB()

    @staticmethod
    def test_two_structs_are_not_equal():
        assert ObjectA() != ObjectC()
