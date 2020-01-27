import pytest

from sparkql import Struct, String, Array, Float


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
        field = String()
        another_field = String()
        assert field == another_field

    @staticmethod
    def test_two_structs_are_equal():
        field = ObjectA()
        another_field = ObjectB()
        assert field == another_field

    @staticmethod
    def test_two_structs_are_not_equal():
        field = ObjectA()
        another_field = ObjectC()
        assert field != another_field

    @staticmethod
    def test_two_arrays_are_equal():
        field = Array(String())
        another_field = Array(String())
        assert field == another_field

    @staticmethod
    def test_two_arrays_with_different_names_are_not_equal():
        field = Array(String(), name="field_name")
        another_field = Array(String(), name="different_name")
        assert field != another_field

    @staticmethod
    def test_two_arrays_with_different_element_types_are_not_equal():
        field = Array(String())
        another_field = Array(Float())
        assert field != another_field
