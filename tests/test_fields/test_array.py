import re

import pytest

from sparkql import String, Array, path_str, Struct, Float


class TestArrayField:
    @staticmethod
    def should_not_allow_element_with_explicit_name():
        # given
        element = String(name="explicit_name")

        # when, then
        with pytest.raises(
            ValueError, match="When using a field as the element field of an array, the field should not have a name."
        ):
            Array(element)

    @staticmethod
    def should_enable_path_via_explicit_element_field():
        # given
        class ComplexElementStruct(Struct):
            string_field = String()
            float_field = Float()

        class OuterObject(Struct):
            sequence = Array(ComplexElementStruct())

        # when
        path = path_str(OuterObject.sequence.e.string_field)

        # then
        assert path == "sequence.string_field"

    @staticmethod
    def should_enable_path_via_passthrough():
        # given
        class ComplexElementStruct(Struct):
            string_field = String()
            float_field = Float()

        class OuterObject(Struct):
            sequence = Array(ComplexElementStruct())

        # when
        path = path_str(OuterObject.sequence.string_field)

        # then
        assert path == "sequence.string_field"

    @staticmethod
    def should_replace_parent_should_replace_parent_of_element():
        # given
        array_element = String()
        array = Array(array_element, name="array")

        class ParentStruct(Struct):
            pass

        new_parent = ParentStruct()

        # when
        returned_array = array._replace_parent(new_parent)

        # then
        assert array._parent_struct is None
        assert returned_array is not array
        assert returned_array._parent_struct is new_parent
        assert isinstance(returned_array, Array)
        assert returned_array.e._parent_struct is new_parent

    @staticmethod
    def should_reject_non_field_element():
        # given
        bad_element = "this is a str, which is not a field"

        # when, then
        with pytest.raises(ValueError, match=re.escape("Array element must be a field. Found type: str")):
            Array(bad_element)


class TestArrayFieldValidateOnValue:
    FIELD = Array(Float())

    @staticmethod
    def should_reject_non_sequence():
        # given
        value = {}

        # when, then
        with pytest.raises(ValueError, match=re.escape("Value for an array must be a sequence, not 'dict'")):
            TestArrayFieldValidateOnValue.FIELD._validate_on_value(value)

    @staticmethod
    def should_reject_invalid_element_value():
        # given
        value = [3.5, "string"]

        # when, then
        with pytest.raises(TypeError, match=re.escape("Invalid type <class 'str'>. Allowed types are: 'float'")):
            TestArrayFieldValidateOnValue.FIELD._validate_on_value(value)
