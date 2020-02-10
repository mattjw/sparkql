import pytest

from sparkql import String, Array, path_str, Struct, Float


class TestArrayField:
    @staticmethod
    def should_not_allow_element_with_explicit_name():
        # given
        element = String(name="explicit_name")

        # when, then
        with pytest.raises(
            ValueError, match="When using a field as the element field of an array, the field shoud not have a name."
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
    def should_reject_non_field_element():
        # given
        bad_element = "this is a str, which is not a field"

        # when, then
        with pytest.raises(ValueError, match="Array element must be a field. Found type: str"):
            Array(bad_element)
