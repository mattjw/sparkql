import re
import pytest

from sparkql import String, Float, Struct, Map


class TestMapField:
    @staticmethod
    def test_not_allow_key_with_explicit_name():
        # given
        key = String(name="explicit_name")
        value = String()

        # when, then
        with pytest.raises(
            ValueError,
            match="When using a field as the key field of an map, the field should not have a name. "
            "The field's name resolved to: explicit_name",
        ):
            Map(key, value)

    @staticmethod
    def test_not_allow_value_with_explicit_name():
        # given
        key = String()
        value = String(name="explicit_name")

        # when, then
        with pytest.raises(
            ValueError,
            match="When using a field as the value field of an map, the field should not have a name. "
            "The field's name resolved to: explicit_name",
        ):
            Map(key, value)

    @staticmethod
    def test_enable_path_via_explicit_value_field():
        # given
        class ComplexElementStruct(Struct):
            string_field = String()
            float_field = Float()

        class OuterObject(Struct):
            map = Map(String(), ComplexElementStruct())

        # when
        path = OuterObject.map.valueType.string_field.PATH

        # then
        assert path == "map.string_field"

    @staticmethod
    def test_enable_path_via_passthrough():
        # given
        class ComplexElementStruct(Struct):
            string_field = String()
            float_field = Float()

        class OuterObject(Struct):
            map = Map(String(), ComplexElementStruct())

        # when
        path = OuterObject.map.string_field.PATH

        # then
        assert path == "map.string_field"

    @staticmethod
    def test_replace_parent_should_replace_parent_of_element():
        # given
        map_value = String()
        my_map = Map(String(), map_value, name="map")

        class ParentStruct(Struct):
            pass

        new_parent = ParentStruct()

        # when
        returned_map = my_map._replace_parent(new_parent)

        # then
        assert my_map._parent_struct is None
        assert returned_map is not my_map
        assert returned_map._parent_struct is new_parent
        assert isinstance(returned_map, Map)
        assert returned_map.valueType._parent_struct is new_parent

    @staticmethod
    def test_reject_non_field_key():
        # given
        bad_key = "this is a str, which is not a field"
        value = String()

        # when, then
        with pytest.raises(ValueError, match=re.escape("Map key type must be a field. Found type: str")):
            Map(bad_key, value)

    @staticmethod
    def test_reject_non_field_value():
        # given
        key = String()
        bad_value = "this is a str, which is not a field"

        # when, then
        with pytest.raises(ValueError, match=re.escape("Map value type must be a field. Found type: str")):
            Map(key, bad_value)

    @staticmethod
    @pytest.mark.parametrize(
        "instance",
        [
            pytest.param(Map(String(), Float(), True, "name"), id="nullable instance"),
            pytest.param(Map(String(), Float(), False, "name"), id="non-nullable instance"),
            pytest.param(Map(String(), Float(), False), id="non-nullable nameless instance"),
            pytest.param(Map(String(), Float()), id="instance with default constructor"),
        ],
    )
    def test_be_hashable(instance: Map):
        _field_can_be_used_as_a_key = {instance: "value"}

    @staticmethod
    def test_can_compare_to_other_map():
        # given
        map1 = Map(String(), Float())
        map2 = Map(String(), Float())

        # when, then
        assert map1 == map2
