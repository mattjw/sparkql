import pytest

from sparkql import String, Array


class TestArrayField:
    @staticmethod
    def test_should_not_allow_element_with_explicit_name():
        # given
        element = String(name="explicit_name")

        # when, then
        with pytest.raises(
            ValueError, match="When using a field as the element field of an array, the field shoud not have a name."
        ):
            Array(element)
