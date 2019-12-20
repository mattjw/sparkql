import pytest

from sparkql import StringField, ArrayField


class TestArrayField:
    @staticmethod
    def test_should_not_allow_element_with_explicit_name():
        # given
        element = StringField(name="explicit_name")

        # when, then
        with pytest.raises(
            ValueError, match="The element field of an array should not have an explicit name"
        ):
            ArrayField(element)
