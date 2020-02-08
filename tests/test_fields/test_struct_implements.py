"""
Suite of tests for Struct "implements".

Partner to `test_struct.py`.
"""

from sparkql import Struct, String

import pytest


class SimpleStruct(Struct):
    field_a = String()


class TestStructImplements:

    @staticmethod
    def should_reject_due_to_missing_fields():
        # given, when, then
        with pytest.raises(ValueError, match="wtf"):
            class ExampleStruct(Struct):
                class Meta:
                    implements = [SimpleStruct]

    @staticmethod
    def should_reject_due_to_incorrect_field_type():
        # given, when, then
        with pytest.raises(ValueError, match="wtf"):
            class ExampleStruct(Struct):
                class Meta:
                    implements = [SimpleStruct]
                field_a = String(nullable=False)

    @staticmethod
    def should_allow_struct_that_correctly_implements_fields():
        # given, when, then
        class ExampleStruct(Struct):
            class Meta:
                implements = [SimpleStruct]

            field_a = String()

    @staticmethod
    def should_allow_struct_whose_superclass_implements_fields():
        # given, when, then
        class SimpleBase(Struct):
            field_a = String()

        class ExampleStruct(SimpleBase):
            class Meta:
                implements = [SimpleStruct]
