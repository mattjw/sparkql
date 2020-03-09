"""Exceptions for this package."""
from typing import List, Any


class FieldNameError(Exception):
    """A field name management error occurred."""


class FieldParentError(Exception):
    """A field-to-field-parent management error occurred."""


class InvalidStructError(Exception):
    """Invalid creation of a custom Struct subclass."""


class StructImplementationError(InvalidStructError):
    """A custom Struct subclass fails to correctly implement required fields."""


class InvalidDataFrameError(Exception):
    """A DataFrame does not match a schema."""


class StructInstantiationArgumentsError(ValueError):
    """Incorrect arguments specified when creating a data object from a schema."""

    def __init__(
        self,
        properties: List[str],
        unfilled_properties: List[str],
        duplicate_properties: List[str],
        surplus_positional_values: List[Any],
        surplus_keyword_args: List[str],
    ):
        msg = ""
        if unfilled_properties:
            msg += "Some struct properties were not specified: " + ", ".join(unfilled_properties) + " \n"
        if duplicate_properties:
            msg += (
                "There were struct properties with multiple values. Repeated properties: "
                + ", ".join(duplicate_properties)
                + " \n"
            )
        if surplus_positional_values:
            msg += (
                f"There were {len(surplus_positional_values)} surplus positional arguments. Surplus values: "
                + ", ".join(map(str, surplus_positional_values))
                + " \n"
            )
        if surplus_keyword_args:
            msg += "There were surplus keyword arguments: " + ", ".join(surplus_keyword_args) + " \n"
        msg += f"Properties required by this struct are: " + ", ".join(properties)
        super().__init__(msg)
