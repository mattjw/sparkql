"""Exceptions for this package."""


class FieldNameError(Exception):
    """A field name management error occurred."""


class FieldParentError(Exception):
    """A field-to-field-parent management error occurred."""


class InvalidStructError(Exception):
    """Invalid creation of a custom Struct subclass."""
