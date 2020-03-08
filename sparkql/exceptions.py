"""Exceptions for this package."""


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


class InvalidStructInstanceArgumentsError(ValueError):
    """Incorrect arguments specified when creating an data object from a schema."""
