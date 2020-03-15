"""Utilities to support testing; tests do not live under here."""

from contextlib import contextmanager


@contextmanager
def does_not_raise():
    """For use in pytest """
    yield


__all__ = ["does_not_raise"]
