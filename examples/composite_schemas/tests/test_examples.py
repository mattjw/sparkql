from sparkql import pretty_schema, schema

from .. import inheritance, includes


def test_inheritance_registration_schema():
    generated_schema = pretty_schema(schema(inheritance.RegistrationEvent))
    assert generated_schema == inheritance.prettified_registration_event_schema.strip()


def test_includes_registration_schema():
    generated_schema = pretty_schema(schema(includes.RegistrationEvent))
    assert generated_schema == includes.prettified_registration_event_schema.strip()
