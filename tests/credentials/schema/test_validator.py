"""Unit tests for the cerberus-based credentials validator.

Cerberus does the heavy lifting; the validator here is a thin wrapper that
runs cerberus and returns its native errors dict.
"""

from __future__ import annotations

import pytest

from apollo.credentials.schema import validate


# A schema mirroring the Snowflake shape — required identity fields plus
# multiple mutually-exclusive auth modes expressed via cerberus's
# ``oneof_schema``. Used by the variant-selection tests below.
_SNOWFLAKE_LIKE: dict = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "oneof_schema": [
            {
                "user": {"type": "string", "required": True, "empty": False},
                "account": {"type": "string", "required": True, "empty": False},
                "password": {"type": "string", "required": True, "empty": False},
            },
            {
                "user": {"type": "string", "required": True, "empty": False},
                "account": {"type": "string", "required": True, "empty": False},
                "private_key": {"type": "string", "required": True, "empty": False},
            },
            {
                "user": {"type": "string", "required": True, "empty": False},
                "account": {"type": "string", "required": True, "empty": False},
                "oauth": {"type": "dict", "required": True, "allow_unknown": True},
            },
        ],
    },
}


def test_valid_credentials_return_empty_errors():
    errors = validate(
        _SNOWFLAKE_LIKE,
        {"connect_args": {"user": "u", "account": "a", "password": "p"}},
    )
    assert errors == {}


def test_missing_required_field_is_reported_by_cerberus():
    errors = validate(
        _SNOWFLAKE_LIKE,
        {"connect_args": {"user": "u", "password": "p"}},
    )
    assert "connect_args" in errors


def test_missing_all_variants_is_reported():
    # No auth field set — cerberus's oneof_schema lists every candidate so
    # the customer can see which auth combinations are valid.
    errors = validate(
        _SNOWFLAKE_LIKE,
        {"connect_args": {"user": "u", "account": "a"}},
    )
    assert "connect_args" in errors
    msg_str = str(errors["connect_args"])
    assert "password" in msg_str
    assert "private_key" in msg_str
    assert "oauth" in msg_str


def test_multiple_variants_present_is_rejected_as_ambiguous():
    # Supplying BOTH password AND private_key is ambiguous — cerberus's
    # oneof_schema rejects "more than one rule validates" by design.
    errors = validate(
        _SNOWFLAKE_LIKE,
        {
            "connect_args": {
                "user": "u",
                "account": "a",
                "password": "p",
                "private_key": "k",
            }
        },
    )
    assert "connect_args" in errors


def test_satisfying_exactly_one_variant_is_enough():
    errors = validate(
        _SNOWFLAKE_LIKE,
        {
            "connect_args": {
                "user": "u",
                "account": "a",
                "oauth": {"grant_type": "client_credentials", "scope": "x"},
            },
        },
    )
    assert errors == {}


def test_unknown_fields_at_root_are_allowed():
    # allow_unknown=True at the root means unrecognised top-level keys do
    # not cause an error. Forward compat for new self-hosted features.
    errors = validate(
        _SNOWFLAKE_LIKE,
        {
            "connect_args": {"user": "u", "account": "a", "password": "p"},
            "future_field": "anything",
        },
    )
    assert errors == {}


def test_non_dict_credentials_are_reported_clearly():
    # The validator is robust against non-dict input rather than crashing
    # — the route does not have to do its own type check.
    errors = validate(_SNOWFLAKE_LIKE, "not a dict")  # type: ignore[arg-type]
    assert "__root__" in errors


def test_schema_without_variants_runs_cerberus_normally():
    schema = {"host": {"type": "string", "required": True, "empty": False}}
    assert validate(schema, {"host": "h"}) == {}
    errs = validate(schema, {})
    assert "host" in errs


def test_string_or_dict_anyof_accepts_both():
    # Mirrors the SQL Server / Motherduck `connect_args` shape.
    schema = {
        "connect_args": {
            "required": True,
            "anyof": [
                {"type": "string", "empty": False},
                {
                    "type": "dict",
                    "schema": {"host": {"type": "string", "required": True}},
                },
            ],
        },
    }
    assert validate(schema, {"connect_args": "DRIVER={ODBC};SERVER=..."}) == {}
    assert validate(schema, {"connect_args": {"host": "h"}}) == {}
    bad = validate(schema, {"connect_args": ""})
    assert "connect_args" in bad


@pytest.mark.parametrize(
    "value, ok",
    [
        ({"connect_args": {"type": "service_account"}}, True),
        ({"connect_args": {"type": "user_account"}}, False),
    ],
)
def test_enum_constraint_via_allowed(value, ok):
    schema = {
        "connect_args": {
            "type": "dict",
            "required": True,
            "schema": {
                "type": {"type": "string", "allowed": ["service_account"]},
            },
        },
    }
    errors = validate(schema, value)
    assert (errors == {}) is ok
