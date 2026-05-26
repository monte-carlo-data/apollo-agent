"""End-to-end tests for POST /api/v1/self-hosted-credentials/validate/<connection_type>.

Exercises the wiring between the Flask route, the CredentialsFactory fetch
path (with mocked providers), and the schema validator. Every test goes
through the self-hosted-credentials wrapper because the endpoint rejects
inline credentials — that path-rejection itself is covered by a dedicated
test below.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import patch

import pytest


@pytest.fixture(scope="module")
def client():
    """Build a Flask test client, isolating the side effects of importing
    ``apollo.interfaces.generic.main``.

    The module mutates the root logger's handler formatters at import time
    (wrapping them with ``RedactFormatterWrapper``). pytest installs its
    own handlers for log capture; if we wrap them, subsequent test modules
    that emit ``logger.error("...%s", args)`` blow up inside pytest's
    log capture path. We snapshot and restore handler formatters around
    the import so the wrap doesn't leak past this test module.
    """
    root = logging.getLogger()
    saved = [(h, h.formatter) for h in root.handlers]
    try:
        from apollo.interfaces.generic.main import app  # noqa: PLC0415

        yield app.test_client()
    finally:
        for handler, formatter in saved:
            handler.setFormatter(formatter)


def _asm_envelope() -> dict[str, Any]:
    """Build a minimal AWS Secrets Manager wrapper around an arbitrary ARN.

    Tests pair this with ``patch("apollo.credentials.asm.SecretsManagerProxyClient")``
    to drive the mocked ``get_secret_string`` return / raise behavior.
    """
    return {
        "self_hosted_credentials_type": "aws_secrets_manager",
        "aws_secret": "arn:aws:secretsmanager:us-east-1:123:secret:foo",
    }


def _post_validate(client, connection_type: str, secret_payload: dict[str, Any]) -> Any:
    """Run the validate endpoint with the secret store returning ``secret_payload``.

    Stubs out the ASM proxy client; the request envelope is fixed. Returns
    the parsed JSON response (with ``__mcd_result__`` unwrapped).
    """
    body = {"credentials": _asm_envelope()}
    with patch("apollo.credentials.asm.SecretsManagerProxyClient") as mock_client:
        mock_client.return_value.get_secret_string.return_value = json.dumps(
            secret_payload
        )
        r = client.post(
            f"/api/v1/self-hosted-credentials/validate/{connection_type}", json=body
        )
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()["__mcd_result__"]


def test_valid_snowflake_credentials(client):
    payload = _post_validate(
        client,
        "snowflake",
        {
            "connect_args": {
                "user": "USER",
                "account": "acct.us-east-1",
                "password": "secret",
            }
        },
    )
    assert payload["valid"] is True
    assert payload["connection_type"] == "snowflake"
    assert payload["errors"] == {}


def test_missing_required_field_returns_errors_at_200(client):
    payload = _post_validate(
        client,
        "snowflake",
        {"connect_args": {"user": "USER"}},
    )
    assert payload["valid"] is False
    assert "connect_args" in payload["errors"]


def test_unknown_connection_type_returns_400(client):
    r = client.post(
        "/api/v1/self-hosted-credentials/validate/bogus-connector",
        json={"credentials": _asm_envelope()},
    )
    assert r.status_code == 400


def test_malformed_body_returns_400(client):
    r = client.post(
        "/api/v1/self-hosted-credentials/validate/snowflake",
        data="not json",
        content_type="text/plain",
    )
    assert r.status_code == 400


def test_inline_credentials_without_wrapper_returns_400(client):
    # The endpoint is specifically for self-hosted credentials. Inline
    # credentials (no `self_hosted_credentials_type`) are rejected to
    # prevent the schema check from silently running outside the real
    # production fetch flow.
    body = {
        "credentials": {"connect_args": {"user": "u", "account": "a", "password": "p"}}
    }
    r = client.post("/api/v1/self-hosted-credentials/validate/snowflake", json=body)
    assert r.status_code == 400
    payload = r.get_json()
    assert "__mcd_error__" in payload
    assert "self_hosted_credentials_type" in payload["__mcd_error__"]


def test_aws_secrets_manager_fetch_failure_surfaces_as_400(client):
    # Simulate the customer's IAM role lacking GetSecretValue on the ARN.
    # The CredentialsFactory's ASM service wraps the underlying error into a
    # ValueError; the route catches it and returns the same __mcd_error__
    # envelope as execute_agent_operation.
    body = {"credentials": _asm_envelope()}
    with patch("apollo.credentials.asm.SecretsManagerProxyClient") as mock_client:
        mock_client.return_value.get_secret_string.side_effect = Exception(
            "AccessDeniedException: User is not authorized to perform: secretsmanager:GetSecretValue"
        )
        r = client.post("/api/v1/self-hosted-credentials/validate/snowflake", json=body)
    assert r.status_code == 400
    payload = r.get_json()
    assert "__mcd_error__" in payload
    assert "Failed to read self-hosted credentials" in payload["__mcd_error__"]
    assert "AccessDeniedException" in payload["__mcd_error__"]


def test_motherduck_string_form_accepted(client):
    # Docs document Motherduck connect_args as a string (DuckDB connection
    # string). The validator must accept it without parsing the string.
    payload = _post_validate(
        client,
        "motherduck",
        {"connect_args": "md:my_db?motherduck_token=abc123"},
    )
    assert payload["valid"] is True


def test_motherduck_dict_form_accepted(client):
    # The CTP also accepts a structured dict — validator covers both.
    payload = _post_validate(
        client,
        "motherduck",
        {"connect_args": {"db_name": "my_db", "token": "abc123"}},
    )
    assert payload["valid"] is True


def test_salesforce_crm_token_variant_only(client):
    payload = _post_validate(
        client,
        "salesforce-crm",
        {
            "connect_args": {
                "user": "user@example.com",
                "password": "pwd",
                "security_token": "tok",
            }
        },
    )
    assert payload["valid"] is True


def test_salesforce_crm_oauth_variant_only(client):
    payload = _post_validate(
        client,
        "salesforce-crm",
        {
            "connect_args": {
                "consumer_key": "ck",
                "consumer_secret": "cs",
                "domain": "myorg",
            }
        },
    )
    assert payload["valid"] is True


def test_salesforce_crm_no_variant_fails(client):
    payload = _post_validate(
        client,
        "salesforce-crm",
        {"connect_args": {"user": "user@example.com"}},
    )
    assert payload["valid"] is False
    # cerberus's oneof_schema flags the dict — every candidate variant is
    # listed under the connect_args error so the customer can see the choices.
    assert "connect_args" in payload["errors"]
