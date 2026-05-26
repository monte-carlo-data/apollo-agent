"""End-to-end tests for POST /api/v1/credentials/validate/<connection_type>.

Exercises the wiring between the Flask route, the CredentialsFactory fetch
path (with mocked providers), and the schema validator. Verifies that:

- A passthrough body (no self_hosted_credentials_type wrapper) is validated
  directly.
- An AWS Secrets Manager wrapper triggers a fetch and surfaces fetch
  failures as HTTP 400 with __mcd_error__ (same envelope as
  execute_agent_operation).
- An unknown connection type returns HTTP 400.
- A malformed body returns HTTP 400.
"""

from __future__ import annotations

import json
import logging
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


def test_passthrough_valid_snowflake(client):
    body = {
        "credentials": {
            "connect_args": {
                "user": "USER",
                "account": "acct.us-east-1",
                "password": "secret",
            }
        }
    }
    r = client.post("/api/v1/credentials/validate/snowflake", json=body)
    assert r.status_code == 200
    payload = r.get_json()["__mcd_result__"]
    assert payload["valid"] is True
    assert payload["connection_type"] == "snowflake"
    assert payload["errors"] == {}


def test_passthrough_missing_required_returns_errors_at_200(client):
    body = {"credentials": {"connect_args": {"user": "USER"}}}
    r = client.post("/api/v1/credentials/validate/snowflake", json=body)
    assert r.status_code == 200
    payload = r.get_json()["__mcd_result__"]
    assert payload["valid"] is False
    assert "connect_args" in payload["errors"]


def test_unknown_connection_type_returns_400(client):
    r = client.post(
        "/api/v1/credentials/validate/bogus-connector",
        json={"credentials": {}},
    )
    assert r.status_code == 400


def test_malformed_body_returns_400(client):
    r = client.post(
        "/api/v1/credentials/validate/snowflake",
        data="not json",
        content_type="text/plain",
    )
    assert r.status_code == 400


def test_aws_secrets_manager_fetch_failure_surfaces_as_400(client):
    # Simulate the customer's IAM role lacking GetSecretValue on the ARN.
    # The CredentialsFactory's ASM service wraps the underlying error into a
    # ValueError; the route catches it and returns the same __mcd_error__
    # envelope as execute_agent_operation.
    body = {
        "credentials": {
            "self_hosted_credentials_type": "aws_secrets_manager",
            "aws_secret": "arn:aws:secretsmanager:us-east-1:123:secret:foo",
        }
    }
    with patch("apollo.credentials.asm.SecretsManagerProxyClient") as MockClient:
        MockClient.return_value.get_secret_string.side_effect = Exception(
            "AccessDeniedException: User is not authorized to perform: secretsmanager:GetSecretValue"
        )
        r = client.post("/api/v1/credentials/validate/snowflake", json=body)
    assert r.status_code == 400
    payload = r.get_json()
    assert "__mcd_error__" in payload
    assert "Failed to read self-hosted credentials" in payload["__mcd_error__"]
    assert "AccessDeniedException" in payload["__mcd_error__"]


def test_aws_secrets_manager_returns_valid_json_then_validates(client):
    # Simulate ASM returning a valid Snowflake JSON. The endpoint should
    # validate it and return valid=True at 200.
    secret_payload = {
        "connect_args": {
            "user": "snowflake_user",
            "account": "acct.us-east-1",
            "private_key_pem": "-----BEGIN PRIVATE KEY-----\nXXX\n-----END PRIVATE KEY-----",
        }
    }
    body = {
        "credentials": {
            "self_hosted_credentials_type": "aws_secrets_manager",
            "aws_secret": "arn:aws:secretsmanager:us-east-1:123:secret:foo",
        }
    }
    with patch("apollo.credentials.asm.SecretsManagerProxyClient") as MockClient:
        MockClient.return_value.get_secret_string.return_value = json.dumps(
            secret_payload
        )
        r = client.post("/api/v1/credentials/validate/snowflake", json=body)
    assert r.status_code == 200
    payload = r.get_json()["__mcd_result__"]
    assert payload["valid"] is True
    assert payload["errors"] == {}


def test_aws_secrets_manager_returns_invalid_json_then_validates(client):
    # ASM returns valid JSON but the contents don't match the schema.
    # The endpoint should report schema errors at 200.
    body = {
        "credentials": {
            "self_hosted_credentials_type": "aws_secrets_manager",
            "aws_secret": "arn:aws:secretsmanager:us-east-1:123:secret:foo",
        }
    }
    with patch("apollo.credentials.asm.SecretsManagerProxyClient") as MockClient:
        MockClient.return_value.get_secret_string.return_value = json.dumps(
            {"connect_args": {"user": "u"}}
        )
        r = client.post("/api/v1/credentials/validate/snowflake", json=body)
    assert r.status_code == 200
    payload = r.get_json()["__mcd_result__"]
    assert payload["valid"] is False
    assert "errors" in payload


def test_passthrough_motherduck_string_form_accepted(client):
    # Docs document Motherduck connect_args as a string (DuckDB connection
    # string). The validator must accept it without parsing the string.
    body = {
        "credentials": {
            "connect_args": "md:my_db?motherduck_token=abc123",
        }
    }
    r = client.post("/api/v1/credentials/validate/motherduck", json=body)
    assert r.status_code == 200
    assert r.get_json()["__mcd_result__"]["valid"] is True


def test_passthrough_motherduck_dict_form_accepted(client):
    # The CTP also accepts a structured dict — validator covers both.
    body = {
        "credentials": {
            "connect_args": {"db_name": "my_db", "token": "abc123"},
        }
    }
    r = client.post("/api/v1/credentials/validate/motherduck", json=body)
    assert r.status_code == 200
    assert r.get_json()["__mcd_result__"]["valid"] is True


def test_salesforce_crm_token_variant_only(client):
    # Token-auth variant: user + password + security_token. No oauth fields.
    body = {
        "credentials": {
            "connect_args": {
                "user": "user@example.com",
                "password": "pwd",
                "security_token": "tok",
            }
        }
    }
    r = client.post("/api/v1/credentials/validate/salesforce-crm", json=body)
    assert r.status_code == 200
    assert r.get_json()["__mcd_result__"]["valid"] is True


def test_salesforce_crm_oauth_variant_only(client):
    body = {
        "credentials": {
            "connect_args": {
                "consumer_key": "ck",
                "consumer_secret": "cs",
                "domain": "myorg",
            }
        }
    }
    r = client.post("/api/v1/credentials/validate/salesforce-crm", json=body)
    assert r.status_code == 200
    assert r.get_json()["__mcd_result__"]["valid"] is True


def test_salesforce_crm_no_variant_fails(client):
    body = {
        "credentials": {
            "connect_args": {
                "user": "user@example.com",
                # password and security_token missing — neither variant satisfied.
            }
        }
    }
    r = client.post("/api/v1/credentials/validate/salesforce-crm", json=body)
    assert r.status_code == 200
    payload = r.get_json()["__mcd_result__"]
    assert payload["valid"] is False
    # cerberus's oneof_schema flags the dict — every candidate variant is
    # listed under the connect_args error so the customer can see the choices.
    assert "connect_args" in payload["errors"]
