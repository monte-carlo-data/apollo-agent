"""
End-to-end integration tests for ctp_config support.

Two test layers:

1. **Agent Flask tests** (fast, no real server) — validate and execute endpoints
   via Flask test client; Databricks HTTP calls are mocked so no credentials needed.

2. **DC→Agent live HTTP test** — starts the agent on a local port using werkzeug,
   then sends raw HTTP requests (the same JSON a real DC sends) to verify the
   full wire protocol. Proves that ctp_config travels over HTTP correctly and the
   agent applies it to the credential transform.

Run:
    source ~/.venv/apollo-agent/bin/activate
    python -m pytest tests/integration/test_ctp_config_e2e.py -v
"""

import socket
import threading
from unittest.mock import create_autospec, patch

import pytest
import requests
from requests import Response
from werkzeug.serving import make_server

from apollo.interfaces.generic.main import app

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_WORKSPACE_URL = "https://adb-123.azuredatabricks.net"
_CUSTOM_TOKEN = "my-local-integration-test-token"

# A custom CTP that maps a non-standard "custom_token" field → "token".
# This is the key proof: the registered default CTP reads "databricks_token",
# so if the integration test passes, the custom CTP ran (not the default).
_CTP_CONFIG = {
    "name": "integration-test-ctp",
    "steps": [],
    "mapper": {
        "name": "integration-test-mapper",
        "field_map": {
            "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
            "token": "{{ raw.custom_token }}",
        },
    },
}

_CREDENTIALS = {
    "databricks_workspace_url": _WORKSPACE_URL,
    "custom_token": _CUSTOM_TOKEN,  # non-standard field — only the custom CTP reads it
}

_OPERATION = {
    "trace_id": "ctp-integration-test",
    "skip_cache": True,
    "commands": [
        {
            "method": "do_request",
            "kwargs": {
                "url": f"{_WORKSPACE_URL}/api/2.0/sql/warehouses/abc/start",
                "http_method": "POST",
            },
        }
    ],
}


# ---------------------------------------------------------------------------
# Layer 1: Flask test client — fast, no real server
# ---------------------------------------------------------------------------


@pytest.fixture
def agent_client():
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


class TestValidateEndpoint:
    """Validate endpoint returns structured errors without executing the pipeline."""

    def test_valid_ctp_config_passes(self, agent_client):
        resp = agent_client.post(
            "/api/v1/ctp/validate/databricks-rest",
            json={"ctp_config": _CTP_CONFIG},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert (
            body["valid"] is True
        ), f"Expected valid=True, got errors: {body['errors']}"
        assert body["errors"] == []

    def test_missing_ctp_config_returns_400(self, agent_client):
        resp = agent_client.post("/api/v1/ctp/validate/databricks-rest", json={})
        assert resp.status_code == 400

    def test_bad_jinja2_syntax_caught(self, agent_client):
        bad = {
            **_CTP_CONFIG,
            "mapper": {
                "name": "bad",
                "field_map": {"token": "{{ raw.custom_token "},  # unclosed brace
            },
        }
        resp = agent_client.post(
            "/api/v1/ctp/validate/databricks-rest",
            json={"ctp_config": bad},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["valid"] is False
        assert any("syntax" in e.lower() for e in body["errors"])

    def test_missing_required_schema_key_caught(self, agent_client):
        # databricks-rest TypedDict requires "token" — omit it
        no_token = {
            "name": "no-token",
            "steps": [],
            "mapper": {
                "name": "m",
                "field_map": {
                    "databricks_workspace_url": "{{ raw.databricks_workspace_url }}"
                },
            },
        }
        resp = agent_client.post(
            "/api/v1/ctp/validate/databricks-rest",
            json={"ctp_config": no_token},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["valid"] is False
        assert any("token" in e for e in body["errors"])

    def test_unknown_connection_type_valid_if_well_formed(self, agent_client):
        resp = agent_client.post(
            "/api/v1/ctp/validate/unknown-type",
            json={"ctp_config": _CTP_CONFIG},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["valid"] is True


class TestExecuteWithCtpConfig:
    """Execute endpoint applies ctp_config instead of the registered default."""

    @patch("requests.request")
    def test_custom_token_forwarded_as_bearer(self, mock_request, agent_client):
        """
        The custom CTP maps custom_token → token.
        Proves the custom CTP ran (not the registered default which reads databricks_token).
        """
        mock_resp = create_autospec(Response)
        mock_resp.json.return_value = {"result": "ok"}
        mock_request.return_value = mock_resp

        resp = agent_client.post(
            "/api/v1/agent/execute/databricks-rest/start_warehouse",
            json={
                "operation": _OPERATION,
                "credentials": _CREDENTIALS,
                "ctp_config": _CTP_CONFIG,
            },
        )
        assert resp.status_code == 200
        assert mock_request.called, "Agent never made the outbound Databricks HTTP call"
        auth_header = mock_request.call_args[1]["headers"]["Authorization"]
        assert (
            auth_header == f"Bearer {_CUSTOM_TOKEN}"
        ), f"Expected Bearer {_CUSTOM_TOKEN!r} — custom CTP may not have run. Got: {auth_header!r}"

    @patch("requests.request")
    def test_absent_ctp_config_falls_back_to_default(self, mock_request, agent_client):
        """Without ctp_config the registered default CTP runs (reads databricks_token)."""
        mock_resp = create_autospec(Response)
        mock_resp.json.return_value = {"result": "ok"}
        mock_request.return_value = mock_resp

        resp = agent_client.post(
            "/api/v1/agent/execute/databricks-rest/start_warehouse",
            json={
                "operation": _OPERATION,
                "credentials": {
                    "databricks_workspace_url": _WORKSPACE_URL,
                    "databricks_token": "dapi-default-pat",
                },
            },
        )
        assert resp.status_code == 200
        auth_header = mock_request.call_args[1]["headers"]["Authorization"]
        assert auth_header == "Bearer dapi-default-pat"


# ---------------------------------------------------------------------------
# Layer 2: Live HTTP server — real socket, simulates DC wire protocol
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def live_agent_url():
    """
    Start the Flask agent on a free local port using werkzeug's make_server.
    Yields the base URL (e.g. http://127.0.0.1:54321).
    Shuts down cleanly after the test module finishes.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    server = make_server("127.0.0.1", port, app)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

    base = f"http://127.0.0.1:{port}"
    # Wait until the server responds
    for _ in range(30):
        try:
            requests.get(f"{base}/health", timeout=0.3)
            break
        except Exception:
            import time

            time.sleep(0.1)

    yield base
    server.shutdown()


class TestLiveServerCtpConfig:
    """
    Real HTTP requests to the live agent — closest to what the DC sends.
    Databricks calls are still mocked so no real credentials are needed.
    """

    def test_validate_endpoint_over_http(self, live_agent_url):
        resp = requests.post(
            f"{live_agent_url}/api/v1/ctp/validate/databricks-rest",
            json={"ctp_config": _CTP_CONFIG},
            timeout=5,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True, f"Unexpected errors: {body['errors']}"

    @patch("requests.request")
    def test_execute_ctp_config_over_http(self, mock_request, live_agent_url):
        """
        Sends the same JSON body the DC would send over real HTTP.
        Verifies the agent correctly applies the custom CTP.
        """
        mock_resp = create_autospec(Response)
        mock_resp.json.return_value = {"result": "ok"}
        mock_request.return_value = mock_resp

        resp = requests.post(
            f"{live_agent_url}/api/v1/agent/execute/databricks-rest/start_warehouse",
            json={
                "operation": _OPERATION,
                "credentials": _CREDENTIALS,
                "ctp_config": _CTP_CONFIG,
            },
            timeout=5,
        )
        assert resp.status_code == 200

        # Verify the agent used the custom CTP (custom_token → Bearer token)
        assert mock_request.called
        auth_header = mock_request.call_args[1]["headers"]["Authorization"]
        assert (
            auth_header == f"Bearer {_CUSTOM_TOKEN}"
        ), f"Custom CTP did not run — expected Bearer {_CUSTOM_TOKEN!r}, got {auth_header!r}"

    def test_health_endpoint(self, live_agent_url):
        resp = requests.get(f"{live_agent_url}/health", timeout=5)
        assert resp.status_code == 200
