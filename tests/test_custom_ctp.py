# tests/test_custom_ctp.py
"""Integration tests for custom CTP support in execute_operation.

Uses the databricks-rest connection type as the test vehicle because it has a
simple CTP (PAT → token) and a well-defined TypedDict output contract.
"""
from unittest import TestCase
from unittest.mock import create_autospec, patch

from requests import Response

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.ctp.validator import validate_ctp_config

_WORKSPACE_URL = "https://adb-123.azuredatabricks.net"
_CUSTOM_TOKEN = "custom-pipeline-token"

_OPERATION = {
    "trace_id": "custom-ctp-test",
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

# Credentials use a non-standard field name ("custom_token") that the
# registered default CTP would not recognize — proves the custom pipeline ran.
_CUSTOM_CREDENTIALS = {
    "databricks_workspace_url": _WORKSPACE_URL,
    "custom_token": _CUSTOM_TOKEN,
}

# Custom CTP: no steps, mapper reads "custom_token" instead of "databricks_token".
_CUSTOM_CTP = {
    "name": "custom-databricks-rest",
    "steps": [],
    "mapper": {
        "name": "custom_mapper",
        "field_map": {
            "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
            "token": "{{ raw.custom_token }}",
        },
    },
}

# Custom CTP that deliberately omits "token" from its mapper output —
# used to verify the TypedDict schema is injected and enforced.
_CUSTOM_CTP_MISSING_TOKEN = {
    "name": "bad-custom-ctp",
    "steps": [],
    "mapper": {
        "name": "bad_mapper",
        "field_map": {
            "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
            # token intentionally absent
        },
    },
}


class TestCustomCtpExecution(TestCase):
    """Full agent → proxy client path with a ctp_config supplied."""

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    def _mock_http_success(self, mock_request):
        mock_response = create_autospec(Response)
        mock_response.json.return_value = {"result": "ok"}
        mock_request.return_value = mock_response
        return mock_response

    @patch("requests.request")
    def test_ctp_config_used_instead_of_default(self, mock_request):
        """Custom CTP pipeline runs in place of the registered default."""
        self._mock_http_success(mock_request)

        response = self._agent.execute_operation(
            "databricks-rest",
            "start_warehouse",
            _OPERATION,
            _CUSTOM_CREDENTIALS,
            ctp_config=_CUSTOM_CTP,
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertIn(ATTRIBUTE_NAME_RESULT, response.result)
        # The custom CTP reads custom_token — verify it reached the HTTP call
        self.assertEqual(
            f"Bearer {_CUSTOM_TOKEN}",
            mock_request.call_args[1]["headers"]["Authorization"],
        )

    @patch("requests.request")
    def test_ctp_config_schema_injected_enforces_required_fields(self, mock_request):
        """TypedDict schema is injected from the registered CTP; missing required fields raise."""
        # DatabricksRestClientArgs requires "token" — the bad CTP omits it.
        response = self._agent.execute_operation(
            "databricks-rest",
            "start_warehouse",
            _OPERATION,
            _CUSTOM_CREDENTIALS,
            ctp_config=_CUSTOM_CTP_MISSING_TOKEN,
        )

        self.assertIsNotNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertIn("token", response.result.get(ATTRIBUTE_NAME_ERROR, ""))

    @patch("requests.request")
    def test_absent_ctp_config_uses_registered_default(self, mock_request):
        """When ctp_config is absent the registered default pipeline runs unchanged."""
        self._mock_http_success(mock_request)

        response = self._agent.execute_operation(
            "databricks-rest",
            "start_warehouse",
            _OPERATION,
            {
                "databricks_workspace_url": _WORKSPACE_URL,
                "databricks_token": "dapi-pat",
            },
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            "Bearer dapi-pat",
            mock_request.call_args[1]["headers"]["Authorization"],
        )

    @patch("requests.request")
    def test_ctp_config_with_pre_shaped_connect_args(self, mock_request):
        """DC pre-shaped connect_args are unwrapped before the custom pipeline runs."""
        self._mock_http_success(mock_request)

        response = self._agent.execute_operation(
            "databricks-rest",
            "start_warehouse",
            _OPERATION,
            {"connect_args": _CUSTOM_CREDENTIALS},
            ctp_config=_CUSTOM_CTP,
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            f"Bearer {_CUSTOM_TOKEN}",
            mock_request.call_args[1]["headers"]["Authorization"],
        )


class TestCustomCtpSchemaInjection(TestCase):
    """Unit tests for CtpRegistry.resolve_custom schema injection."""

    def test_schema_injected_from_registered_ctp(self):
        """resolve_custom injects the TypedDict schema from the registered default."""
        registered = CtpRegistry.get("databricks-rest")
        self.assertIsNotNone(registered)

        result = CtpRegistry.resolve_custom(
            "databricks-rest",
            _CUSTOM_CREDENTIALS,
            _CUSTOM_CTP,
        )
        self.assertIn("connect_args", result)
        self.assertEqual(_CUSTOM_TOKEN, result["connect_args"]["token"])
        self.assertEqual(
            _WORKSPACE_URL, result["connect_args"]["databricks_workspace_url"]
        )

    def test_schema_injected_for_unknown_connection_type(self):
        """resolve_custom works without a registered CTP — no schema injection, no error."""
        result = CtpRegistry.resolve_custom(
            "unknown-type",
            _CUSTOM_CREDENTIALS,
            _CUSTOM_CTP,
        )
        self.assertIn("connect_args", result)
        self.assertEqual(_CUSTOM_TOKEN, result["connect_args"]["token"])

    def test_non_dict_connect_args_returned_unchanged(self):
        """Legacy ODBC string passthrough is preserved even with a custom CTP."""
        odbc = "DRIVER={SQL Server};SERVER=db.example.com"
        credentials = {"connect_args": odbc}
        result = CtpRegistry.resolve_custom(
            "sql-server",
            credentials,
            _CUSTOM_CTP,
        )
        self.assertIs(credentials, result)


class TestValidateCtp(TestCase):
    """Unit tests for validate_ctp_config."""

    def test_valid_ctp_returns_valid_true(self):
        """A well-formed custom CTP for a registered type passes validation."""
        result = validate_ctp_config("databricks-rest", _CUSTOM_CTP)
        self.assertTrue(result["valid"])
        self.assertEqual([], result["errors"])

    def test_missing_required_field_returns_error(self):
        """CtpConfig.from_dict raises when a required top-level field is absent."""
        result = validate_ctp_config("databricks-rest", {"name": "bad", "steps": []})
        self.assertFalse(result["valid"])
        self.assertTrue(any("mapper" in e for e in result["errors"]))

    def test_unknown_transform_type_returns_error(self):
        """An unregistered transform type in steps is flagged."""
        ctp_with_bad_step = {
            "name": "bad-step-ctp",
            "steps": [
                {
                    "type": "does-not-exist",
                    "input": {},
                    "output": {},
                }
            ],
            "mapper": {
                "name": "m",
                "field_map": {
                    "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
                    "token": "{{ raw.custom_token }}",
                },
            },
        }
        result = validate_ctp_config("databricks-rest", ctp_with_bad_step)
        self.assertFalse(result["valid"])
        self.assertTrue(any("does-not-exist" in e for e in result["errors"]))

    def test_bad_jinja2_syntax_returns_error(self):
        """A malformed Jinja2 template in field_map is flagged."""
        bad_template_ctp = {
            "name": "bad-template-ctp",
            "steps": [],
            "mapper": {
                "name": "m",
                "field_map": {
                    "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
                    "token": "{{ raw.custom_token ",  # missing closing }}
                },
            },
        }
        result = validate_ctp_config("databricks-rest", bad_template_ctp)
        self.assertFalse(result["valid"])
        self.assertTrue(
            any("token" in e and "syntax" in e.lower() for e in result["errors"])
        )

    def test_missing_required_schema_keys_returns_error(self):
        """A mapper that omits TypedDict required keys (no steps) is flagged."""
        result = validate_ctp_config("databricks-rest", _CUSTOM_CTP_MISSING_TOKEN)
        self.assertFalse(result["valid"])
        self.assertTrue(any("token" in e for e in result["errors"]))

    def test_unknown_mapper_keys_returns_error(self):
        """A mapper field_map with keys not in the TypedDict schema is flagged."""
        ctp_with_unknown_key = {
            "name": "unknown-key-ctp",
            "steps": [],
            "mapper": {
                "name": "m",
                "field_map": {
                    "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
                    "token": "{{ raw.custom_token }}",
                    "passwordx": "{{ raw.bad }}",  # not in DatabricksRestClientArgs
                },
            },
        }
        result = validate_ctp_config("databricks-rest", ctp_with_unknown_key)
        self.assertFalse(result["valid"])
        self.assertTrue(any("passwordx" in e for e in result["errors"]))

    def test_unknown_connection_type_validates_without_schema(self):
        """An unknown connection type has no schema to inject — valid if well-formed."""
        result = validate_ctp_config("unknown-type", _CUSTOM_CTP)
        self.assertTrue(result["valid"])

    def test_missing_ctp_required_fields_returns_error_list(self):
        """validate_ctp_config returns a list with the deserialization error."""
        result = validate_ctp_config("databricks-rest", {})
        self.assertFalse(result["valid"])
        self.assertIsInstance(result["errors"], list)
        self.assertTrue(len(result["errors"]) > 0)

    def test_non_dict_ctp_config_returns_error(self):
        """Non-dict ctp_config returns valid=False without raising."""
        for bad_value in (["a", "list"], "a string", 42, None):
            with self.subTest(value=bad_value):
                result = validate_ctp_config("databricks-rest", bad_value)
                self.assertFalse(result["valid"])
                self.assertTrue(len(result["errors"]) > 0)
                self.assertEqual([], result["warnings"])

    def test_missing_required_key_with_steps_is_warning_not_error(self):
        """When a required key is absent from top-level field_map but steps are present,
        the result is valid=True with a warning (not an error)."""
        ctp_with_step_covering_token = {
            "name": "step-covered-ctp",
            # Step contributes "token" via its own field_map at runtime.
            "steps": [
                {
                    "type": "resolve_databricks_token",
                    "when": "raw.databricks_token is defined",
                    "input": {
                        "workspace_url": "{{ raw.databricks_workspace_url }}",
                        "databricks_token": "{{ raw.databricks_token }}",
                    },
                    "output": {"token": "resolved_token"},
                    "field_map": {"token": "{{ derived.resolved_token }}"},
                }
            ],
            "mapper": {
                "name": "m",
                "field_map": {
                    # "token" intentionally omitted — step provides it
                    "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
                },
            },
        }
        result = validate_ctp_config("databricks-rest", ctp_with_step_covering_token)
        self.assertTrue(result["valid"])
        self.assertEqual([], result["errors"])
        self.assertTrue(any("token" in w for w in result["warnings"]))

    def test_step_field_map_unknown_key_returns_error(self):
        """A step field_map containing a key not in the TypedDict schema is flagged."""
        ctp_with_bad_step_key = {
            "name": "bad-step-key-ctp",
            "steps": [
                {
                    "type": "resolve_databricks_token",
                    "when": "raw.databricks_token is defined",
                    "input": {
                        "workspace_url": "{{ raw.databricks_workspace_url }}",
                        "databricks_token": "{{ raw.databricks_token }}",
                    },
                    "output": {"token": "resolved_token"},
                    "field_map": {
                        "token": "{{ derived.resolved_token }}",
                        "not_a_real_key": "{{ raw.something }}",  # unknown
                    },
                }
            ],
            "mapper": {
                "name": "m",
                "field_map": {
                    "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
                    "token": "{{ raw.token | default(none) }}",
                },
            },
        }
        result = validate_ctp_config("databricks-rest", ctp_with_bad_step_key)
        self.assertFalse(result["valid"])
        self.assertTrue(any("not_a_real_key" in e for e in result["errors"]))
