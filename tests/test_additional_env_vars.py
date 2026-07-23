import json
import os
from unittest import TestCase
from unittest.mock import patch

from apollo.agent.additional_env_vars import (
    ADDITIONAL_ENV_VARS_ENV_VAR,
    apply_additional_env_vars,
    is_sensitive_env_var_name,
    sanitized_blob_for_health,
)
from apollo.common.agent.constants import ATTRIBUTE_VALUE_REDACTED


class ApplyAdditionalEnvVarsTests(TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_injects_and_coerces_scalars(self):
        os.environ[ADDITIONAL_ENV_VARS_ENV_VAR] = json.dumps(
            {"MCD_ORACLE_THICK_MODE": "true", "MCD_FLAG": True, "MCD_NUM": 5}
        )
        apply_additional_env_vars()
        self.assertEqual("true", os.environ["MCD_ORACLE_THICK_MODE"])
        self.assertEqual("true", os.environ["MCD_FLAG"])  # bool -> "true"/"false"
        self.assertEqual("5", os.environ["MCD_NUM"])  # numbers stringified

    @patch.dict(os.environ, {}, clear=True)
    def test_does_not_override_explicit_env_var(self):
        os.environ["MCD_ORACLE_THICK_MODE"] = "false"
        os.environ[ADDITIONAL_ENV_VARS_ENV_VAR] = json.dumps(
            {"MCD_ORACLE_THICK_MODE": "true"}
        )
        apply_additional_env_vars()
        self.assertEqual("false", os.environ["MCD_ORACLE_THICK_MODE"])

    @patch.dict(os.environ, {}, clear=True)
    def test_skips_non_scalar_values(self):
        os.environ[ADDITIONAL_ENV_VARS_ENV_VAR] = json.dumps(
            {"MCD_OK": "1", "MCD_BAD": {"nested": 1}, "MCD_ALSO_BAD": [1, 2]}
        )
        apply_additional_env_vars()
        self.assertEqual("1", os.environ["MCD_OK"])
        self.assertNotIn("MCD_BAD", os.environ)
        self.assertNotIn("MCD_ALSO_BAD", os.environ)

    @patch.dict(os.environ, {}, clear=True)
    def test_invalid_json_is_noop(self):
        os.environ[ADDITIONAL_ENV_VARS_ENV_VAR] = "{not json"
        apply_additional_env_vars()  # must not raise
        self.assertEqual([ADDITIONAL_ENV_VARS_ENV_VAR], list(os.environ.keys()))

    @patch.dict(os.environ, {}, clear=True)
    def test_non_object_json_is_noop(self):
        os.environ[ADDITIONAL_ENV_VARS_ENV_VAR] = json.dumps(["a", "b"])
        apply_additional_env_vars()
        self.assertEqual([ADDITIONAL_ENV_VARS_ENV_VAR], list(os.environ.keys()))

    @patch.dict(os.environ, {}, clear=True)
    def test_absent_blob_is_noop(self):
        apply_additional_env_vars()
        self.assertEqual({}, dict(os.environ))

    @patch.dict(os.environ, {}, clear=True)
    def test_idempotent(self):
        os.environ[ADDITIONAL_ENV_VARS_ENV_VAR] = json.dumps({"MCD_X": "1"})
        apply_additional_env_vars()
        apply_additional_env_vars()  # second call is a no-op
        self.assertEqual("1", os.environ["MCD_X"])


class SanitizedBlobForHealthTests(TestCase):
    def test_redacts_sensitive_named_entries_only(self):
        raw = json.dumps(
            {
                "MCD_ORACLE_THICK_MODE": "true",
                "MCD_API_TOKEN": "t0ken",
                "MCD_STORAGE_ACCESS_KEY": "AKIA",
            }
        )
        out = json.loads(sanitized_blob_for_health(raw))
        self.assertEqual("true", out["MCD_ORACLE_THICK_MODE"])
        self.assertEqual(ATTRIBUTE_VALUE_REDACTED, out["MCD_API_TOKEN"])
        self.assertEqual(ATTRIBUTE_VALUE_REDACTED, out["MCD_STORAGE_ACCESS_KEY"])

    def test_invalid_json_returns_placeholder(self):
        self.assertIn("invalid", sanitized_blob_for_health("{not json").lower())


class IsSensitiveEnvVarNameTests(TestCase):
    def test_matches(self):
        for name in [
            "MCD_DB_PASSWORD",
            "MCD_X_SECRET",
            "MCD_API_TOKEN",
            "MCD_STORAGE_ACCESS_KEY",
            "MCD_SVC_CREDENTIAL",
        ]:
            self.assertTrue(is_sensitive_env_var_name(name), name)

    def test_non_matches(self):
        for name in ["MCD_ORACLE_THICK_MODE", "MCD_AGENT_WRAPPER_TYPE"]:
            self.assertFalse(is_sensitive_env_var_name(name), name)
