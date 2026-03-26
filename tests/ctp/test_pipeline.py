# tests/ctp/test_pipeline.py
import os
from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import (
    CtpConfig,
    MapperConfig,
    PipelineState,
    TransformStep,
)
from apollo.integrations.ctp import pipeline as _pipeline_module
from apollo.integrations.ctp.pipeline import CtpPipeline


def _minimal_config(field_map=None, steps=None):
    return CtpConfig(
        name="test-ctp",
        steps=steps or [],
        mapper=MapperConfig(
            name="test_mapper",
            field_map=field_map or {},
            passthrough=False,
        ),
    )


class TestCtpPipeline(TestCase):
    def setUp(self):
        self._pipeline = CtpPipeline()

    def test_empty_steps_passthrough(self):
        config = _minimal_config(field_map={"host": "{{ raw.host }}"})
        result = self._pipeline.execute(config, {"host": "localhost"})
        self.assertEqual({"host": "localhost"}, result)

    def test_transform_step_executed(self):
        config = _minimal_config(
            steps=[
                TransformStep(
                    type="tmp_file_write",
                    input={"contents": "{{ raw.ca_pem }}", "file_suffix": ".pem"},
                    output={"path": "ssl_ca_path"},
                ),
            ],
            field_map={"sslrootcert": "{{ derived.ssl_ca_path }}"},
        )
        result = self._pipeline.execute(config, {"ca_pem": "CERT"})
        path = result["sslrootcert"]
        self.assertTrue(os.path.exists(path))
        os.unlink(path)

    def test_when_condition_false_skips_step(self):
        config = _minimal_config(
            steps=[
                TransformStep(
                    type="tmp_file_write",
                    input={"contents": "{{ raw.ca_pem }}", "file_suffix": ".pem"},
                    output={"path": "ssl_ca_path"},
                    when="raw.ca_pem is defined",
                )
            ],
            field_map={},
        )
        result = self._pipeline.execute(config, {})  # no ca_pem
        self.assertNotIn("sslrootcert", result)

    def test_when_condition_true_runs_step(self):
        config = _minimal_config(
            steps=[
                TransformStep(
                    type="tmp_file_write",
                    input={"contents": "{{ raw.ca_pem }}", "file_suffix": ".pem"},
                    output={"path": "ssl_ca_path"},
                    when="raw.ca_pem is defined",
                )
            ],
            field_map={"cert": "{{ derived.ssl_ca_path }}"},
        )
        result = self._pipeline.execute(config, {"ca_pem": "CERT_DATA"})
        self.assertIn("cert", result)
        os.unlink(result["cert"])

    def test_unknown_transform_raises(self):
        config = _minimal_config(
            steps=[TransformStep(type="not_a_transform", input={}, output={})]
        )
        with self.assertRaises(CtpPipelineError):
            self._pipeline.execute(config, {})

    def test_context_available_in_templates(self):
        config = _minimal_config(field_map={"env": "{{ context.env }}"})
        result = self._pipeline.execute(config, {}, context={"env": "prod"})
        self.assertEqual("prod", result["env"])

    def test_step_field_map_applied_when_step_runs(self):
        config = _minimal_config(
            steps=[
                TransformStep(
                    type="tmp_file_write",
                    input={"contents": "{{ raw.ca_pem }}", "file_suffix": ".pem"},
                    output={"path": "ssl_ca_path"},
                    when="raw.ca_pem is defined",
                    field_map={"sslrootcert": "{{ derived.ssl_ca_path }}"},
                )
            ],
            field_map={"host": "{{ raw.host }}"},
        )
        result = self._pipeline.execute(config, {"host": "localhost", "ca_pem": "CERT"})
        self.assertIn("sslrootcert", result)
        os.unlink(result["sslrootcert"])

    def test_step_field_map_not_applied_when_step_skipped(self):
        config = _minimal_config(
            steps=[
                TransformStep(
                    type="tmp_file_write",
                    input={"contents": "{{ raw.ca_pem }}", "file_suffix": ".pem"},
                    output={"path": "ssl_ca_path"},
                    when="raw.ca_pem is defined",
                    field_map={"sslrootcert": "{{ derived.ssl_ca_path }}"},
                )
            ],
            field_map={"host": "{{ raw.host }}"},
        )
        result = self._pipeline.execute(config, {"host": "localhost"})  # no ca_pem
        self.assertNotIn("sslrootcert", result)

    def test_raw_credentials_not_mutated_by_pipeline(self):
        """pipeline.execute() must not clear or mutate the caller's credentials dict."""
        creds = {"host": "db.example.com", "password": "secret"}
        config = _minimal_config(field_map={"host": "{{ raw.host }}"})
        self._pipeline.execute(config, creds)
        self.assertEqual("db.example.com", creds["host"])
        self.assertEqual("secret", creds["password"])

    def test_credential_state_cleared_after_execute(self):
        """raw and derived state must be scrubbed after client_args are returned."""
        captured_state: list = []
        original_mapper_execute = _pipeline_module.Mapper.execute

        def capturing_execute(self_mapper, config, state, **kwargs):
            captured_state.append(state)
            return original_mapper_execute(self_mapper, config, state, **kwargs)

        _pipeline_module.Mapper.execute = capturing_execute
        try:
            config = _minimal_config(field_map={"host": "{{ raw.host }}"})
            self._pipeline.execute(
                config, {"host": "db.example.com", "password": "s3cr3t"}
            )
        finally:
            _pipeline_module.Mapper.execute = original_mapper_execute

        state = captured_state[0]
        self.assertEqual({}, state.raw, "state.raw should be cleared after execute")
        self.assertEqual(
            {}, state.derived, "state.derived should be cleared after execute"
        )

    def test_credential_state_cleared_on_exception(self):
        """raw state must be scrubbed even when the pipeline raises."""
        captured_state: list = []
        original_mapper_execute = _pipeline_module.Mapper.execute

        def failing_execute(self_mapper, config, state, **kwargs):
            captured_state.append(state)
            raise RuntimeError("simulated mapper failure")

        _pipeline_module.Mapper.execute = failing_execute
        try:
            config = _minimal_config(field_map={"host": "{{ raw.host }}"})
            with self.assertRaises(RuntimeError):
                self._pipeline.execute(
                    config, {"host": "db.example.com", "password": "s3cr3t"}
                )
        finally:
            _pipeline_module.Mapper.execute = original_mapper_execute

        state = captured_state[0]
        self.assertEqual({}, state.raw, "state.raw should be cleared even on exception")
        self.assertEqual(
            {}, state.derived, "state.derived should be cleared even on exception"
        )


class TestPostgresDefaultCtp(TestCase):
    def setUp(self):
        self._pipeline = CtpPipeline()

    def test_basic_connection_args(self):
        from apollo.integrations.ctp.defaults.postgres import POSTGRES_DEFAULT_CTP

        result = self._pipeline.execute(
            POSTGRES_DEFAULT_CTP,
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertEqual("db.example.com", result["host"])
        self.assertEqual(5432, result["port"])
        self.assertEqual("mydb", result["dbname"])
        self.assertEqual("admin", result["user"])
        self.assertEqual("secret", result["password"])
        self.assertNotIn("sslmode", result)  # no SSL cert, no sslmode emitted
        self.assertNotIn("sslrootcert", result)

    def test_ssl_ca_pem_materialized(self):
        from apollo.integrations.ctp.defaults.postgres import POSTGRES_DEFAULT_CTP

        result = self._pipeline.execute(
            POSTGRES_DEFAULT_CTP,
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
                "ssl_options": {
                    "ca_data": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"
                },
            },
        )
        self.assertIn("sslrootcert", result)
        path = result["sslrootcert"]
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            self.assertIn("FAKE", f.read())
        os.unlink(path)

    def test_ssl_mode_override(self):
        from apollo.integrations.ctp.defaults.postgres import POSTGRES_DEFAULT_CTP

        result = self._pipeline.execute(
            POSTGRES_DEFAULT_CTP,
            {
                "host": "h",
                "port": 5432,
                "database": "d",
                "user": "u",
                "password": "p",
                "ssl_mode": "verify-full",
            },
        )
        self.assertEqual("verify-full", result["sslmode"])
