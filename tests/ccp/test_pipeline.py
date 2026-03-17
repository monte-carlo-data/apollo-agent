# tests/ccp/test_pipeline.py
import os
from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.models import CcpConfig, MapperConfig, PipelineState, TransformStep
from apollo.integrations.ccp.pipeline import CcpPipeline


def _minimal_config(field_map=None, steps=None):
    return CcpConfig(
        name="test-ccp",
        steps=steps or [],
        mapper=MapperConfig(
            name="test_mapper",
            output_schema="TestArgs",
            field_map=field_map or {},
            passthrough=False,
        ),
    )


class TestCcpPipeline(TestCase):
    def setUp(self):
        self._pipeline = CcpPipeline()

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
            steps=[
                TransformStep(type="not_a_transform", input={}, output={})
            ]
        )
        with self.assertRaises(CcpPipelineError):
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


class TestPostgresDefaultCcp(TestCase):
    def setUp(self):
        self._pipeline = CcpPipeline()

    def test_basic_connection_args(self):
        from apollo.integrations.ccp.defaults.postgres import POSTGRES_DEFAULT_CCP

        result = self._pipeline.execute(
            POSTGRES_DEFAULT_CCP,
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
        self.assertEqual("require", result["sslmode"])  # default
        self.assertNotIn("sslrootcert", result)

    def test_ssl_ca_pem_materialized(self):
        from apollo.integrations.ccp.defaults.postgres import POSTGRES_DEFAULT_CCP

        result = self._pipeline.execute(
            POSTGRES_DEFAULT_CCP,
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
                "ssl_ca_pem": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----",
            },
        )
        self.assertIn("sslrootcert", result)
        path = result["sslrootcert"]
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            self.assertIn("FAKE", f.read())
        os.unlink(path)

    def test_ssl_mode_override(self):
        from apollo.integrations.ccp.defaults.postgres import POSTGRES_DEFAULT_CCP

        result = self._pipeline.execute(
            POSTGRES_DEFAULT_CCP,
            {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p", "ssl_mode": "verify-full"},
        )
        self.assertEqual("verify-full", result["sslmode"])
