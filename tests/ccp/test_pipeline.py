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
