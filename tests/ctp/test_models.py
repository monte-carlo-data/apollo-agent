# tests/ctp/test_models.py
from unittest import TestCase
from apollo.integrations.ctp.models import (
    CtpConfig,
    MapperConfig,
    PipelineState,
    TransformStep,
)


class TestCtpModels(TestCase):
    def test_pipeline_state_defaults(self):
        state = PipelineState(raw={"host": "localhost"})
        self.assertEqual({}, state.derived)
        self.assertEqual({}, state.context)

    def test_pipeline_state_derived_is_not_shared(self):
        # Each instance should have its own derived dict
        s1 = PipelineState(raw={})
        s2 = PipelineState(raw={})
        s1.derived["x"] = 1
        self.assertNotIn("x", s2.derived)

    def test_ctp_config_roundtrip(self):
        step = TransformStep(
            type="tmp_file_write",
            input={"contents": "{{ raw.ca_pem }}", "file_suffix": ".pem"},
            output={"path": "ssl_ca_path"},
            when="raw.ca_pem is defined",
            field_map={"sslrootcert": "{{ derived.ssl_ca_path }}"},
        )
        mapper = MapperConfig(
            name="pg_args",
            field_map={"host": "{{ raw.host }}"},
        )
        config = CtpConfig(name="pg-default", steps=[step], mapper=mapper)
        self.assertEqual("pg-default", config.name)
        self.assertEqual(1, len(config.steps))
        self.assertEqual(
            {"sslrootcert": "{{ derived.ssl_ca_path }}"}, config.steps[0].field_map
        )
        self.assertFalse(config.mapper.passthrough)

    def test_transform_step_field_map_defaults_empty(self):
        step = TransformStep(type="tmp_file_write", input={}, output={})
        self.assertEqual({}, step.field_map)
