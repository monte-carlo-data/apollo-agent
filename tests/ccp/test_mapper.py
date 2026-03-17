# tests/ccp/test_mapper.py
from unittest import TestCase

from apollo.integrations.ccp.mapper import Mapper
from apollo.integrations.ccp.models import MapperConfig, PipelineState


class TestMapper(TestCase):
    def _mapper(self, field_map, passthrough=False):
        return Mapper(), MapperConfig(
            name="test_mapper",
            output_schema="TestArgs",
            field_map=field_map,
            passthrough=passthrough,
        )

    def test_renders_simple_field_map(self):
        mapper, config = self._mapper({"host": "{{ raw.host }}", "port": "{{ raw.port }}"})
        state = PipelineState(raw={"host": "db.example.com", "port": 5432})
        result = mapper.execute(config, state)
        self.assertEqual({"host": "db.example.com", "port": 5432}, result)

    def test_none_values_omitted(self):
        mapper, config = self._mapper({"host": "{{ raw.host }}", "optional": "{{ raw.missing | default(none) }}"})
        state = PipelineState(raw={"host": "localhost"})
        result = mapper.execute(config, state)
        self.assertIn("host", result)
        self.assertNotIn("optional", result)

    def test_reads_from_derived(self):
        mapper, config = self._mapper({"sslrootcert": "{{ derived.ssl_ca_path }}"})
        state = PipelineState(raw={}, derived={"ssl_ca_path": "/tmp/ca.pem"})
        result = mapper.execute(config, state)
        self.assertEqual("/tmp/ca.pem", result["sslrootcert"])

    def test_passthrough_returns_raw(self):
        mapper, config = self._mapper({}, passthrough=True)
        state = PipelineState(raw={"host": "localhost", "port": 5432})
        result = mapper.execute(config, state)
        self.assertEqual({"host": "localhost", "port": 5432}, result)

    def test_literal_value_not_a_template(self):
        mapper, config = self._mapper({"app": "montecarlo"})
        state = PipelineState(raw={})
        result = mapper.execute(config, state)
        self.assertEqual("montecarlo", result["app"])

    def test_step_field_map_contributions_merged(self):
        mapper, config = self._mapper({"host": "{{ raw.host }}"})
        state = PipelineState(raw={"host": "localhost"}, derived={"ssl_ca_path": "/tmp/ca.pem"})
        result = mapper.execute(config, state, step_field_maps={"sslrootcert": "{{ derived.ssl_ca_path }}"})
        self.assertEqual("localhost", result["host"])
        self.assertEqual("/tmp/ca.pem", result["sslrootcert"])

    def test_step_field_map_none_values_omitted(self):
        mapper, config = self._mapper({})
        state = PipelineState(raw={})
        result = mapper.execute(config, state, step_field_maps={"optional": None})
        self.assertNotIn("optional", result)

    def test_step_field_map_overrides_base(self):
        # step field_map takes precedence over mapper field_map on collision
        mapper, config = self._mapper({"key": "base_value"})
        state = PipelineState(raw={})
        result = mapper.execute(config, state, step_field_maps={"key": "step_value"})
        self.assertEqual("step_value", result["key"])
