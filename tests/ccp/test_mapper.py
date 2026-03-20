# tests/ccp/test_mapper.py
from typing import TypedDict, Required, NotRequired
from unittest import TestCase

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.mapper import Mapper
from apollo.integrations.ccp.models import MapperConfig, PipelineState


class TestMapper(TestCase):
    def _mapper(self, field_map, passthrough=False, schema=None):
        return Mapper(), MapperConfig(
            name="test_mapper",
            field_map=field_map,
            schema=schema,
            passthrough=passthrough,
        )

    def test_renders_simple_field_map(self):
        mapper, config = self._mapper(
            {"host": "{{ raw.host }}", "port": "{{ raw.port }}"}
        )
        state = PipelineState(raw={"host": "db.example.com", "port": 5432})
        result = mapper.execute(config, state)
        self.assertEqual({"host": "db.example.com", "port": 5432}, result)

    def test_none_values_omitted(self):
        mapper, config = self._mapper(
            {"host": "{{ raw.host }}", "optional": "{{ raw.missing | default(none) }}"}
        )
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
        state = PipelineState(
            raw={"host": "localhost"}, derived={"ssl_ca_path": "/tmp/ca.pem"}
        )
        result = mapper.execute(
            config, state, step_field_maps={"sslrootcert": "{{ derived.ssl_ca_path }}"}
        )
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


class _MinimalSchema(TypedDict):
    host: Required[str]
    port: Required[int]
    sslmode: NotRequired[str]


class TestMapperSchemaValidation(TestCase):
    def _mapper(self, field_map, passthrough=False, schema=None):
        return Mapper(), MapperConfig(
            name="test_mapper",
            field_map=field_map,
            schema=schema,
            passthrough=passthrough,
        )

    def test_no_schema_no_validation(self):
        mapper, config = self._mapper(
            {"unknown_key": "value", "another_unknown": "value2"}
        )
        state = PipelineState(raw={})
        # No schema set — unknown keys should be fine, no exception
        result = mapper.execute(config, state)
        self.assertIn("unknown_key", result)
        self.assertIn("another_unknown", result)

    def test_all_required_fields_present_passes(self):
        mapper, config = self._mapper(
            {"host": "{{ raw.host }}", "port": "{{ raw.port }}"},
            schema=_MinimalSchema,
        )
        state = PipelineState(raw={"host": "localhost", "port": 5432})
        # All required keys present — no exception
        result = mapper.execute(config, state)
        self.assertEqual("localhost", result["host"])
        self.assertEqual(5432, result["port"])

    def test_optional_field_accepted(self):
        mapper, config = self._mapper(
            {
                "host": "{{ raw.host }}",
                "port": "{{ raw.port }}",
                "sslmode": "{{ raw.sslmode }}",
            },
            schema=_MinimalSchema,
        )
        state = PipelineState(
            raw={"host": "localhost", "port": 5432, "sslmode": "require"}
        )
        # Required keys + optional key present — no exception
        result = mapper.execute(config, state)
        self.assertEqual("require", result["sslmode"])

    def test_missing_required_field_raises(self):
        mapper, config = self._mapper(
            {"host": "{{ raw.host }}"},  # missing 'port' which is Required
            schema=_MinimalSchema,
        )
        state = PipelineState(raw={"host": "localhost"})
        with self.assertRaises(CcpPipelineError) as ctx:
            mapper.execute(config, state)
        self.assertEqual("mapper_validation", ctx.exception.stage)
        self.assertIn("port", str(ctx.exception))

    def test_unknown_field_raises(self):
        mapper, config = self._mapper(
            {"host": "{{ raw.host }}", "port": "{{ raw.port }}", "bad_key": "value"},
            schema=_MinimalSchema,
        )
        state = PipelineState(raw={"host": "localhost", "port": 5432})
        with self.assertRaises(CcpPipelineError) as ctx:
            mapper.execute(config, state)
        self.assertEqual("mapper_validation", ctx.exception.stage)
        self.assertIn("bad_key", str(ctx.exception))

    def test_passthrough_skips_schema_validation(self):
        # passthrough=True with a schema that would fail (missing required keys)
        mapper, config = self._mapper(
            {},
            passthrough=True,
            schema=_MinimalSchema,
        )
        # raw doesn't have 'host' or 'port' — but passthrough bypasses validation
        state = PipelineState(raw={"totally_wrong_key": "value"})
        # No exception should be raised
        result = mapper.execute(config, state)
        self.assertEqual({"totally_wrong_key": "value"}, result)
