# tests/ctp/test_models.py
from dataclasses import asdict
from unittest import TestCase

from apollo.integrations.ctp.models import (
    CtpConfig,
    MapperConfig,
    PipelineState,
    TransformStep,
)

_STEP_DICT = {
    "type": "resolve_databricks_token",
    "input": {"workspace_url": "{{ raw.databricks_workspace_url }}"},
    "output": {"token": "databricks_rest_token"},
    "when": "raw.databricks_token is defined",
    "field_map": {"token": "{{ derived.databricks_rest_token }}"},
}

_MAPPER_DICT = {
    "name": "my_mapper",
    "field_map": {
        "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
        "token": "{{ raw.token | default(none) }}",
    },
}

_CTP_DICT = {
    "name": "my-custom-ctp",
    "steps": [_STEP_DICT],
    "mapper": _MAPPER_DICT,
}


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


class TestTransformStepFromDict(TestCase):
    def test_required_fields(self):
        step = TransformStep.from_dict(_STEP_DICT)
        self.assertEqual("resolve_databricks_token", step.type)
        self.assertEqual(
            {"workspace_url": "{{ raw.databricks_workspace_url }}"}, step.input
        )
        self.assertEqual({"token": "databricks_rest_token"}, step.output)

    def test_optional_when(self):
        step = TransformStep.from_dict(_STEP_DICT)
        self.assertEqual("raw.databricks_token is defined", step.when)

    def test_when_defaults_to_none(self):
        data = {k: v for k, v in _STEP_DICT.items() if k != "when"}
        step = TransformStep.from_dict(data)
        self.assertIsNone(step.when)

    def test_field_map_defaults_to_empty(self):
        data = {k: v for k, v in _STEP_DICT.items() if k != "field_map"}
        step = TransformStep.from_dict(data)
        self.assertEqual({}, step.field_map)

    def test_missing_type_raises(self):
        with self.assertRaises(ValueError) as ctx:
            TransformStep.from_dict(
                {k: v for k, v in _STEP_DICT.items() if k != "type"}
            )
        self.assertIn("type", str(ctx.exception))

    def test_missing_input_raises(self):
        with self.assertRaises(ValueError) as ctx:
            TransformStep.from_dict(
                {k: v for k, v in _STEP_DICT.items() if k != "input"}
            )
        self.assertIn("input", str(ctx.exception))

    def test_missing_output_raises(self):
        with self.assertRaises(ValueError) as ctx:
            TransformStep.from_dict(
                {k: v for k, v in _STEP_DICT.items() if k != "output"}
            )
        self.assertIn("output", str(ctx.exception))

    def test_non_str_type_raises(self):
        with self.assertRaises(ValueError) as ctx:
            TransformStep.from_dict({**_STEP_DICT, "type": 123})
        self.assertIn("type", str(ctx.exception))

    def test_non_dict_input_raises(self):
        with self.assertRaises(ValueError) as ctx:
            TransformStep.from_dict({**_STEP_DICT, "input": "bad"})
        self.assertIn("input", str(ctx.exception))

    def test_non_dict_output_raises(self):
        with self.assertRaises(ValueError) as ctx:
            TransformStep.from_dict({**_STEP_DICT, "output": ["bad"]})
        self.assertIn("output", str(ctx.exception))

    def test_non_dict_field_map_raises(self):
        with self.assertRaises(ValueError) as ctx:
            TransformStep.from_dict({**_STEP_DICT, "field_map": ["bad"]})
        self.assertIn("field_map", str(ctx.exception))


class TestMapperConfigFromDict(TestCase):
    def test_required_fields(self):
        mapper = MapperConfig.from_dict(_MAPPER_DICT)
        self.assertEqual("my_mapper", mapper.name)
        self.assertEqual(_MAPPER_DICT["field_map"], mapper.field_map)

    def test_schema_always_none(self):
        # schema is not deserialized — injected at runtime from the registered CTP
        mapper = MapperConfig.from_dict(_MAPPER_DICT)
        self.assertIsNone(mapper.schema)

    def test_passthrough_defaults_to_false(self):
        mapper = MapperConfig.from_dict(_MAPPER_DICT)
        self.assertFalse(mapper.passthrough)

    def test_passthrough_true(self):
        mapper = MapperConfig.from_dict({**_MAPPER_DICT, "passthrough": True})
        self.assertTrue(mapper.passthrough)

    def test_name_defaults_to_empty(self):
        mapper = MapperConfig.from_dict(
            {k: v for k, v in _MAPPER_DICT.items() if k != "name"}
        )
        self.assertEqual("", mapper.name)

    def test_flat_dict_shorthand(self):
        # Mapper can be a flat {key: template} dict — no 'field_map' wrapper required
        flat = {"host": "{{ raw.hostname }}", "port": "{{ raw.port }}"}
        mapper = MapperConfig.from_dict(flat)
        self.assertEqual(flat, mapper.field_map)
        self.assertEqual("", mapper.name)
        self.assertFalse(mapper.passthrough)

    def test_dict_without_field_map_key_treated_as_shorthand(self):
        # A dict without a 'field_map' key is treated as a flat shorthand — the whole
        # dict becomes the field_map. There is no longer a "missing field_map" error.
        mapper = MapperConfig.from_dict(
            {k: v for k, v in _MAPPER_DICT.items() if k != "field_map"}
        )
        # _MAPPER_DICT without field_map is {"name": "my_mapper"}, treated as field_map
        self.assertEqual({"name": "my_mapper"}, mapper.field_map)


class TestCtpConfigFromDict(TestCase):
    def test_full_config(self):
        ctp = CtpConfig.from_dict(_CTP_DICT)
        self.assertEqual("my-custom-ctp", ctp.name)
        self.assertEqual(1, len(ctp.steps))
        self.assertEqual("resolve_databricks_token", ctp.steps[0].type)
        self.assertEqual("my_mapper", ctp.mapper.name)

    def test_empty_steps(self):
        ctp = CtpConfig.from_dict({**_CTP_DICT, "steps": []})
        self.assertEqual([], ctp.steps)

    def test_name_defaults_to_empty(self):
        ctp = CtpConfig.from_dict({k: v for k, v in _CTP_DICT.items() if k != "name"})
        self.assertEqual("", ctp.name)

    def test_steps_default_to_empty_list(self):
        ctp = CtpConfig.from_dict({k: v for k, v in _CTP_DICT.items() if k != "steps"})
        self.assertEqual([], ctp.steps)

    def test_mapper_only_config(self):
        # Minimal valid config: just a mapper (flat shorthand)
        ctp = CtpConfig.from_dict(
            {"mapper": {"host": "{{ raw.hostname }}", "port": "{{ raw.port }}"}}
        )
        self.assertEqual("", ctp.name)
        self.assertEqual([], ctp.steps)
        self.assertEqual(
            {"host": "{{ raw.hostname }}", "port": "{{ raw.port }}"},
            ctp.mapper.field_map,
        )

    def test_missing_mapper_raises(self):
        with self.assertRaises(ValueError) as ctx:
            CtpConfig.from_dict({k: v for k, v in _CTP_DICT.items() if k != "mapper"})
        self.assertIn("mapper", str(ctx.exception))

    def test_invalid_step_raises(self):
        with self.assertRaises(ValueError):
            CtpConfig.from_dict({**_CTP_DICT, "steps": [{"type": "something"}]})

    def test_steps_not_a_list_raises(self):
        with self.assertRaises(ValueError) as ctx:
            CtpConfig.from_dict({**_CTP_DICT, "steps": {"type": "something"}})
        self.assertIn("steps", str(ctx.exception))

    def test_steps_not_a_list_of_dicts_raises(self):
        with self.assertRaises(ValueError) as ctx:
            CtpConfig.from_dict({**_CTP_DICT, "steps": ["not-a-dict"]})
        self.assertIn("steps", str(ctx.exception))

    def test_connect_args_defaults_not_a_dict_raises(self):
        with self.assertRaises(ValueError) as ctx:
            CtpConfig.from_dict({**_CTP_DICT, "connect_args_defaults": ["bad"]})
        self.assertIn("connect_args_defaults", str(ctx.exception))

    def test_round_trip(self):
        ctp = CtpConfig.from_dict(_CTP_DICT)
        serialized = asdict(ctp)
        # schema is always None in JSON form — remove before round-tripping
        del serialized["mapper"]["schema"]
        ctp2 = CtpConfig.from_dict(serialized)
        self.assertEqual(ctp.name, ctp2.name)
        self.assertEqual(ctp.steps[0].type, ctp2.steps[0].type)
        self.assertEqual(ctp.mapper.field_map, ctp2.mapper.field_map)
