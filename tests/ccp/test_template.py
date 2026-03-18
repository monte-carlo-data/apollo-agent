# tests/ccp/test_template.py
from unittest import TestCase
from jinja2 import UndefinedError

from apollo.integrations.ccp.models import PipelineState
from apollo.integrations.ccp.template import TemplateEngine


class TestTemplateEngine(TestCase):
    def _state(self, raw=None, derived=None):
        return PipelineState(raw=raw or {}, derived=derived or {})

    def test_non_template_value_returned_as_is(self):
        state = self._state(raw={"host": "localhost"})
        self.assertEqual(42, TemplateEngine.render(42, state))
        self.assertEqual("plain", TemplateEngine.render("plain", state))

    def test_renders_string_reference(self):
        state = self._state(raw={"host": "db.example.com"})
        result = TemplateEngine.render("{{ raw.host }}", state)
        self.assertEqual("db.example.com", result)

    def test_preserves_native_int_type(self):
        state = self._state(raw={"port": 5432})
        result = TemplateEngine.render("{{ raw.port }}", state)
        self.assertEqual(5432, result)
        self.assertIsInstance(result, int)

    def test_preserves_native_bool_type(self):
        state = self._state(raw={"flag": True})
        result = TemplateEngine.render("{{ raw.flag }}", state)
        self.assertIs(True, result)

    def test_reads_from_derived(self):
        state = self._state(derived={"ssl_ca_path": "/tmp/ca.pem"})
        result = TemplateEngine.render("{{ derived.ssl_ca_path }}", state)
        self.assertEqual("/tmp/ca.pem", result)

    def test_jinja_default_filter(self):
        state = self._state(raw={})
        result = TemplateEngine.render("{{ raw.ssl_mode | default('require') }}", state)
        self.assertEqual("require", result)

    def test_none_returned_for_none_value(self):
        state = self._state(raw={"val": None})
        result = TemplateEngine.render("{{ raw.val }}", state)
        self.assertIsNone(result)

    def test_undefined_variable_raises(self):
        state = self._state(raw={})
        with self.assertRaises(UndefinedError):
            TemplateEngine.render("{{ raw.missing }}", state)

    def test_evaluate_condition_true(self):
        state = self._state(raw={"ssl_ca_pem": "cert-data"})
        self.assertTrue(
            TemplateEngine.evaluate_condition("raw.ssl_ca_pem is defined", state)
        )

    def test_evaluate_condition_false(self):
        state = self._state(raw={})
        self.assertFalse(
            TemplateEngine.evaluate_condition("raw.ssl_ca_pem is defined", state)
        )

    def test_evaluate_condition_with_and(self):
        state = self._state(raw={"a": 1, "b": 2})
        self.assertTrue(
            TemplateEngine.evaluate_condition(
                "raw.a is defined and raw.b is defined", state
            )
        )
