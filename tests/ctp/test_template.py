# tests/ctp/test_template.py
from unittest import TestCase
from jinja2 import UndefinedError
from jinja2.sandbox import SecurityError

from apollo.integrations.ctp.models import PipelineState
from apollo.integrations.ctp.template import TemplateEngine, _ENV


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

    # ------------------------------------------------------------------
    # Sandbox security tests
    # ------------------------------------------------------------------

    def test_sandbox_blocks_dunder_traversal(self):
        """Malicious template attempting class traversal raises SecurityError."""
        state = self._state(raw={})
        with self.assertRaises(SecurityError):
            TemplateEngine.render(
                "{{ ().__class__.__bases__[0].__subclasses__() }}", state
            )

    def test_sandbox_blocks_double_underscore_prefixed_attr_on_credential_namespace(
        self,
    ):
        """__-prefixed non-dunder attrs (e.g. __secret) are blocked even on credential namespaces."""
        state = self._state(raw={"__secret": "leaked"})
        with self.assertRaises((SecurityError, Exception)):
            TemplateEngine.render("{{ raw.__secret }}", state)

    def test_credential_value_containing_template_syntax_is_literal(self):
        """A credential value that looks like a template is never re-rendered."""
        malicious = "{{ ().__class__.__bases__[0].__subclasses__() }}"
        state = self._state(raw={"host": malicious})
        result = TemplateEngine.render("{{ raw.host }}", state)
        self.assertEqual(malicious, result)

    def test_underscore_prefixed_credential_field_accessible(self):
        """Field names starting with _ (e.g. _user_agent_entry) work in templates."""
        state = self._state(raw={"_user_agent_entry": "Monte Carlo"})
        result = TemplateEngine.render(
            "{{ raw._user_agent_entry | default(none) }}", state
        )
        self.assertEqual("Monte Carlo", result)
