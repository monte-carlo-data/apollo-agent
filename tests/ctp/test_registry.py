# tests/ctp/test_registry.py
from unittest import TestCase

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.registry import CtpRegistry


class TestCtpRegistry(TestCase):
    def test_unknown_type_returns_none(self):
        self.assertIsNone(CtpRegistry.get("not_a_real_type"))

    def test_resolve_unknown_type_raises(self):
        with self.assertRaises(CtpPipelineError):
            CtpRegistry.resolve("unknown_type", {"host": "db.example.com"})

    def test_resolve_legacy_credentials_returned_unchanged(self):
        legacy = {"connect_args": {"host": "db.example.com", "dbname": "mydb"}}
        self.assertEqual(legacy, CtpRegistry.resolve("postgres", legacy))
