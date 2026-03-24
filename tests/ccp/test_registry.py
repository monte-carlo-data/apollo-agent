# tests/ccp/test_registry.py
from unittest import TestCase

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.registry import CcpRegistry


class TestCcpRegistry(TestCase):
    def test_unknown_type_returns_none(self):
        self.assertIsNone(CcpRegistry.get("not_a_real_type"))

    def test_resolve_unknown_type_raises(self):
        with self.assertRaises(CcpPipelineError):
            CcpRegistry.resolve("unknown_type", {"host": "db.example.com"})

    def test_resolve_legacy_credentials_returned_unchanged(self):
        legacy = {"connect_args": {"host": "db.example.com", "dbname": "mydb"}}
        self.assertEqual(legacy, CcpRegistry.resolve("postgres", legacy))
