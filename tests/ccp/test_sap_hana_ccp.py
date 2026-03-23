# tests/ccp/test_sap_hana_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


class TestSapHanaCcp(TestCase):
    def test_sap_hana_registered(self):
        config = CcpRegistry.get("sap-hana")
        self.assertIsNotNone(config)
        self.assertEqual("sap-hana-default", config.name)

    def test_resolve_flat_credentials(self):
        result = CcpRegistry.resolve(
            "sap-hana",
            {
                "host": "hana.example.com",
                "port": 30015,
                "user": "SYSTEM",
                "password": "secret",
                "db_name": "HXE",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("hana.example.com", ca["address"])
        self.assertEqual(30015, ca["port"])
        self.assertEqual("SYSTEM", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual("HXE", ca["databaseName"])

    def test_host_mapped_to_address(self):
        result = CcpRegistry.resolve(
            "sap-hana",
            {"host": "hana.example.com", "port": 30015, "user": "u", "password": "p"},
        )
        self.assertIn("address", result["connect_args"])
        self.assertNotIn("host", result["connect_args"])

    def test_timeouts_converted_to_milliseconds(self):
        result = CcpRegistry.resolve(
            "sap-hana",
            {
                "host": "h",
                "port": 30015,
                "user": "u",
                "password": "p",
                "login_timeout_in_seconds": 10,
                "query_timeout_in_seconds": 30,
            },
        )
        ca = result["connect_args"]
        self.assertEqual(10000, ca["connectTimeout"])
        self.assertEqual(30000, ca["communicationTimeout"])

    def test_no_timeouts_when_not_provided(self):
        result = CcpRegistry.resolve(
            "sap-hana",
            {"host": "h", "port": 30015, "user": "u", "password": "p"},
        )
        ca = result["connect_args"]
        self.assertNotIn("connectTimeout", ca)
        self.assertNotIn("communicationTimeout", ca)

    def test_no_database_name_when_not_provided(self):
        result = CcpRegistry.resolve(
            "sap-hana",
            {"host": "h", "port": 30015, "user": "u", "password": "p"},
        )
        self.assertNotIn("databaseName", result["connect_args"])

    def test_resolve_legacy_credentials_unchanged(self):
        legacy = {
            "connect_args": {
                "address": "h",
                "port": 30015,
                "user": "u",
                "password": "p",
            }
        }
        self.assertEqual(legacy, CcpRegistry.resolve("sap-hana", legacy))
