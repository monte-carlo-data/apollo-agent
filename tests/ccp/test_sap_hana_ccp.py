# tests/ccp/test_sap_hana_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.defaults.sap_hana import SAP_HANA_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestSapHanaCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("sap-hana"))

    def test_resolve_flat_credentials(self):
        result = CcpPipeline().execute(
            SAP_HANA_DEFAULT_CCP,
            {
                "host": "hana.example.com",
                "port": 30015,
                "user": "SYSTEM",
                "password": "secret",
                "db_name": "HXE",
            },
        )
        self.assertEqual("hana.example.com", result["address"])
        self.assertEqual(30015, result["port"])
        self.assertEqual("SYSTEM", result["user"])
        self.assertEqual("secret", result["password"])
        self.assertEqual("HXE", result["databaseName"])

    def test_host_mapped_to_address(self):
        result = CcpPipeline().execute(
            SAP_HANA_DEFAULT_CCP,
            {"host": "hana.example.com", "port": 30015, "user": "u", "password": "p"},
        )
        self.assertIn("address", result)
        self.assertNotIn("host", result)

    def test_timeouts_converted_to_milliseconds(self):
        result = CcpPipeline().execute(
            SAP_HANA_DEFAULT_CCP,
            {
                "host": "h",
                "port": 30015,
                "user": "u",
                "password": "p",
                "login_timeout_in_seconds": 10,
                "query_timeout_in_seconds": 30,
            },
        )
        self.assertEqual(10000, result["connectTimeout"])
        self.assertEqual(30000, result["communicationTimeout"])

    def test_no_timeouts_when_not_provided(self):
        result = CcpPipeline().execute(
            SAP_HANA_DEFAULT_CCP,
            {"host": "h", "port": 30015, "user": "u", "password": "p"},
        )
        self.assertNotIn("connectTimeout", result)
        self.assertNotIn("communicationTimeout", result)

    def test_no_database_name_when_not_provided(self):
        result = CcpPipeline().execute(
            SAP_HANA_DEFAULT_CCP,
            {"host": "h", "port": 30015, "user": "u", "password": "p"},
        )
        self.assertNotIn("databaseName", result)
