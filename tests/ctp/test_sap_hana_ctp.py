# tests/ctp/test_sap_hana_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.sap_hana import SAP_HANA_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestSapHanaCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("sap-hana"))

    def test_resolve_flat_credentials(self):
        result = CtpPipeline().execute(
            SAP_HANA_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            SAP_HANA_DEFAULT_CTP,
            {"host": "hana.example.com", "port": 30015, "user": "u", "password": "p"},
        )
        self.assertIn("address", result)
        self.assertNotIn("host", result)

    def test_timeouts_converted_to_milliseconds(self):
        result = CtpPipeline().execute(
            SAP_HANA_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            SAP_HANA_DEFAULT_CTP,
            {"host": "h", "port": 30015, "user": "u", "password": "p"},
        )
        self.assertNotIn("connectTimeout", result)
        self.assertNotIn("communicationTimeout", result)

    def test_no_database_name_when_not_provided(self):
        result = CtpPipeline().execute(
            SAP_HANA_DEFAULT_CTP,
            {"host": "h", "port": 30015, "user": "u", "password": "p"},
        )
        self.assertNotIn("databaseName", result)
