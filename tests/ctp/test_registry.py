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

    def test_resolve_unregistered_type_with_connect_args_raises(self):
        # connect_args no longer bypasses the pipeline — unregistered types always raise
        with self.assertRaises(CtpPipelineError):
            CtpRegistry.resolve(
                "postgres", {"connect_args": {"host": "db.example.com"}}
            )


class TestStarburstGalaxyCtp(TestCase):
    def test_starburst_galaxy_registered(self):
        config = CtpRegistry.get("starburst-galaxy")
        self.assertIsNotNone(config)
        self.assertEqual("starburst-galaxy-default", config.name)

    def test_resolve_flat_starburst_galaxy_credentials(self):
        result = CtpRegistry.resolve(
            "starburst-galaxy",
            {
                "host": "mcdev-us-east-1-cluster.trino.galaxy.starburst.io",
                "port": "443",
                "user": "monte-carlo-service@mcdev.galaxy.starburst.io",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual(
            "mcdev-us-east-1-cluster.trino.galaxy.starburst.io", ca["host"]
        )
        self.assertEqual(443, ca["port"])
        self.assertIsInstance(ca["port"], int)
        self.assertEqual("https", ca["http_scheme"])
        self.assertEqual("monte-carlo-service@mcdev.galaxy.starburst.io", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertNotIn("catalog", ca)
        self.assertNotIn("schema", ca)

    def test_resolve_starburst_galaxy_with_catalog_and_schema(self):
        result = CtpRegistry.resolve(
            "starburst-galaxy",
            {
                "host": "cluster.trino.galaxy.starburst.io",
                "port": 443,
                "user": "svc@org.galaxy.starburst.io",
                "password": "secret",
                "catalog": "my_catalog",
                "schema": "my_schema",
            },
        )
        ca = result["connect_args"]
        self.assertEqual("my_catalog", ca["catalog"])
        self.assertEqual("my_schema", ca["schema"])

    def test_resolve_starburst_galaxy_dc_shaped_credentials(self):
        # DC pre-shapes credentials into connect_args before calling the agent.
        # The pipeline unwraps and re-runs the transform, producing the same output
        # as flat credentials would.
        result = CtpRegistry.resolve(
            "starburst-galaxy",
            {
                "connect_args": {
                    "host": "cluster.trino.galaxy.starburst.io",
                    "port": 443,
                    "user": "svc@org.galaxy.starburst.io",
                    "password": "secret",
                    "http_scheme": "https",
                }
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("cluster.trino.galaxy.starburst.io", ca["host"])
        self.assertEqual(443, ca["port"])
        self.assertIsInstance(ca["port"], int)
        self.assertEqual("https", ca["http_scheme"])
        self.assertEqual("svc@org.galaxy.starburst.io", ca["user"])
        self.assertEqual("secret", ca["password"])


class TestRedshiftCtp(TestCase):
    def test_registered(self):
        config = CtpRegistry.get("redshift")
        self.assertIsNotNone(config)
        self.assertEqual("redshift-default", config.name)

    def test_resolve_flat_credentials(self):
        result = CtpRegistry.resolve(
            "redshift",
            {
                "host": "cluster.abc123.us-east-1.redshift.amazonaws.com",
                "port": "5439",
                "db_name": "dev",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("cluster.abc123.us-east-1.redshift.amazonaws.com", ca["host"])
        self.assertEqual(5439, ca["port"])
        self.assertIsInstance(ca["port"], int)
        self.assertEqual("dev", ca["dbname"])
        self.assertEqual("admin", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual(1, ca["keepalives"])

    def test_resolve_dc_shaped_credentials(self):
        # DC pre-shapes credentials into connect_args before calling the agent.
        # The pipeline unwraps and re-runs the transform, producing the same output
        # as flat credentials would.
        result = CtpRegistry.resolve(
            "redshift",
            {
                "connect_args": {
                    "host": "cluster.abc123.us-east-1.redshift.amazonaws.com",
                    "port": 5439,
                    "dbname": "dev",
                    "user": "admin",
                    "password": "secret",
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                },
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("cluster.abc123.us-east-1.redshift.amazonaws.com", ca["host"])
        self.assertEqual(5439, ca["port"])
        self.assertEqual("dev", ca["dbname"])

    def test_resolve_dc_shaped_dbname_variants(self):
        # DC sends dbname (driver-native key); pipeline handles it via default() chaining
        for key in ("db_name", "dbname", "database"):
            dc_input = {
                "connect_args": {
                    "host": "h",
                    "port": 5439,
                    key: "mydb",
                    "user": "u",
                    "password": "p",
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                },
            }
            result = CtpRegistry.resolve("redshift", dc_input)
            self.assertEqual(
                "mydb", result["connect_args"]["dbname"], f"failed for key={key}"
            )
