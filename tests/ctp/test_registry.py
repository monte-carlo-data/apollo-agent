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
                "not_a_real_type", {"connect_args": {"host": "db.example.com"}}
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


class TestSapHanaCtp(TestCase):
    def test_registered(self):
        config = CtpRegistry.get("sap-hana")
        self.assertIsNotNone(config)
        self.assertEqual("sap-hana-default", config.name)

    def test_resolve_flat_credentials(self):
        result = CtpRegistry.resolve(
            "sap-hana",
            {
                "host": "hana.example.com",
                "port": 39015,
                "user": "SYSTEM",
                "password": "secret",
                "db_name": "HXE",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("hana.example.com", ca["address"])
        self.assertNotIn("host", ca)
        self.assertEqual(39015, ca["port"])
        self.assertEqual("SYSTEM", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual("HXE", ca["databaseName"])

    def test_resolve_flat_credentials_without_optional_fields(self):
        result = CtpRegistry.resolve(
            "sap-hana",
            {"host": "h", "port": 39015, "user": "u", "password": "p"},
        )
        ca = result["connect_args"]
        self.assertNotIn("databaseName", ca)
        self.assertNotIn("connectTimeout", ca)
        self.assertNotIn("communicationTimeout", ca)

    def test_resolve_flat_credentials_with_timeouts(self):
        result = CtpRegistry.resolve(
            "sap-hana",
            {
                "host": "h",
                "port": 39015,
                "user": "u",
                "password": "p",
                "login_timeout_in_seconds": 10,
                "query_timeout_in_seconds": 30,
            },
        )
        ca = result["connect_args"]
        self.assertEqual(10000, ca["connectTimeout"])
        self.assertEqual(30000, ca["communicationTimeout"])

    def test_resolve_dc_shaped_credentials(self):
        # DC pre-shapes credentials into connect_args before calling the agent.
        # The pipeline unwraps and re-runs the transform, producing the same output.
        result = CtpRegistry.resolve(
            "sap-hana",
            {
                "connect_args": {
                    "address": "hana.example.com",
                    "port": 39015,
                    "user": "SYSTEM",
                    "password": "secret",
                    "databaseName": "HXE",
                    "connectTimeout": 10000,
                    "communicationTimeout": 30000,
                }
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("hana.example.com", ca["address"])
        self.assertEqual(39015, ca["port"])
        self.assertEqual("HXE", ca["databaseName"])
        self.assertEqual(10000, ca["connectTimeout"])
        self.assertEqual(30000, ca["communicationTimeout"])


class TestStarburstEnterpriseCtp(TestCase):
    def test_registered(self):
        config = CtpRegistry.get("starburst-enterprise")
        self.assertIsNotNone(config)
        self.assertEqual("starburst-enterprise-default", config.name)

    def test_resolve_flat_credentials(self):
        result = CtpRegistry.resolve(
            "starburst-enterprise",
            {
                "host": "cluster.example.com",
                "port": "8443",
                "user": "svc",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("cluster.example.com", ca["host"])
        self.assertEqual(8443, ca["port"])
        self.assertIsInstance(ca["port"], int)
        self.assertEqual("svc", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual("https", ca["http_scheme"])
        self.assertNotIn("ssl_options", ca)
        self.assertNotIn("verify", ca)

    def test_resolve_dc_shaped_credentials(self):
        result = CtpRegistry.resolve(
            "starburst-enterprise",
            {
                "connect_args": {
                    "host": "cluster.example.com",
                    "port": 8443,
                    "user": "svc",
                    "password": "secret",
                    "http_scheme": "https",
                }
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("cluster.example.com", ca["host"])
        self.assertEqual(8443, ca["port"])
        self.assertEqual("https", ca["http_scheme"])

    def test_resolve_with_catalog_and_schema(self):
        result = CtpRegistry.resolve(
            "starburst-enterprise",
            {
                "host": "h",
                "port": "8443",
                "user": "u",
                "password": "p",
                "catalog": "my_catalog",
                "schema": "my_schema",
            },
        )
        ca = result["connect_args"]
        self.assertEqual("my_catalog", ca["catalog"])
        self.assertEqual("my_schema", ca["schema"])

    def test_resolve_ssl_disabled(self):
        result = CtpRegistry.resolve(
            "starburst-enterprise",
            {
                "host": "h",
                "port": "8443",
                "user": "u",
                "password": "p",
                "ssl_options": {"disabled": True},
            },
        )
        ca = result["connect_args"]
        self.assertIs(False, ca["verify"])
        self.assertNotIn("ssl_options", ca)

    def test_resolve_dc_shaped_ssl_disabled(self):
        # DC already resolved ssl_options → verify: False before calling agent.
        # Mapper passes verify through; step doesn't run (no ssl_options in raw).
        result = CtpRegistry.resolve(
            "starburst-enterprise",
            {
                "connect_args": {
                    "host": "h",
                    "port": 8443,
                    "user": "u",
                    "password": "p",
                    "http_scheme": "https",
                    "verify": False,
                }
            },
        )
        ca = result["connect_args"]
        self.assertIs(False, ca["verify"])
        self.assertNotIn("ssl_options", ca)


class TestSalesforceCrmCtp(TestCase):
    def test_registered(self):
        config = CtpRegistry.get("salesforce-crm")
        self.assertIsNotNone(config)
        self.assertEqual("salesforce-crm-default", config.name)

    def test_resolve_flat_token_credentials(self):
        result = CtpRegistry.resolve(
            "salesforce-crm",
            {
                "user": "admin@example.com",
                "password": "secret",
                "security_token": "ABC123",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("admin@example.com", ca["username"])
        self.assertNotIn("user", ca)
        self.assertEqual("secret", ca["password"])
        self.assertEqual("ABC123", ca["security_token"])
        self.assertNotIn("consumer_key", ca)
        self.assertNotIn("domain", ca)

    def test_resolve_flat_oauth_credentials(self):
        result = CtpRegistry.resolve(
            "salesforce-crm",
            {
                "consumer_key": "key123",
                "consumer_secret": "secret456",
                "domain": "myorg",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("key123", ca["consumer_key"])
        self.assertEqual("secret456", ca["consumer_secret"])
        self.assertEqual("myorg", ca["domain"])
        self.assertNotIn("username", ca)

    def test_domain_suffix_stripped(self):
        result = CtpRegistry.resolve(
            "salesforce-crm",
            {
                "consumer_key": "k",
                "consumer_secret": "s",
                "domain": "myorg.salesforce.com",
            },
        )
        self.assertEqual("myorg", result["connect_args"]["domain"])

    def test_resolve_dc_shaped_credentials(self):
        result = CtpRegistry.resolve(
            "salesforce-crm",
            {
                "connect_args": {
                    "username": "admin@example.com",
                    "password": "secret",
                    "security_token": "ABC123",
                }
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("admin@example.com", ca["username"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual("ABC123", ca["security_token"])


class TestPostgresCtp(TestCase):
    def test_registered(self):
        config = CtpRegistry.get("postgres")
        self.assertIsNotNone(config)
        self.assertEqual("postgres-default", config.name)

    def test_resolve_flat_credentials(self):
        result = CtpRegistry.resolve(
            "postgres",
            {
                "host": "db.example.com",
                "port": "5432",
                "database": "mydb",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("db.example.com", ca["host"])
        self.assertEqual(5432, ca["port"])
        self.assertIsInstance(ca["port"], int)
        self.assertEqual("mydb", ca["dbname"])
        self.assertEqual("admin", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertNotIn("sslmode", ca)

    def test_resolve_dc_shaped_credentials(self):
        result = CtpRegistry.resolve(
            "postgres",
            {
                "connect_args": {
                    "host": "db.example.com",
                    "port": 5432,
                    "dbname": "mydb",
                    "user": "admin",
                    "password": "secret",
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                }
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("db.example.com", ca["host"])
        self.assertEqual(5432, ca["port"])
        self.assertEqual("mydb", ca["dbname"])

    def test_resolve_dc_shaped_dbname_variants(self):
        # DC sends dbname (driver-native key); pipeline handles all variants via default() chaining
        for key in ("db_name", "dbname", "database"):
            dc_input = {
                "connect_args": {
                    "host": "h",
                    "port": 5432,
                    key: "mydb",
                    "user": "u",
                    "password": "p",
                }
            }
            result = CtpRegistry.resolve("postgres", dc_input)
            self.assertEqual(
                "mydb", result["connect_args"]["dbname"], f"failed for key={key}"
            )

    def test_resolve_with_ssl_mode(self):
        result = CtpRegistry.resolve(
            "postgres",
            {
                "host": "h",
                "port": 5432,
                "database": "d",
                "user": "u",
                "password": "p",
                "ssl_mode": "verify-full",
            },
        )
        self.assertEqual("verify-full", result["connect_args"]["sslmode"])


class TestMysqlCtp(TestCase):
    def test_registered(self):
        config = CtpRegistry.get("mysql")
        self.assertIsNotNone(config)
        self.assertEqual("mysql-default", config.name)

    def test_resolve_flat_credentials(self):
        result = CtpRegistry.resolve(
            "mysql",
            {
                "host": "db.example.com",
                "port": "3306",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("db.example.com", ca["host"])
        self.assertEqual(3306, ca["port"])
        self.assertIsInstance(ca["port"], int)
        self.assertEqual("admin", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertNotIn("ssl", ca)

    def test_resolve_flat_credentials_with_database(self):
        result = CtpRegistry.resolve(
            "mysql",
            {
                "host": "db.example.com",
                "port": "3306",
                "user": "admin",
                "password": "secret",
                "database": "mydb",
            },
        )
        self.assertEqual("mydb", result["connect_args"]["database"])

    def test_resolve_dc_shaped_credentials(self):
        result = CtpRegistry.resolve(
            "mysql",
            {
                "connect_args": {
                    "host": "db.example.com",
                    "port": 3306,
                    "user": "admin",
                    "password": "secret",
                }
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("db.example.com", ca["host"])
        self.assertEqual(3306, ca["port"])


class TestOracleCtp(TestCase):
    def test_registered(self):
        config = CtpRegistry.get("oracle")
        self.assertIsNotNone(config)
        self.assertEqual("oracle-default", config.name)

    def test_resolve_flat_credentials(self):
        result = CtpRegistry.resolve(
            "oracle",
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("db.example.com:1521/ORCL", ca["dsn"])
        self.assertEqual("admin", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual(1, ca["expire_time"])  # default applied by CTP

    def test_resolve_with_explicit_expire_time(self):
        result = CtpRegistry.resolve(
            "oracle",
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
                "expire_time": 5,
            },
        )
        self.assertEqual(5, result["connect_args"]["expire_time"])

    def test_resolve_dc_shaped_credentials(self):
        result = CtpRegistry.resolve(
            "oracle",
            {
                "connect_args": {
                    "dsn": "db.example.com:1521/ORCL",
                    "user": "admin",
                    "password": "secret",
                    "expire_time": 1,
                }
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("db.example.com:1521/ORCL", ca["dsn"])
        self.assertEqual(1, ca["expire_time"])
