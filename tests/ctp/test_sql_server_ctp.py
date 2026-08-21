# tests/ctp/test_sql_server_ctp.py
import os
import pathlib
import stat
from contextlib import suppress
from unittest import TestCase

from apollo.integrations.ctp.errors import CtpPipelineError

from apollo.credentials.schema import validate
from apollo.integrations.ctp.defaults.sql_server import (
    AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP,
    AZURE_SQL_DATABASE_DEFAULT_CTP,
    SQL_SERVER_CREDENTIALS_SCHEMA,
    SQL_SERVER_DEFAULT_CTP,
)
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry

_ALL_CONFIGS = [
    ("sql-server", SQL_SERVER_DEFAULT_CTP),
    ("azure-sql-database", AZURE_SQL_DATABASE_DEFAULT_CTP),
    ("azure-dedicated-sql-pool", AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP),
]


def _resolve(config, credentials: dict) -> dict:
    return CtpPipeline().execute(config, credentials)


class TestSqlServerCtp(TestCase):
    def test_sql_server_variants_registered(self):
        for connection_type, _ in _ALL_CONFIGS:
            with self.subTest(connection_type=connection_type):
                self.assertIsNotNone(CtpRegistry.get(connection_type))

    # ── Basic connection fields ────────────────────────────────────────

    def test_sql_server_basic_connection(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {
                "host": "db.example.com",
                "port": 1433,
                "user": "alice",
                "password": "secret",
            },
        )
        self.assertEqual("{ODBC Driver 17 for SQL Server}", args["DRIVER"])
        self.assertEqual("tcp:db.example.com,1433", args["SERVER"])
        self.assertEqual("alice", args["UID"])
        self.assertEqual("secret", args["PWD"])
        self.assertEqual("Yes", args["MARS_Connection"])

    def test_port_defaults_to_1433(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {"host": "db.example.com", "user": "u", "password": "p"},
        )
        self.assertEqual("tcp:db.example.com,1433", args["SERVER"])

    def test_username_field_alias(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {"host": "h", "port": 1433, "username": "bob", "password": "p"},
        )
        self.assertEqual("bob", args["UID"])

    def test_sql_server_no_database_field(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {"host": "h", "port": 1433, "user": "u", "password": "p"},
        )
        self.assertNotIn("DATABASE", args)

    # ── Azure variants — DATABASE field ───────────────────────────────

    def test_azure_sql_database_includes_database(self):
        args = _resolve(
            AZURE_SQL_DATABASE_DEFAULT_CTP,
            {
                "host": "myserver.database.windows.net",
                "port": 1433,
                "user": "u",
                "password": "p",
                "db_name": "mydb",
            },
        )
        self.assertEqual("mydb", args["DATABASE"])
        self.assertEqual("tcp:myserver.database.windows.net,1433", args["SERVER"])

    def test_azure_dedicated_sql_pool_includes_database(self):
        args = _resolve(
            AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP,
            {
                "host": "mypool.sql.azuresynapse.net",
                "port": 1433,
                "user": "u",
                "password": "p",
                "database": "mypool_db",
            },
        )
        self.assertEqual("mypool_db", args["DATABASE"])

    def test_azure_database_field_alias(self):
        # db_name takes precedence over database when both present
        args = _resolve(
            AZURE_SQL_DATABASE_DEFAULT_CTP,
            {
                "host": "h",
                "port": 1433,
                "user": "u",
                "password": "p",
                "db_name": "primary",
                "database": "fallback",
            },
        )
        self.assertEqual("primary", args["DATABASE"])

    # ── Azure variants share base fields ─────────────────────────────

    def test_azure_variants_share_driver_and_mars(self):
        for _, config in [
            ("azure-sql-database", AZURE_SQL_DATABASE_DEFAULT_CTP),
            ("azure-dedicated-sql-pool", AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP),
        ]:
            with self.subTest(config=config.name):
                args = _resolve(
                    config,
                    {
                        "host": "h",
                        "port": 1433,
                        "user": "u",
                        "password": "p",
                        "db_name": "d",
                    },
                )
                self.assertEqual("{ODBC Driver 17 for SQL Server}", args["DRIVER"])
                self.assertEqual("Yes", args["MARS_Connection"])


# ══════════════════════════════════════════════════════════════════════
# Windows Authentication (Kerberos) — PRO-3016
# ══════════════════════════════════════════════════════════════════════


class TestSqlServerKerberosCtp(TestCase):
    """The auth_type=kerberos path on the sql-server CTP.

    Kerberos setup is one cohesive operation -- krb5.conf, keytab, and the MIT
    environment variables must all agree, and it must no-op entirely on the sql
    path -- so it lives in a single `prepare_kerberos` transform rather than being
    assembled from write_ini_file + tmp_file_write. (write_ini_file could not
    produce a krb5.conf anyway: it emits one flat [section] of key=value lines,
    and krb5.conf needs multiple sections plus the nested `REALM = { kdc = ... }`
    brace form.)
    """

    _REALM = "MCLAB.INTERNAL"
    _KDC = "labdc.mclab.internal"
    _PRINCIPAL = "svc-montecarlo@MCLAB.INTERNAL"
    # "keytab-bytes" -- content is irrelevant, but it must survive base64 round-trip.
    _KEYTAB_B64 = "a2V5dGFiLWJ5dGVz"
    _PASSWORD = "sup3r-s3cret-pw"

    def setUp(self):
        # prepare_kerberos exports KRB5_* into the process environment, so each
        # test starts from a known state and restores afterwards.
        self._saved_env = {
            key: os.environ.get(key)
            for key in ("KRB5_CONFIG", "KRB5_CLIENT_KTNAME", "KRB5CCNAME")
        }
        for key in self._saved_env:
            os.environ.pop(key, None)
        self._temp_paths: list[str] = []

    def tearDown(self):
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        for path in self._temp_paths:
            with suppress(OSError):
                os.unlink(path)

    def _kerberos_credentials(self, **overrides) -> dict:
        creds = {
            "auth_type": "kerberos",
            "host": "labsql.mclab.internal",
            "port": 1433,
            "database": "mcdemo",
            "realm": self._REALM,
            "kdc": self._KDC,
            "principal": self._PRINCIPAL,
            "keytab_base64": self._KEYTAB_B64,
        }
        creds.update(overrides)
        return {k: v for k, v in creds.items() if v is not None}

    def _resolve_kerberos(self, **overrides) -> dict:
        args = _resolve(SQL_SERVER_DEFAULT_CTP, self._kerberos_credentials(**overrides))
        for key in ("KRB5_CONFIG", "KRB5_CLIENT_KTNAME"):
            if os.environ.get(key):
                self._temp_paths.append(os.environ[key])
        return args

    # ── The connection string change ──────────────────────────────────

    def test_kerberos_emits_trusted_connection(self):
        args = self._resolve_kerberos()
        self.assertEqual("yes", args["Trusted_Connection"])

    def test_kerberos_drops_uid_and_pwd(self):
        """The whole point: no credentials on the wire, AD vouches for us instead.

        Leaving PWD in place would also send the AD service-account password to
        SQL Server as a SQL login attempt, which is exactly what the customer
        banned.
        """
        args = self._resolve_kerberos()
        self.assertNotIn("UID", args)
        self.assertNotIn("PWD", args)

    def test_kerberos_keeps_server_and_database(self):
        args = self._resolve_kerberos()
        self.assertEqual("tcp:labsql.mclab.internal,1433", args["SERVER"])
        self.assertEqual("{ODBC Driver 17 for SQL Server}", args["DRIVER"])

    def test_sql_path_is_unchanged_by_the_kerberos_addition(self):
        """Regression guard: every existing SQL Server customer rides this path."""
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {"host": "db.example.com", "port": 1433, "user": "alice", "password": "s"},
        )
        self.assertEqual("alice", args["UID"])
        self.assertEqual("s", args["PWD"])
        self.assertNotIn("Trusted_Connection", args)

    def test_absent_auth_type_defaults_to_sql_path(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {"host": "h", "port": 1433, "user": "u", "password": "p"},
        )
        self.assertNotIn("Trusted_Connection", args)
        self.assertEqual("u", args["UID"])

    def test_auth_type_is_case_insensitive(self):
        args = self._resolve_kerberos(auth_type="Kerberos")
        self.assertEqual("yes", args["Trusted_Connection"])

    # ── krb5.conf assembly ────────────────────────────────────────────

    def test_krb5_conf_written_and_env_var_points_at_it(self):
        self._resolve_kerberos()
        path = os.environ.get("KRB5_CONFIG")
        self.assertIsNotNone(path, "KRB5_CONFIG must be exported for libkrb5 to see it")
        self.assertTrue(os.path.exists(path))

    def test_krb5_conf_contents(self):
        self._resolve_kerberos()
        conf = pathlib.Path(os.environ["KRB5_CONFIG"]).read_text()
        self.assertIn(f"default_realm = {self._REALM}", conf)
        self.assertIn(f"kdc = {self._KDC}", conf)
        # Explicit KDC + these two off means Kerberos needs only forward A-record
        # resolution: no SRV lookups, no PTR lookups. Removes the agent's
        # dependence on a fully AD-shaped resolver.
        self.assertIn("dns_lookup_kdc = false", conf)
        self.assertIn("rdns = false", conf)

    def test_krb5_conf_is_not_world_readable(self):
        self._resolve_kerberos()
        mode = stat.S_IMODE(os.stat(os.environ["KRB5_CONFIG"]).st_mode)
        self.assertEqual(0o600, mode)

    # ── Keytab handling ───────────────────────────────────────────────

    def test_keytab_decoded_from_base64_to_a_file(self):
        self._resolve_kerberos()
        path = os.environ.get("KRB5_CLIENT_KTNAME")
        self.assertIsNotNone(path, "KRB5_CLIENT_KTNAME drives GSSAPI auto-acquire")
        self.assertEqual(b"keytab-bytes", pathlib.Path(path).read_bytes())

    def test_keytab_is_not_world_readable(self):
        """A keytab is a long-lived AD credential; 0600 is the floor."""
        self._resolve_kerberos()
        mode = stat.S_IMODE(os.stat(os.environ["KRB5_CLIENT_KTNAME"]).st_mode)
        self.assertEqual(0o600, mode)

    def test_keytab_form_uses_an_in_memory_ccache(self):
        """MEMORY: is correct here: GSSAPI acquires the TGT in-process from the client
        keytab, so nothing else needs to see the cache and the ticket never touches disk.
        """
        self._resolve_kerberos()
        self.assertEqual("MEMORY:", os.environ.get("KRB5CCNAME"))

    def test_password_form_uses_a_file_ccache_not_memory(self):
        """Regression: MEMORY: cannot work for the password form.

        kinit runs as a separate process and populates its own per-process memory cache,
        which dies with it -- the connecting process then gets "No Kerberos credentials
        available (default cache: MEMORY:)". Confirmed against a live KDC on 2026-08-21:
        the keytab form passed and the password form failed exactly this way.

        A file cache is required so the acquiring and connecting processes share it.
        """
        self._resolve_kerberos(keytab_base64=None, password=self._PASSWORD)
        ccache = os.environ.get("KRB5CCNAME", "")
        self.assertNotEqual("MEMORY:", ccache)
        self.assertTrue(
            ccache.startswith("FILE:"), f"expected a FILE: ccache, got {ccache!r}"
        )

    def test_password_form_ccache_is_not_world_readable(self):
        self._resolve_kerberos(keytab_base64=None, password=self._PASSWORD)
        path = os.environ["KRB5CCNAME"].removeprefix("FILE:")
        self._temp_paths.append(path)
        self.assertEqual(0o600, stat.S_IMODE(os.stat(path).st_mode))

    def test_password_form_sets_no_client_keytab(self):
        """No keytab means no library auto-acquire; the client kinits instead."""
        self._resolve_kerberos(keytab_base64=None, password=self._PASSWORD)
        self.assertIsNone(os.environ.get("KRB5_CLIENT_KTNAME"))
        self.assertIsNotNone(os.environ.get("KRB5_CONFIG"))

    def test_password_form_still_drops_pwd_from_odbc_args(self):
        """The password is for kinit against the KDC, never for SQL Server."""
        args = self._resolve_kerberos(keytab_base64=None, password=self._PASSWORD)
        self.assertNotIn("PWD", args)
        self.assertEqual("yes", args["Trusted_Connection"])

    # ── Validation ────────────────────────────────────────────────────

    def test_rejects_keytab_and_password_together(self):
        """Ambiguous: which credential authenticated? SDD 5.2 rejects the mix."""
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(password=self._PASSWORD)
        self.assertIn("keytab", str(ctx.exception).lower())

    def test_rejects_neither_keytab_nor_password(self):
        with self.assertRaises(CtpPipelineError):
            self._resolve_kerberos(keytab_base64=None)

    def test_requires_realm(self):
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(realm=None)
        self.assertIn("realm", str(ctx.exception).lower())

    def test_requires_kdc(self):
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(kdc=None)
        self.assertIn("kdc", str(ctx.exception).lower())

    def test_requires_principal(self):
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(principal=None)
        self.assertIn("principal", str(ctx.exception).lower())

    def test_rejects_malformed_base64_keytab(self):
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(keytab_base64="not!valid!base64!")
        self.assertIn("keytab", str(ctx.exception).lower())

    def test_error_messages_never_contain_the_credential(self):
        """CTP errors reach the DC and are forwarded to Sentry."""
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(realm=None, password=self._PASSWORD)
        message = str(ctx.exception)
        self.assertNotIn(self._PASSWORD, message)
        self.assertNotIn(self._KEYTAB_B64, message)

    # ── DC passthrough shape ──────────────────────────────────────────

    def test_dc_shaped_credentials_produce_the_same_output(self):
        """The DC pre-wraps everything in connect_args today; both shapes must work.

        Unwrapping connect_args is CtpRegistry.resolve()'s job, not the bare
        pipeline's, so this goes through the registry -- which is also the entry
        point the agent actually calls.
        """
        flat = self._resolve_kerberos()

        temp_files: list[str] = []
        wrapped = CtpRegistry.resolve(
            "sql-server",
            {"connect_args": self._kerberos_credentials()},
            temp_files=temp_files,
        )
        self._temp_paths.extend(temp_files)

        self.assertEqual(flat, wrapped["connect_args"])

    # ── Temp-file lifecycle ───────────────────────────────────────────

    def test_materialised_files_are_registered_for_cleanup(self):
        """A keytab left on disk after close is a long-lived AD credential leak.

        BaseProxyClient.close() deletes everything the pipeline reports in
        temp_files, so both artefacts have to be registered there.
        """
        temp_files: list[str] = []
        CtpRegistry.resolve(
            "sql-server", self._kerberos_credentials(), temp_files=temp_files
        )
        self._temp_paths.extend(temp_files)

        self.assertEqual(
            2, len(temp_files), f"expected krb5.conf + keytab, got {temp_files}"
        )
        self.assertIn(os.environ["KRB5_CONFIG"], temp_files)
        self.assertIn(os.environ["KRB5_CLIENT_KTNAME"], temp_files)

    def test_password_form_registers_the_krb5_conf_and_the_ccache(self):
        """No keytab on this path, but the file ccache holds a live TGT, so it has to be
        cleaned up too."""
        temp_files: list[str] = []
        CtpRegistry.resolve(
            "sql-server",
            self._kerberos_credentials(keytab_base64=None, password=self._PASSWORD),
            temp_files=temp_files,
        )
        self._temp_paths.extend(temp_files)

        self.assertEqual(
            2, len(temp_files), f"expected krb5.conf + ccache, got {temp_files}"
        )
        self.assertIn(os.environ["KRB5_CONFIG"], temp_files)
        self.assertIn(os.environ["KRB5CCNAME"].removeprefix("FILE:"), temp_files)

    def test_sql_path_materialises_no_files(self):
        temp_files: list[str] = []
        CtpRegistry.resolve(
            "sql-server",
            {"host": "h", "port": 1433, "user": "u", "password": "p"},
            temp_files=temp_files,
        )
        self.assertEqual([], temp_files)


class TestSqlServerKerberosCredentialsSchema(TestCase):
    """The customer-facing self-hosted credentials JSON.

    This is the validation surface a customer authors by hand (and the shape both
    PRO-3016 labs publish into Key Vault / Secrets Manager), so it has to accept the
    Kerberos forms while still rejecting an incomplete one. Mutual exclusion of keytab
    and password falls out of oneof_schema: supplying both matches neither variant.
    """

    _BASE = {
        "host": "labsql.mclab.internal",
        "port": 1433,
        "database": "mcdemo",
        "auth_type": "kerberos",
        "realm": "MCLAB.INTERNAL",
        "kdc": "labdc.mclab.internal",
        "principal": "svc-montecarlo@MCLAB.INTERNAL",
    }

    def _validate(self, connect_args: dict):
        return validate(SQL_SERVER_CREDENTIALS_SCHEMA, {"connect_args": connect_args})

    def _assert_valid(self, connect_args: dict) -> None:
        errors = self._validate(connect_args)
        self.assertFalse(errors, f"expected valid, got errors: {errors}")

    def _assert_invalid(self, connect_args: dict) -> None:
        self.assertTrue(self._validate(connect_args), "expected a validation error")

    def test_sql_form_still_valid(self):
        """Regression guard for every existing SQL Server customer."""
        self._assert_valid(
            {"host": "db.example.com", "port": 1433, "user": "u", "password": "p"}
        )

    def test_keytab_form_valid(self):
        self._assert_valid({**self._BASE, "keytab_base64": "a2V5"})

    def test_password_form_valid(self):
        self._assert_valid({**self._BASE, "password": "pw"})

    def test_both_keytab_and_password_rejected(self):
        self._assert_invalid({**self._BASE, "keytab_base64": "a2V5", "password": "pw"})

    def test_kerberos_without_a_credential_rejected(self):
        self._assert_invalid(dict(self._BASE))

    def test_kerberos_missing_realm_rejected(self):
        creds = {**self._BASE, "keytab_base64": "a2V5"}
        del creds["realm"]
        self._assert_invalid(creds)

    def test_kerberos_missing_principal_rejected(self):
        creds = {**self._BASE, "keytab_base64": "a2V5"}
        del creds["principal"]
        self._assert_invalid(creds)

    def test_legacy_odbc_string_form_still_accepted(self):
        """The docs document connect_args as a pre-built ODBC string; unchanged."""
        self.assertFalse(
            validate(
                SQL_SERVER_CREDENTIALS_SCHEMA,
                {"connect_args": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=..."},
            )
        )
