# tests/ctp/test_sql_server_ctp.py
import os
import pathlib
import stat
import subprocess
import threading
import time
from contextlib import suppress
from unittest import TestCase
from unittest.mock import Mock, patch

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.transforms import prepare_kerberos

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


def _release_tgt_lock_if_held() -> None:
    """Clear ``_TGT_LOCK`` if a previous test left it held.

    Lives here rather than in the production module: ``threading.Lock`` has no ownership,
    so a helper that releases it unconditionally would let any caller destroy the
    single-flight guarantee for a thread mid-kinit.
    """
    lock = prepare_kerberos._TGT_LOCK
    if lock.locked():
        try:
            lock.release()
        except RuntimeError:
            pass


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

        # These tests are about what the CTP *produces*; the password form's TGT
        # acquisition is covered in TestSqlServerKerberosTgtGuard. Make kinit succeed so
        # no password-form assertion depends on a reachable KDC.
        patcher = patch.object(
            prepare_kerberos.subprocess, "run", return_value=Mock(returncode=0)
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        _release_tgt_lock_if_held()

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

    def test_credential_with_neither_user_nor_username_does_not_explode(self):
        """`{{ raw.user | default(raw.username) }}` evaluates its argument eagerly, so a
        credential carrying NEITHER field renders a StrictUndefined and raises
        `'username' is undefined` -- an opaque Jinja error with nothing pointing at the
        real cause.

        Reachable whenever the kerberos step's `when` guard does not fire (auth_type
        absent, or not 'kerberos') on a credential with no SQL login. Predates the
        kerberos work, but Windows auth is the first shape that gets there.
        """
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {"host": "db.example.com", "port": 1433, "password": "s"},
        )
        self.assertNotIn("UID", args)

    def test_legacy_username_alias_still_resolves(self):
        """Guard for the fix above: the alias must keep working."""
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {
                "host": "db.example.com",
                "port": 1433,
                "username": "legacy",
                "password": "s",
            },
        )
        self.assertEqual("legacy", args["UID"])

    def test_timeouts_pass_through_the_mapper(self):
        """The base field map emits login_timeout and query_timeout_in_seconds, and
        SqlServerProxyClient pops both off connect_args -- so the schema has to declare
        them or the mapper rejects its own output.

        Latent until now: the sql path sends a pre-built ODBC string, so the mapper never
        ran for sql-server. Kerberos is the first credential to take the dict path.
        """
        args = self._resolve_kerberos(login_timeout=15, query_timeout_in_seconds=840)
        self.assertEqual(15, args["login_timeout"])
        self.assertEqual(840, args["query_timeout_in_seconds"])

    def test_timeouts_are_absent_when_not_supplied(self):
        """The mapper drops None, so an unset timeout must not appear as a key at all --
        the proxy client's pop() default is what supplies the fallback."""
        args = self._resolve_kerberos()
        self.assertNotIn("login_timeout", args)
        self.assertNotIn("query_timeout_in_seconds", args)

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
        self.assertTrue((os.environ.get("KRB5CCNAME") or "").startswith("MEMORY:"))

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


class TestSqlServerKerberosTgtGuard(TestCase):
    """The password form's TGT acquisition — SDD §5.3.

    The keytab form needs nothing: MIT Kerberos' default client keytab makes GSSAPI
    acquire and refresh the TGT itself. There is no equivalent for a stored password, so
    the agent must acquire one — but only when the cache lacks a valid ticket, since
    acquisition is a KDC round trip and tickets last ~10h.

    Verified against a live KDC on 2026-08-21 that a subprocess kinit populating a FILE
    ccache does satisfy msodbcsql's GSSAPI at connect time.
    """

    _REALM = "MCLAB.INTERNAL"
    _PRINCIPAL = "svc-montecarlo@MCLAB.INTERNAL"
    _PASSWORD = "p@ss-with-$dollar-and-'quote"
    _KEYTAB_B64 = "a2V5dGFiLWJ5dGVz"

    def setUp(self):
        self._saved_env = {
            key: os.environ.get(key)
            for key in ("KRB5_CONFIG", "KRB5_CLIENT_KTNAME", "KRB5CCNAME")
        }
        for key in self._saved_env:
            os.environ.pop(key, None)
        self._temp_paths: list[str] = []
        _release_tgt_lock_if_held()

    def tearDown(self):
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        for path in self._temp_paths:
            with suppress(OSError):
                os.unlink(path)
        _release_tgt_lock_if_held()

    def _credentials(self, **overrides) -> dict:
        creds = {
            "auth_type": "kerberos",
            "host": "labsql.mclab.internal",
            "port": 1433,
            "realm": self._REALM,
            "kdc": "labdc.mclab.internal",
            "principal": self._PRINCIPAL,
        }
        creds.update(overrides)
        return {k: v for k, v in creds.items() if v is not None}

    def _resolve(self, **overrides) -> dict:
        args = _resolve(SQL_SERVER_DEFAULT_CTP, self._credentials(**overrides))
        for key in ("KRB5_CONFIG", "KRB5_CLIENT_KTNAME"):
            if os.environ.get(key):
                self._temp_paths.append(os.environ[key])
        ccache = os.environ.get("KRB5CCNAME", "")
        if ccache.startswith("FILE:"):
            self._temp_paths.append(ccache.removeprefix("FILE:"))
        return args

    # ── The keytab form must not kinit ─────────────────────────────────

    @patch.object(prepare_kerberos.subprocess, "run")
    def test_keytab_form_never_invokes_kinit(self, mock_run):
        """GSSAPI auto-acquires from the client keytab; a kinit here would be redundant
        work on every connection and would defeat the design's whole point."""
        self._resolve(keytab_base64=self._KEYTAB_B64)
        mock_run.assert_not_called()

    # ── The password form acquires ─────────────────────────────────────

    @patch.object(prepare_kerberos.subprocess, "run")
    def test_password_form_acquires_a_tgt(self, mock_run):
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")  # kinit
        self._resolve(password=self._PASSWORD)

        self.assertEqual(1, mock_run.call_count)
        kinit_argv = mock_run.call_args_list[0].args[0]
        self.assertEqual("kinit", kinit_argv[0])
        self.assertIn(self._PRINCIPAL, kinit_argv)

    @patch.object(prepare_kerberos.subprocess, "run")
    def test_password_is_passed_on_stdin_never_in_argv(self, mock_run):
        """argv is world-readable via /proc; a password there leaks to any local process."""
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        self._resolve(password=self._PASSWORD)

        kinit_call = mock_run.call_args_list[0]
        self.assertNotIn(self._PASSWORD, kinit_call.args[0])
        self.assertIn(self._PASSWORD, kinit_call.kwargs.get("input", ""))

    @patch.object(prepare_kerberos.subprocess, "run")
    def test_a_ticket_is_always_acquired_rather_than_reused(self, mock_run):
        """No reuse probe. Each connection gets a fresh NamedTemporaryFile ccache, so a
        probe could only ever hit a *concurrent* connection's cache via the shared
        KRB5CCNAME and reuse its ticket under a different service principal."""
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        self._resolve(password=self._PASSWORD)
        self._resolve(password=self._PASSWORD)

        self.assertEqual(2, mock_run.call_count)
        for call in mock_run.call_args_list:
            self.assertEqual("kinit", call.args[0][0])

    # ── Failure handling ──────────────────────────────────────────────

    @patch.object(prepare_kerberos.subprocess, "run")
    def test_kinit_failure_raises_without_echoing_the_password(self, mock_run):
        mock_run.return_value = Mock(
            returncode=1, stdout="", stderr="kinit: Password incorrect"
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve(password=self._PASSWORD)

        message = str(ctx.exception)
        self.assertNotIn(self._PASSWORD, message)
        self.assertIn("Password incorrect", message)  # actionable

    @patch.object(prepare_kerberos.subprocess, "run")
    def test_kinit_timeout_is_reported_as_a_kdc_reachability_problem(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="kinit", timeout=30)
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve(password=self._PASSWORD)

        message = str(ctx.exception).lower()
        self.assertIn("kdc", message)
        self.assertNotIn(self._PASSWORD.lower(), message)

    @patch.object(prepare_kerberos.subprocess, "run")
    def test_missing_kinit_binary_is_actionable(self, mock_run):
        """krb5-user is a deployment prerequisite; say so rather than surfacing ENOENT."""
        mock_run.side_effect = FileNotFoundError("kinit")
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve(password=self._PASSWORD)
        self.assertIn("krb5", str(ctx.exception).lower())

    # ── Single-flight ─────────────────────────────────────────────────

    @patch.object(prepare_kerberos.subprocess, "run")
    def test_concurrent_threads_each_acquire_into_their_own_cache(self, mock_run):
        """Every connection gets its own ccache, so every one must acquire -- the lock keeps
        each KRB5CCNAME write paired with its own kinit rather than skipping work."""
        kinit_calls = []
        ccache_at_kinit = []

        def _fake_run(argv, **kwargs):
            kinit_calls.append(argv)
            ccache_at_kinit.append(os.environ.get("KRB5CCNAME"))
            time.sleep(0.05)  # widen the window a race would exploit
            return Mock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = _fake_run

        errors: list[BaseException] = []

        def _worker():
            try:
                self._resolve(password=self._PASSWORD)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual([], errors)
        self.assertEqual(8, len(kinit_calls))
        # The pairing is what the lock buys: an interleaved env write would make two kinits
        # target the same cache.
        self.assertEqual(8, len(set(ccache_at_kinit)))
