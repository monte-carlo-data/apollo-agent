# tests/ctp/test_sql_server_ctp.py
import os
import pathlib
import stat
from contextlib import suppress
from unittest import TestCase
from unittest.mock import patch

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState
from apollo.integrations.ctp.template import TemplateEngine
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
        # No environment save/restore and no kinit stub: the transform materializes files
        # and reports their locations. Setting KRB5_* and acquiring the ticket belong to
        # sql_server_kerberos_env, tested in tests/test_sql_server_kerberos_env.py.
        self._temp_paths: list[str] = []

    def tearDown(self):
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
        params = args.get("kerberos") or {}
        for key in ("krb5_config_path", "client_keytab_path"):
            if params.get(key):
                self._temp_paths.append(params[key])
        ccache = params.get("ccache", "")
        if ccache.startswith("FILE:"):
            self._temp_paths.append(ccache.removeprefix("FILE:"))
        return args

    def _kerberos_params(self, **overrides) -> dict:
        """The block the proxy client consumes -- where the paths now live."""
        return self._resolve_kerberos(**overrides)["kerberos"]

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
        # The collector sends `database` in connect_args, so dropping it here silently
        # lands every Windows-auth session in master instead of failing.
        self.assertEqual("mcdemo", args["DATABASE"])

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

    def test_krb5_conf_written_and_its_path_is_reported(self):
        path = self._kerberos_params()["krb5_config_path"]
        self.assertTrue(os.path.exists(path))

    def test_krb5_conf_contents(self):
        conf = pathlib.Path(self._kerberos_params()["krb5_config_path"]).read_text()
        self.assertIn(f"default_realm = {self._REALM}", conf)
        self.assertIn(f"kdc = {self._KDC}", conf)
        # Explicit KDC + these two off means Kerberos needs only forward A-record
        # resolution: no SRV lookups, no PTR lookups. Removes the agent's
        # dependence on a fully AD-shaped resolver.
        self.assertIn("dns_lookup_kdc = false", conf)
        self.assertIn("rdns = false", conf)

    def test_krb5_conf_is_not_world_readable(self):
        path = self._kerberos_params()["krb5_config_path"]
        self.assertEqual(0o600, stat.S_IMODE(os.stat(path).st_mode))

    # ── Keytab handling ───────────────────────────────────────────────

    def test_keytab_decoded_from_base64_to_a_file(self):
        path = self._kerberos_params()["client_keytab_path"]
        self.assertEqual(b"keytab-bytes", pathlib.Path(path).read_bytes())

    def test_keytab_is_not_world_readable(self):
        """A keytab is a long-lived AD credential; 0600 is the floor."""
        path = self._kerberos_params()["client_keytab_path"]
        self.assertEqual(0o600, stat.S_IMODE(os.stat(path).st_mode))

    def test_keytab_form_uses_an_in_memory_ccache(self):
        """MEMORY: is correct here: GSSAPI acquires the TGT in-process from the client
        keytab, so nothing else needs to see the cache and the ticket never touches disk.
        """
        self.assertTrue(self._kerberos_params()["ccache"].startswith("MEMORY:"))

    def test_password_form_uses_a_file_ccache_not_memory(self):
        """Regression: MEMORY: cannot work for the password form.

        kinit runs as a separate process and populates its own per-process memory cache,
        which dies with it -- the connecting process then gets "No Kerberos credentials
        available (default cache: MEMORY:)". Confirmed against a live KDC on 2026-08-21:
        the keytab form passed and the password form failed exactly this way.

        A file cache is required so the acquiring and connecting processes share it.
        """
        params = self._kerberos_params(keytab_base64=None, password=self._PASSWORD)
        ccache = params["ccache"]
        self.assertNotEqual("MEMORY:", ccache)
        self.assertTrue(
            ccache.startswith("FILE:"), f"expected a FILE: ccache, got {ccache!r}"
        )

    def test_password_form_ccache_is_not_world_readable(self):
        params = self._kerberos_params(keytab_base64=None, password=self._PASSWORD)
        path = params["ccache"].removeprefix("FILE:")
        self.assertEqual(0o600, stat.S_IMODE(os.stat(path).st_mode))

    def test_password_form_reports_no_client_keytab_but_does_report_a_principal(self):
        """No keytab means no library auto-acquire, so the env module kinits instead --
        which needs the principal and password carried alongside the cache."""
        params = self._kerberos_params(keytab_base64=None, password=self._PASSWORD)
        self.assertNotIn("client_keytab_path", params)
        self.assertEqual(self._PRINCIPAL, params["principal"])
        self.assertEqual(self._PASSWORD, params["password"])
        self.assertTrue(params["krb5_config_path"])

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

        # The kerberos block carries per-resolve temp paths and a uuid ccache, so it can
        # never compare equal across two runs. Compare the ODBC args exactly, then assert
        # the block has the same shape.
        self.assertEqual(
            {k: v for k, v in flat.items() if k != "kerberos"},
            {k: v for k, v in wrapped["connect_args"].items() if k != "kerberos"},
        )
        self.assertEqual(
            set(flat["kerberos"]), set(wrapped["connect_args"]["kerberos"])
        )

    # ── Credential values that look like absence ──────────────────────

    def test_a_secret_of_literally_None_is_lost_in_the_template_engine(self):
        """Documents a known limitation, and pins where it actually lives (review F3 on
        #379).

        A password or keytab of literally "None" is reported as absent. The reviewer
        proposed fixing it by dropping the "None"-string case from this transform's
        _render, but that is not the mechanism: CTP renders through a NativeEnvironment,
        which literal_evals its output, so "None" becomes Python None inside the template
        engine before _render is reached.

        The fix therefore belongs in ctp/template.py and would affect every connector, so
        it is out of scope here. This test exists so the next person does not repeat the
        same dead-end investigation.
        """
        state = PipelineState(raw={"password": "None"}, derived={}, temp_files=[])
        self.assertIsNone(
            TemplateEngine.render("{{ raw.password | default(none) }}", state),
            "if this starts returning the string 'None', template.py changed and the "
            "transform-level guard is worth revisiting",
        )

    def test_an_absent_realm_rendering_as_None_is_still_rejected(self):
        """The reason a naive fix would be worse than the bug: the literal string "None"
        passes the identifier pattern, so letting it through would write it into the
        krb5.conf as a real realm."""
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(realm="None")
        self.assertIn("realm", str(ctx.exception))

    # ── Identifier validation ─────────────────────────────────────────

    def test_a_newline_in_the_realm_cannot_inject_krb5_conf_directives(self):
        """realm/kdc are interpolated into a krb5.conf that libkrb5 then reads.

        A newline would let a caller append arbitrary directives -- e.g. redirecting
        default_ccache_name to a path they control. The monolith validates these, but
        self-hosted credentials never pass through it: they go from the customer's secret
        store straight to the agent, so this is the only check on that path.
        """
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(
                realm="EVIL\n    default_ccache_name = FILE:/tmp/stolen",
            )
        self.assertIn("realm", str(ctx.exception))

    def test_a_newline_in_the_kdc_is_rejected(self):
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(kdc="kdc.example.com\n    udp_preference_limit = 1")
        self.assertIn("kdc", str(ctx.exception))

    def test_a_principal_starting_with_a_dash_is_rejected(self):
        """kinit would parse it as an option rather than a principal."""
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(principal="-X")
        self.assertIn("principal", str(ctx.exception))

    def test_validation_errors_never_echo_the_offending_value(self):
        """These errors reach the DC and are forwarded to Sentry."""
        with self.assertRaises(CtpPipelineError) as ctx:
            self._resolve_kerberos(realm="EVIL\n  default_ccache_name = FILE:/tmp/x")
        self.assertNotIn("/tmp/x", str(ctx.exception))

    def test_legitimate_identifier_shapes_are_accepted(self):
        """The guard must not reject real AD values -- a kdc may carry a :port."""
        params = self._kerberos_params(
            realm="CORP.EXAMPLE.COM",
            kdc="dc1.corp.example.com:88",
            principal="svc-mc/host@CORP.EXAMPLE.COM",
        )
        self.assertTrue(params["krb5_config_path"])

    def test_principals_ending_in_a_dollar_are_accepted(self):
        """gMSA and machine accounts end in "$".

        Microsoft recommends group Managed Service Accounts for SQL Server precisely
        because AD rotates their passwords, so rejecting them would block a recommended
        configuration -- a worse outcome than the injection this validation defends against.
        A "$" cannot inject anything: it is not special to krb5.conf and not an option to
        kinit.
        """
        for principal in (
            "mssql-gmsa$@CORP.EXAMPLE.COM",
            "SQLHOST$@CORP.EXAMPLE.COM",
        ):
            with self.subTest(principal=principal):
                params = self._kerberos_params(principal=principal)
                self.assertTrue(params["krb5_config_path"])

    def test_a_realm_with_an_underscore_is_accepted(self):
        """Legacy AD domains carry underscores."""
        params = self._kerberos_params(realm="CORP_LEGACY.EXAMPLE.COM")
        self.assertTrue(params["krb5_config_path"])

    # ── Storage medium ────────────────────────────────────────────────

    def test_artifacts_prefer_tmpfs_when_available(self):
        """A keytab is a long-lived AD credential and the ccache holds a live TGT, so both
        are kept off durable disk where the platform offers somewhere to do it. Asserted
        because the preference is a security property with no other enforcement.

        Spies on the chosen directory rather than the resulting path: /dev/shm does not
        exist on macOS, so faking isdir and letting the write proceed would just fail.
        """
        real_write = (
            prepare_kerberos.PrepareKerberosTransform._write_secure_temp_file_in
        )
        directories = []

        def _spy(contents, suffix, directory):
            directories.append((suffix, directory))
            return real_write(contents, suffix, None)

        with (
            patch.object(prepare_kerberos.os.path, "isdir", return_value=True),
            patch.object(
                prepare_kerberos.PrepareKerberosTransform,
                "_write_secure_temp_file_in",
                staticmethod(_spy),
            ),
        ):
            self._kerberos_params()
            self._kerberos_params(keytab_base64=None, password=self._PASSWORD)

        chosen = dict(directories)
        self.assertEqual("/dev/shm", chosen[".keytab"])
        self.assertEqual("/dev/shm", chosen[".ccache"])

    def test_falls_back_to_the_default_temp_dir_with_no_tmpfs(self):
        """A working connection matters more than the storage medium, so the absence of
        tmpfs must degrade rather than fail -- and the file must still be 0600."""
        with patch.object(prepare_kerberos.os.path, "isdir", return_value=False):
            params = self._kerberos_params()

        path = params["client_keytab_path"]
        self.assertFalse(path.startswith("/dev/shm/"))
        self.assertEqual(0o600, stat.S_IMODE(os.stat(path).st_mode))

    # ── Temp-file lifecycle ───────────────────────────────────────────

    def test_materialised_files_are_registered_for_cleanup(self):
        """A keytab left on disk after close is a long-lived AD credential leak.

        BaseProxyClient.close() deletes everything the pipeline reports in
        temp_files, so both artefacts have to be registered there.
        """
        temp_files: list[str] = []
        resolved = CtpRegistry.resolve(
            "sql-server", self._kerberos_credentials(), temp_files=temp_files
        )
        self._temp_paths.extend(temp_files)
        params = resolved["connect_args"]["kerberos"]

        self.assertEqual(
            2, len(temp_files), f"expected krb5.conf + keytab, got {temp_files}"
        )
        self.assertIn(params["krb5_config_path"], temp_files)
        self.assertIn(params["client_keytab_path"], temp_files)

    def test_password_form_registers_the_krb5_conf_and_the_ccache(self):
        """No keytab on this path, but the file ccache holds a live TGT, so it has to be
        cleaned up too."""
        temp_files: list[str] = []
        resolved = CtpRegistry.resolve(
            "sql-server",
            self._kerberos_credentials(keytab_base64=None, password=self._PASSWORD),
            temp_files=temp_files,
        )
        self._temp_paths.extend(temp_files)
        params = resolved["connect_args"]["kerberos"]

        self.assertEqual(
            2, len(temp_files), f"expected krb5.conf + ccache, got {temp_files}"
        )
        self.assertIn(params["krb5_config_path"], temp_files)
        self.assertIn(params["ccache"].removeprefix("FILE:"), temp_files)

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
