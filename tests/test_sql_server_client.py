import datetime
import json
import logging
import os
from contextlib import suppress
from typing import (
    Iterable,
    List,
    Any,
    Optional,
)
from unittest import TestCase
from unittest.mock import Mock, call, patch
import pyodbc
from psycopg2.errors import InsufficientPrivilege  # noqa

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.db.sql_server_proxy_client import SqlServerProxyClient
from apollo.common.agent.redact import AgentRedactUtilities
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.db import sql_server_kerberos_env
from apollo.interfaces.lambda_function.json_log_formatter import JsonLogFormatter

_SQL_SERVER_CREDENTIALS = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER=tcp:www.fake.com;"
    f"PORT=1433;"
    f"DATABASE=my_db;"
    f"UID=user;"
    f"PWD=password"
)


class SqlServerClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor
        self.maxDiff = None

    @patch("pyodbc.connect")
    def test_dict_connect_args_serialized(self, mock_connect):
        # CTP path: connect_args is a dict produced by the pipeline; proxy client
        # serializes it to an ODBC connection string before calling pyodbc.connect.
        mock_connect.return_value = self._mock_connection
        SqlServerProxyClient(
            credentials={
                "connect_args": {
                    "DRIVER": "{ODBC Driver 17 for SQL Server}",
                    "SERVER": "tcp:db.example.com,1433",
                    "UID": "alice",
                    "PWD": "s3cr3t",
                    "MARS_Connection": "Yes",
                }
            }
        )
        expected = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=tcp:db.example.com,1433;"
            "UID=alice;"
            "PWD=s3cr3t;"
            "MARS_Connection=Yes"
        )
        mock_connect.assert_called_once_with(expected, timeout=15)

    @patch("pyodbc.connect")
    def test_dict_login_timeout_used_and_not_in_odbc_string(self, mock_connect):
        mock_connect.return_value = self._mock_connection
        SqlServerProxyClient(
            credentials={
                "connect_args": {
                    "DRIVER": "{ODBC Driver 17 for SQL Server}",
                    "SERVER": "tcp:db.example.com,1433",
                    "UID": "alice",
                    "PWD": "s3cr3t",
                    "MARS_Connection": "Yes",
                    "login_timeout": 42,
                }
            }
        )
        args, kwargs = mock_connect.call_args
        self.assertEqual(42, kwargs.get("timeout", args[1] if len(args) > 1 else None))
        self.assertNotIn("login_timeout", args[0])

    @patch("pyodbc.connect")
    def test_dict_query_timeout_used_and_not_in_odbc_string(self, mock_connect):
        mock_connect.return_value = self._mock_connection
        client = SqlServerProxyClient(
            credentials={
                "connect_args": {
                    "DRIVER": "{ODBC Driver 17 for SQL Server}",
                    "SERVER": "tcp:db.example.com,1433",
                    "UID": "alice",
                    "PWD": "s3cr3t",
                    "MARS_Connection": "Yes",
                    "query_timeout_in_seconds": 99,
                }
            }
        )
        self.assertEqual(99, client.wrapped_client.timeout)
        odbc_string = mock_connect.call_args[0][0]
        self.assertNotIn("query_timeout_in_seconds", odbc_string)

    @patch("pyodbc.connect")
    def test_query(self, mock_connect):
        query = (
            "SELECT name, value FROM table OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"  # noqa
        )
        args = [0, 2]
        expected_data = [
            [
                "name_1",
                11.1,
            ],
            [
                "name_2",
                22.2,
            ],
        ]
        expected_description = [
            ["name", str.__class__, None, None, None, None, None],
            ["value", float.__class__, None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect, query, args, expected_data, expected_description
        )

    @patch("pyodbc.connect")
    def test_datetime_query(self, mock_connect):
        query = "SELECT name, created_date, updated_datetime FROM table"  # noqa
        data = [
            [
                "name_1",
                datetime.date.fromisoformat("2023-11-01"),
                datetime.datetime.fromisoformat("2023-11-01T10:59:00"),
            ],
        ]
        description = [
            ["name", str.__class__, None, None, None, None, None],
            ["created_date", str.__class__, None, None, None, None, None],
            ["updated_datetime", str.__class__, None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, None, data, description)

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
        query_args: Optional[Iterable[Any]],
        data: List,
        description: List,
        raise_exception: Optional[Exception] = None,
        expected_error_type: Optional[str] = None,
    ):
        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {
                    "target": "_cursor",
                    "method": "execute",
                    "args": [
                        query,
                        query_args,
                    ],
                },
                {"target": "_cursor", "method": "fetchall", "store": "tmp_1"},
                {"target": "_cursor", "method": "description", "store": "tmp_2"},
                {"target": "_cursor", "method": "rowcount", "store": "tmp_3"},
                {
                    "target": "__utils",
                    "method": "build_dict",
                    "kwargs": {
                        "all_results": {"__reference__": "tmp_1"},
                        "description": {"__reference__": "tmp_2"},
                        "rowcount": {"__reference__": "tmp_3"},
                    },
                },
            ],
        }
        mock_connect.return_value = self._mock_connection

        expected_rows = len(data)

        if raise_exception:
            self._mock_cursor.execute.side_effect = raise_exception
        self._mock_cursor.fetchall.return_value = data
        self._mock_cursor.description.return_value = description
        self._mock_cursor.rowcount.return_value = expected_rows

        response = self._agent.execute_operation(
            "sql-server",
            "run_query",
            operation_dict,
            {
                "connect_args": _SQL_SERVER_CREDENTIALS,
            },
        )

        if raise_exception:
            self.assertEqual(
                str(raise_exception), response.result.get(ATTRIBUTE_NAME_ERROR)
            )
            self.assertEqual(
                expected_error_type, response.result.get(ATTRIBUTE_NAME_ERROR_TYPE)
            )
            return

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        result = response.result.get(ATTRIBUTE_NAME_RESULT)

        mock_connect.assert_called_with(_SQL_SERVER_CREDENTIALS, timeout=15)
        self._mock_cursor.execute.assert_has_calls(
            [
                call(query, query_args if query_args else None),
            ]
        )
        self._mock_cursor.description.assert_called()
        self._mock_cursor.rowcount.assert_called()

        expected_data = self._serialized_data(data)
        self.assertTrue("all_results" in result)
        self.assertEqual(expected_data, result["all_results"])

        expected_description = self._serialized_description(description)
        self.assertTrue("description" in result)
        self.assertEqual(expected_description, result["description"])

        self.assertTrue("rowcount" in result)
        self.assertEqual(expected_rows, result["rowcount"])

    @classmethod
    def _serialized_description(cls, description: List) -> List:
        return [cls._serialized_col(v) for v in description]

    @classmethod
    def _serialized_col(cls, col: List) -> List:
        return [col[0], col[1].__name__, col[2], col[3], col[4], col[5], col[6]]

    @classmethod
    def _serialized_data(cls, data: List) -> List:
        return [cls._serialized_row(v) for v in data]

    @classmethod
    def _serialized_row(cls, row: List) -> List:
        return [cls._serialized_value(v) for v in row]

    @classmethod
    def _serialized_value(cls, value: Any) -> Any:
        if isinstance(value, datetime.datetime):
            return {
                "__type__": "datetime",
                "__data__": value.isoformat(),
            }
        elif isinstance(value, datetime.date):
            return {
                "__type__": "date",
                "__data__": value.isoformat(),
            }
        else:
            return value

    def test_handle_datetimeoffset(self):
        # 2025-12-10T12:32:10.000019+01:00 represented as binary
        datetimeoffset_as_binary = (
            b"\xe9\x07\x0c\x00\n\x00\x0c\x00 \x00\n\x008J\x00\x00\x01\x00\x00\x00"
        )

        expected_datetime = datetime.datetime(
            year=2025,
            month=12,
            day=10,
            hour=12,
            minute=32,
            second=10,
            microsecond=19,
            tzinfo=datetime.timezone(datetime.timedelta(hours=1, minutes=0)),
        )

        # Convert it to datetime
        response = SqlServerProxyClient._handle_datetimeoffset(datetimeoffset_as_binary)

        self.assertEqual(response, expected_datetime)


class SqlServerCredentialSafetyTests(TestCase):
    """The SQL Server client passes credentials as a single ODBC connection string
    (``UID=...;PWD=...``) straight to ``pyodbc.connect``, so it is the structurally-
    exposed client: if that string surfaces in a connection exception, the password
    must be stripped before the error reaches the SaaS. Regression for the
    error-response credential-leak finding (HIGH, CVSS 7.5)."""

    _SERVER = "tcp:db.example.com,1433"
    _PASSWORD = "S3cr3tMsSqlPwd"

    _CONNECTION_STRING = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={_SERVER};"
        "DATABASE=prod;"
        "UID=alice;"
        f"PWD={_PASSWORD}"
    )

    _OPERATION = {
        "trace_id": "ctp-safety-test",
        "skip_cache": True,
        "commands": [
            {"method": "cursor", "store": "_cursor"},
        ],
    }

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch("pyodbc.connect")
    def test_connection_string_in_exception_does_not_leak_credentials(
        self, mock_connect
    ):
        # Fail the connection with an exception that echoes the ODBC connection string
        # back (UID/PWD included) — the worst case if pyodbc or a wrapper surfaces the DSN.
        def _raise_echoing_connection_string(connection_string, *args, **kwargs):
            raise pyodbc.OperationalError(
                "08001",
                f"[08001] Login timeout expired; connection string: {connection_string}",
            )

        mock_connect.side_effect = _raise_echoing_connection_string

        response = self._agent.execute_operation(
            "sql-server",
            "run_query",
            self._OPERATION,
            {"connect_args": self._CONNECTION_STRING},
        )

        serialized = json.dumps(response.result, default=str)
        # safe: the password value never reaches the SaaS, anywhere in the response
        self.assertNotIn(self._PASSWORD, serialized)
        # actionable: the server and the error context survive for debugging
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn(self._SERVER, error)
        self.assertIn("Login timeout expired", error)


class SqlServerKerberosCredentialSafetyTests(TestCase):
    """Response safety for the Windows-authentication path — PRO-3016.

    The Kerberos path shifts what a leak would expose. No credential goes on the wire,
    so ``UID``/``PWD`` are absent from the connection string, but the credential *does*
    pass through the agent: a base64 keytab (a long-lived AD credential, worse than a
    password since it mints tickets until the account is rotated) or the service-account
    password used for kinit.

    Both must be absent from every field the DC forwards to Sentry —
    ``__mcd_error__``, ``__mcd_exception__`` and ``__mcd_stack_trace__`` — while the
    error stays actionable enough to tell Kerberos failures apart.
    """

    _REALM = "MCLAB.INTERNAL"
    _KDC = "labdc.mclab.internal"
    _HOST = "labsql.mclab.internal"
    _PRINCIPAL = "svc-montecarlo@MCLAB.INTERNAL"
    _KEYTAB_B64 = "BQIAAABTAAIADU1DTEFCLklOVEVSTkFM"
    _PASSWORD = "S3cr3tKerberosPw"

    _OPERATION = {
        "trace_id": "kerberos-safety-test",
        "skip_cache": True,
        "commands": [
            {"method": "cursor", "store": "_cursor"},
        ],
    }

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._saved_env = {
            key: os.environ.get(key)
            for key in ("KRB5_CONFIG", "KRB5_CLIENT_KTNAME", "KRB5CCNAME")
        }

    def tearDown(self) -> None:
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _keytab_credentials(self, **overrides) -> dict:
        creds = {
            "auth_type": "kerberos",
            "host": self._HOST,
            "port": 1433,
            "database": "mcdemo",
            "realm": self._REALM,
            "kdc": self._KDC,
            "principal": self._PRINCIPAL,
            "keytab_base64": self._KEYTAB_B64,
        }
        creds.update(overrides)
        return {k: v for k, v in creds.items() if v is not None}

    def _execute(self, connect_args: dict):
        return self._agent.execute_operation(
            "sql-server", "run_query", self._OPERATION, {"connect_args": connect_args}
        )

    def _assert_no_credential_leak(self, response) -> None:
        """Serialize the whole result: error, exception AND stack_trace reach Sentry."""
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._KEYTAB_B64, serialized, "keytab leaked in response")
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")

    # ── CTP validation failures ───────────────────────────────────────

    @patch("pyodbc.connect")
    def test_missing_realm_is_actionable_and_leaks_nothing(self, mock_connect):
        response = self._execute(self._keytab_credentials(realm=None))

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("realm", error.lower())
        self._assert_no_credential_leak(response)
        mock_connect.assert_not_called()

    @patch("pyodbc.connect")
    def test_keytab_and_password_together_is_actionable_and_leaks_nothing(
        self, mock_connect
    ):
        response = self._execute(self._keytab_credentials(password=self._PASSWORD))

        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("keytab", error.lower())
        self._assert_no_credential_leak(response)
        mock_connect.assert_not_called()

    @patch("pyodbc.connect")
    def test_malformed_keytab_names_the_field_without_echoing_it(self, mock_connect):
        """The natural implementation puts the offending value in the message."""
        response = self._execute(
            self._keytab_credentials(keytab_base64="not!valid!base64!")
        )

        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("keytab", error.lower())
        self.assertNotIn("not!valid!base64!", json.dumps(response.result, default=str))
        mock_connect.assert_not_called()

    # ── Driver-level failures ─────────────────────────────────────────

    @patch("pyodbc.connect")
    def test_connection_string_echo_leaks_nothing_and_stays_actionable(
        self, mock_connect
    ):
        """Worst case: the driver echoes the DSN back in its exception.

        On this path the DSN carries Trusted_Connection rather than a password, so the
        assertion that matters is that the *keytab* -- which never belongs in a
        connection string -- has not found its way in either.
        """

        def _raise_echoing_connection_string(connection_string, *args, **kwargs):
            raise pyodbc.OperationalError(
                "08001",
                f"[08001] Login timeout expired; connection string: {connection_string}",
            )

        mock_connect.side_effect = _raise_echoing_connection_string

        response = self._execute(self._keytab_credentials())

        self._assert_no_credential_leak(response)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn(self._HOST, error)
        self.assertIn("Login timeout expired", error)

    @patch("pyodbc.connect")
    def test_sspi_failure_is_distinguishable(self, mock_connect):
        """'Cannot generate SSPI context' means no valid TGT or a bad keytab.

        Support has to be able to tell this apart from an SPN mismatch and from an
        unauthorized login, so the driver text must survive into the error.
        """
        mock_connect.side_effect = pyodbc.Error(
            "HY000", "[HY000] SSPI Provider: No credentials were supplied"
        )

        response = self._execute(self._keytab_credentials())

        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("SSPI", error)
        self._assert_no_credential_leak(response)

    @patch("pyodbc.connect")
    def test_login_unauthorized_is_distinguishable(self, mock_connect):
        """Kerberos succeeded but SQL Server has no Windows login for the principal."""
        mock_connect.side_effect = pyodbc.ProgrammingError(
            "42000",
            f"[42000] Login failed for user '{self._PRINCIPAL}'.",
        )

        response = self._execute(self._keytab_credentials())

        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("Login failed", error)
        self._assert_no_credential_leak(response)

    @patch.object(
        sql_server_kerberos_env.subprocess, "run", return_value=Mock(returncode=0)
    )
    @patch("pyodbc.connect")
    def test_password_form_password_never_reaches_the_response(
        self, mock_connect, _mock_subprocess
    ):
        """The password is for kinit against the KDC; it must not appear anywhere.

        subprocess is stubbed so kinit succeeds. Without that the CTP's TGT acquisition
        fails first (no reachable KDC in a unit test) and pyodbc.connect is never reached
        -- the assertion would still pass while testing nothing about the driver path, so
        mock_connect.assert_called_once() holds it honest.
        """

        def _raise_echoing_connection_string(connection_string, *args, **kwargs):
            raise pyodbc.OperationalError(
                "08001", f"[08001] failed; connection string: {connection_string}"
            )

        mock_connect.side_effect = _raise_echoing_connection_string

        response = self._execute(
            self._keytab_credentials(keytab_base64=None, password=self._PASSWORD)
        )

        mock_connect.assert_called_once()
        self._assert_no_credential_leak(response)

    def test_password_survives_in_connect_args_but_not_through_the_redactor(self):
        """The password form now carries its secret in the resolved connect_args.

        Before the environment moved to the proxy client, the transform consumed the
        password to run kinit and the mapper emitted no credential at all -- the resolved
        structure was inherently clean. It is not any more: kinit runs at connect time, so
        the password rides in connect_args["kerberos"] until then.

        That makes the redactor load-bearing rather than incidental. It only holds because
        redact_attributes recurses into nested dicts and "pass" is in its attribute list;
        renaming the key or flattening the recursion would silently expose the secret.
        """
        temp_files: list[str] = []
        resolved = CtpRegistry.resolve(
            "sql-server",
            {
                "auth_type": "kerberos",
                "host": "labsql.mclab.internal",
                "port": 1433,
                "realm": "MCLAB.INTERNAL",
                "kdc": "labdc.mclab.internal",
                "principal": "svc-mc@MCLAB.INTERNAL",
                "password": self._PASSWORD,
            },
            temp_files=temp_files,
        )
        for path in temp_files:
            with suppress(OSError):
                os.unlink(path)

        # Documents the exposure rather than asserting it is absent -- the design needs it.
        self.assertEqual(
            self._PASSWORD, resolved["connect_args"]["kerberos"]["password"]
        )
        # ...and pins the control that keeps it out of logs and responses.
        redacted = json.dumps(
            AgentRedactUtilities.standard_redact(resolved), default=str
        )
        self.assertNotIn(self._PASSWORD, redacted)

    # ── Log safety (Lambda / Datadog path) ────────────────────────────

    @patch("pyodbc.connect")
    def test_credentials_absent_from_formatted_log_records(self, mock_connect):
        """JsonLogFormatter is what the Lambda handler emits to Datadog."""
        records: list[logging.LogRecord] = []

        class _ListHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = _ListHandler()
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            mock_connect.side_effect = pyodbc.OperationalError(
                "08001", "[08001] Login timeout expired"
            )
            self._execute(self._keytab_credentials())
        finally:
            root.removeHandler(handler)

        formatter = JsonLogFormatter()
        for record in records:
            output = formatter.format(record)
            self.assertNotIn(self._KEYTAB_B64, output, "keytab leaked to logs")
            self.assertNotIn(self._PASSWORD, output, "password leaked to logs")


class SqlServerKerberosErrorTaxonomyTests(TestCase):
    """Kerberos fails in three ways that need different fixes, and the driver's own
    messages do not say which — PRO-3016 / SDD §5.4.

    Support cannot act on "cannot generate SSPI context". The remediation for a missing
    ticket (keytab/clock/KDC reachability) is nothing like the remediation for an SPN
    mismatch (setspn on the server, connect by FQDN) or for an unauthorized login
    (CREATE LOGIN ... FROM WINDOWS). So the client classifies the failure and appends a
    hint, while preserving the driver's original text.

    Hints are added only on the Kerberos path: a SQL-Authentication login failure must
    not be answered with Kerberos advice.
    """

    _HOST = "labsql.mclab.internal"
    _PRINCIPAL = "svc-montecarlo@MCLAB.INTERNAL"
    _KEYTAB_B64 = "BQIAAABTAAIADU1DTEFCLklOVEVSTkFM"
    # Asserted explicitly: several driver messages already contain words like "keytab"
    # or "clock", so without a marker these tests would pass on the echoed driver text
    # alone and prove nothing about the diagnosis.
    _MARKER = "Windows authentication diagnosis:"

    _OPERATION = {
        "trace_id": "kerberos-taxonomy-test",
        "skip_cache": True,
        "commands": [{"method": "cursor", "store": "_cursor"}],
    }

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._saved_env = {
            key: os.environ.get(key)
            for key in ("KRB5_CONFIG", "KRB5_CLIENT_KTNAME", "KRB5CCNAME")
        }

    def tearDown(self) -> None:
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _kerberos_credentials(self) -> dict:
        return {
            "auth_type": "kerberos",
            "host": self._HOST,
            "port": 1433,
            "database": "mcdemo",
            "realm": "MCLAB.INTERNAL",
            "kdc": "labdc.mclab.internal",
            "principal": self._PRINCIPAL,
            "keytab_base64": self._KEYTAB_B64,
        }

    def _error_for(self, driver_exception: Exception, connect_args=None) -> str:
        with patch("pyodbc.connect") as mock_connect:
            mock_connect.side_effect = driver_exception
            response = self._agent.execute_operation(
                "sql-server",
                "run_query",
                self._OPERATION,
                {"connect_args": connect_args or self._kerberos_credentials()},
            )
        return response.result.get(ATTRIBUTE_NAME_ERROR, "")

    # ── 1. No usable ticket ───────────────────────────────────────────

    def test_no_credentials_supplied_is_diagnosed_as_a_ticket_problem(self):
        error = self._error_for(
            pyodbc.Error("HY000", "[HY000] SSPI Provider: No credentials were supplied")
        )
        self.assertIn("No credentials were supplied", error)  # driver text preserved
        self.assertIn(self._MARKER, error)
        self.assertIn("keytab", error.lower())  # points at the credential

    def test_keytab_with_no_suitable_keys_is_diagnosed(self):
        error = self._error_for(
            pyodbc.Error(
                "HY000", "[HY000] Keytab contains no suitable keys for host/x@REALM"
            )
        )
        self.assertIn(self._MARKER, error)
        self.assertIn("key version", error.lower())

    def test_clock_skew_is_called_out_explicitly(self):
        """Kerberos requires client and KDC within ~5 minutes; nothing else hints at it."""
        error = self._error_for(pyodbc.Error("HY000", "[HY000] Clock skew too great"))
        self.assertIn(self._MARKER, error)
        self.assertIn("5 minutes", error)

    # ── 2. SPN / DNS mismatch ─────────────────────────────────────────

    def test_sspi_context_failure_mentions_the_spn(self):
        error = self._error_for(
            pyodbc.Error(
                "HY000", "[HY000] Cannot generate SSPI context (SQLDriverConnect)"
            )
        )
        self.assertIn("Cannot generate SSPI context", error)
        self.assertIn("SPN", error)

    def test_server_not_found_in_kerberos_database_mentions_the_spn(self):
        error = self._error_for(
            pyodbc.Error("HY000", "[HY000] Server not found in Kerberos database")
        )
        self.assertIn("SPN", error)

    def test_spn_derivation_degrades_rather_than_crashing(self):
        """Review F7 on #379: only the happy path was exercised.

        These branches exist precisely for the malformed-SERVER case, which is when a
        support engineer most needs a hint rather than a traceback from inside our own
        error handling.
        """
        cases = [
            # (SERVER value, expected SPN)
            ("tcp:sql.corp.example.com,1433", "MSSQLSvc/sql.corp.example.com:1433"),
            # No tcp: prefix -- still recoverable.
            ("sql.corp.example.com,1433", "MSSQLSvc/sql.corp.example.com:1433"),
            # No port -- the driver defaults to 1433 and so does the hint.
            ("tcp:sql.corp.example.com", "MSSQLSvc/sql.corp.example.com:1433"),
            # Nothing usable: name the service class only.
            (None, "MSSQLSvc/<host>:<port>"),
            ("", "MSSQLSvc/<host>:<port>"),
        ]
        for server, expected in cases:
            with self.subTest(server=server):
                self.assertEqual(expected, SqlServerProxyClient._expected_spn(server))

    def test_spn_hint_names_the_expected_spn_so_it_can_be_compared(self):
        """The actionable step is comparing what we asked for against setspn -L."""
        error = self._error_for(
            pyodbc.Error("HY000", "[HY000] Cannot generate SSPI context")
        )
        self.assertIn(f"MSSQLSvc/{self._HOST}:1433", error)

    # ── 3. Login not authorized ───────────────────────────────────────

    def test_login_failed_is_diagnosed_as_authorization_not_authentication(self):
        error = self._error_for(
            pyodbc.ProgrammingError(
                "42000", f"[42000] Login failed for user '{self._PRINCIPAL}'."
            )
        )
        self.assertIn("Login failed", error)
        self.assertIn(self._MARKER, error)
        self.assertIn("CREATE LOGIN", error)

    # ── Scoping and hygiene ───────────────────────────────────────────

    def test_sql_auth_failures_get_no_kerberos_advice(self):
        """A username/password login failure must not be answered with SPN hints."""
        error = self._error_for(
            pyodbc.ProgrammingError("42000", "[42000] Login failed for user 'alice'."),
            connect_args={
                "host": "db.example.com",
                "port": 1433,
                "user": "alice",
                "password": "pw",
            },
        )
        self.assertIn("Login failed", error)
        self.assertNotIn(self._MARKER, error)
        self.assertNotIn("SPN", error)

    def test_unrecognised_driver_error_is_passed_through_unchanged(self):
        """No speculative diagnosis when we do not recognise the failure."""
        error = self._error_for(
            pyodbc.OperationalError("08001", "[08001] Login timeout expired")
        )
        self.assertIn("Login timeout expired", error)
        self.assertNotIn(self._MARKER, error)

    def test_diagnosis_never_includes_the_credential(self):
        with patch("pyodbc.connect") as mock_connect:
            mock_connect.side_effect = pyodbc.Error(
                "HY000", "[HY000] Cannot generate SSPI context"
            )
            response = self._agent.execute_operation(
                "sql-server",
                "run_query",
                self._OPERATION,
                {"connect_args": self._kerberos_credentials()},
            )
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._KEYTAB_B64, serialized)

    def test_undonstructable_exception_type_does_not_mask_the_original(self):
        """Re-raising as type(error)(str) assumes a single-string constructor.

        A driver exception subclass that requires extra arguments would raise TypeError
        during our own error handling, replacing a real connection failure with a
        confusing one. The original must survive instead.
        """

        class _PickyError(pyodbc.Error):
            def __init__(self, required_arg, another):  # noqa: D107
                super().__init__(f"{required_arg}/{another}")
                self.required_arg = required_arg

        error = self._error_for(_PickyError("Cannot generate SSPI context", "extra"))
        self.assertIn("Cannot generate SSPI context", error)

    def test_failed_connection_still_deletes_the_keytab(self):
        """A failed connect must not leave the keytab on disk.

        The keytab is materialised by the CTP pipeline before the client exists, so
        nothing on the client can clean it up if construction fails. The factory unlinks
        the pipeline's temp files in an `except BaseException` — this asserts the
        enriched re-raise from the error taxonomy does not bypass that.
        """
        # Captured during the connect, not after: the environment is scoped to the
        # connect and restored on the way out, so KRB5_CLIENT_KTNAME is gone by the time
        # execute_operation returns. Reading it here also proves it was set when the
        # driver ran.
        observed: dict[str, Optional[str]] = {}

        def _fail_and_capture(*args, **kwargs):
            observed["keytab"] = os.environ.get("KRB5_CLIENT_KTNAME")
            raise pyodbc.Error("HY000", "[HY000] Cannot generate SSPI context")

        with patch("pyodbc.connect") as mock_connect:
            mock_connect.side_effect = _fail_and_capture
            self._agent.execute_operation(
                "sql-server",
                "run_query",
                self._OPERATION,
                {"connect_args": self._kerberos_credentials()},
            )

        keytab_path = observed.get("keytab")
        self.assertIsNotNone(keytab_path, "the pipeline should have written a keytab")
        self.assertNotIn(
            "KRB5_CLIENT_KTNAME",
            os.environ,
            "the connection must not leave KRB5_* set for other integrations",
        )
        self.assertFalse(
            os.path.exists(keytab_path),
            f"keytab survived a failed connection: {keytab_path}",
        )
