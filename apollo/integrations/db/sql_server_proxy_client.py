from typing import (
    Any,
    Optional,
)

import pyodbc

from apollo.integrations.db.tsql_base_db_proxy_client import (
    TSqlBaseDbProxyClient,
    odbc_string_from_dict,
)

_ATTR_CONNECT_ARGS = "connect_args"
_ATTR_TRUSTED_CONNECTION = "Trusted_Connection"

# Kerberos fails in three ways that need completely different fixes, and the driver's
# own message does not say which. Nobody can act on "cannot generate SSPI context", so
# classify the failure and append a hint — while preserving the driver's text verbatim,
# because that is what matches search results and vendor docs.
#
# Applied ONLY on the Kerberos path: a SQL-Authentication login failure answered with
# SPN advice is worse than no advice.
_KERBEROS_DIAGNOSIS_MARKER = "Windows authentication diagnosis:"

# 1. No usable ticket. The agent never obtained a TGT, or the keytab cannot produce one.
_NO_TICKET_SIGNATURES = (
    "no credentials were supplied",
    "no credentials cache",
    "credentials cache",
    "keytab contains no suitable keys",
    "key table entry not found",
    "client not found in kerberos database",
    "preauthentication failed",
    "clock skew too great",
    "cannot find kdc",
    "cannot contact any kdc",
)

# 2. The ticket request reached the KDC but named a service principal AD does not hold.
#    Not overridable in code: the driver derives the SPN and rejects ServerSPN.
_SPN_SIGNATURES = (
    "cannot generate sspi context",
    "server not found in kerberos database",
    "wrong principal in request",
)

# 3. Kerberos succeeded; SQL Server has no Windows login for the principal.
_UNAUTHORIZED_SIGNATURES = ("login failed for user",)


def _kerberos_diagnosis(message: str, spn: str) -> Optional[str]:
    """Return an actionable hint for a Kerberos connection failure, or None.

    None means "not a failure mode we recognise" — deliberately preferred over a
    speculative diagnosis, which would send support down the wrong path.
    """
    lowered = message.lower()

    if any(signature in lowered for signature in _SPN_SIGNATURES):
        # Listed first because "cannot generate SSPI context" is the most common
        # symptom and is genuinely ambiguous — it can also mean no ticket. The hint
        # says so rather than asserting a single cause.
        return (
            f"{_KERBEROS_DIAGNOSIS_MARKER} the driver requested SPN '{spn}'. Kerberos "
            "derives this from the host and port in the connection string and does not "
            "support overriding it, so it must match an SPN registered in Active "
            "Directory. Compare it against 'setspn -L <account>' on the SQL Server "
            "host, and connect by fully-qualified domain name rather than an IP or an "
            "unregistered alias. If the SPN is correct, the other cause of this error "
            "is having no valid ticket — check 'klist'."
        )

    if any(signature in lowered for signature in _NO_TICKET_SIGNATURES):
        return (
            f"{_KERBEROS_DIAGNOSIS_MARKER} no usable Kerberos ticket. Check that the "
            "keytab matches the configured principal and its current key version "
            "number (a password reset in Active Directory invalidates an older "
            "keytab), that the KDC is reachable on port 88, and that this host's clock "
            "is within 5 minutes of the KDC."
        )

    if any(signature in lowered for signature in _UNAUTHORIZED_SIGNATURES):
        return (
            f"{_KERBEROS_DIAGNOSIS_MARKER} Kerberos authentication succeeded but SQL "
            "Server did not authorize the principal — this is a permissions problem, "
            "not a credentials one. Create a Windows login for it: "
            "CREATE LOGIN [DOMAIN\\account] FROM WINDOWS. Note the login must use the "
            "DOMAIN\\account form; a user principal name is rejected."
        )

    return None


class SqlServerProxyClient(TSqlBaseDbProxyClient):
    """
    Proxy client for SQL Server Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `pyodbc.connect`. 'pyodbc' accepts a connection string contained the connection details,
    the expectation from the DC is that _ATTR_CONNECT_ARGS will be a string.
    """

    _DEFAULT_LOGIN_TIMEOUT_IN_SECONDS = 15
    _DEFAULT_QUERY_TIMEOUT_IN_SECONDS = 60 * 14  # 14 minutes

    def __init__(self, credentials: Optional[dict], **kwargs: Any):
        super().__init__(connection_type="sql-server")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"SQL Server agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        connect_args = credentials[_ATTR_CONNECT_ARGS]
        kerberos_spn: Optional[str] = None
        if isinstance(connect_args, dict):
            # CTP path: timeout fields land in connect_args; pop before building ODBC string
            connect_args = dict(connect_args)
            login_timeout = connect_args.pop(
                "login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS
            )
            query_timeout = connect_args.pop(
                "query_timeout_in_seconds", self._DEFAULT_QUERY_TIMEOUT_IN_SECONDS
            )
            if connect_args.get(_ATTR_TRUSTED_CONNECTION):
                # Reconstruct the SPN the driver will derive, so a failure can be
                # compared against what AD actually holds. SERVER is "tcp:{host},{port}".
                kerberos_spn = self._expected_spn(connect_args.get("SERVER"))
            connection_string = odbc_string_from_dict(connect_args)
        else:
            # Legacy path: pre-built ODBC string; timeouts at top-level credentials
            login_timeout = credentials.get(
                "login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS
            )
            query_timeout = credentials.get(
                "query_timeout_in_seconds", self._DEFAULT_QUERY_TIMEOUT_IN_SECONDS
            )
            connection_string = connect_args
        try:
            self._connection = pyodbc.connect(
                connection_string,
                # Set timeout for establishing connection to db
                timeout=login_timeout,
            )  # type: ignore
        except Exception as error:
            # get_error_type() is the usual place to classify, but it is only consulted
            # when a client instance exists — and this failure happens during
            # construction, so the diagnosis has to travel in the exception itself.
            if kerberos_spn is None:
                raise
            diagnosis = _kerberos_diagnosis(str(error), kerberos_spn)
            if diagnosis is None:
                raise
            # Chain rather than replace: the driver's own text is what matches vendor
            # docs and search results, and the original traceback stays intact.
            #
            # Preserving the exception type matters (callers may branch on it), but
            # type(error)(str) assumes a single-string constructor. A subclass that
            # takes more arguments would raise TypeError from inside our own error
            # handling and bury the real failure, so fall back to the original.
            try:
                enriched = type(error)(f"{error} — {diagnosis}")
            except Exception:
                raise error from None
            raise enriched from error

        # Add output converter to handle datetimeoffset data types that are not supported by pyodbc
        self._connection.add_output_converter(
            self._DATETIMEOFFSET_SQL_TYPE_CODE, self._handle_datetimeoffset
        )

        # Set timeout for any query executed through this connection
        self._connection.timeout = query_timeout

    @staticmethod
    def _expected_spn(server: Optional[str]) -> str:
        """Rebuild the SPN the ODBC driver derives from SERVER="tcp:{host},{port}".

        Best-effort: if SERVER is not in the expected shape the hint degrades to naming
        the service class only, which is still more use than nothing.
        """
        if not server:
            return "MSSQLSvc/<host>:<port>"
        value = str(server)
        if value.startswith("tcp:"):
            value = value[len("tcp:") :]
        host, _, port = value.partition(",")
        return f"MSSQLSvc/{host}:{port or 1433}"

    @property
    def wrapped_client(self):
        return self._connection
