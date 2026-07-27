import json
from typing import Dict, List, Optional

import requests
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError

from apollo.common.agent.models import AgentExecuteSqlQueryResponse
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient
from apollo.integrations.http.relative_path import validate_relative_rest_path

_ATTR_CONNECT_ARGS = "connect_args"


class SnowflakeProxyClient(BaseDbProxyClient):
    """
    Proxy client for Snowflake.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `psycopg2.connect`, so
    only attributes supported as parameters by `snowflake.connector.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        super().__init__(connection_type="snowflake")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Snowflake agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        self._connection = snowflake.connector.connect(
            **credentials[_ATTR_CONNECT_ARGS],
        )

    @property
    def wrapped_client(self):
        return self._connection

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Convert SF errors to error types that can be converted back to SF errors client side.
        """
        if isinstance(error, ProgrammingError):
            return "ProgrammingError"
        elif isinstance(error, DatabaseError):
            return "DatabaseError"
        return super().get_error_type(error)

    def get_error_extra_attributes(self, error: Exception) -> Optional[Dict]:
        """
        Return a dictionary with `errno` and `sqlstate` for SF Errors.
        """
        if isinstance(error, DatabaseError):  # ProgrammingError extends DatabaseError
            return {
                "errno": error.errno,
                "sqlstate": error.sqlstate,
            }
        return super().get_error_extra_attributes(error)

    def execute_sql_query(
        self, sql_query: str, max_results: int, query_timeout: int
    ) -> AgentExecuteSqlQueryResponse:
        """
        Execute a SQL query synchronously and collect results.
        """
        with self._connection.cursor() as cursor:
            cursor.execute(sql_query, timeout=query_timeout)
            results = cursor.fetchmany(max_results + 1)
            if len(results) > max_results:
                is_partial = True
                results = results[:-1]
            else:
                is_partial = False

            description = cursor.description or []

            return AgentExecuteSqlQueryResponse(
                columns=[field[0] for field in description],
                rows=results,
                is_partial=is_partial,
            )

    def execute_rest_request(
        self,
        method: str,
        path: str,
        body: Optional[Dict] = None,
        timeout: Optional[int] = None,
    ) -> Dict:
        """Execute one authenticated REST request against this connection's own
        Snowflake account, reusing the live connector session's token.

        Returns ``{"status_code": int, "response": <JSON-parsed body, or the
        raw body text when it isn't valid JSON>}`` on a 2xx response, or
        ``{"status_code": int, "error": <body text truncated to 10,000
        chars>}`` otherwise. The session token is scrubbed from every
        returned body (it is sent only in the request's ``Authorization``
        header) — the non-2xx body is capped at 10,000 chars since it's
        error/diagnostic text, but the 2xx body is not, since the primary
        production payload is a Cortex SSE stream returned as plain text
        that can legitimately exceed that size.

        ``path`` must be relative (begin with ``/``, no scheme or host) so
        the session token is only ever sent to the connection's own
        Snowflake host. Mirrors ``SalesforceDataCloudProxyClient.ssot_get``.
        """
        validate_relative_rest_path(path)

        rest = getattr(self._connection, "rest", None)
        token = getattr(rest, "token", None)
        server_url = getattr(rest, "server_url", None)
        if not token or not server_url:
            raise ValueError(
                "execute_rest_request: no active Snowflake session token; the "
                "connection may use an auth mode without a session token"
            )

        response = requests.request(
            method,
            f"{server_url}{path}",
            json=body,
            headers={"Authorization": f'Snowflake Token="{token}"'},
            timeout=timeout,
        )

        # The token lives only in the request header, never the response
        # body; redact defensively anyway, uniformly, before branching on
        # status code.
        text = response.text.replace(token, "***")

        if response.status_code // 100 != 2:
            return {"status_code": response.status_code, "error": text[:10_000]}

        try:
            payload = json.loads(text)
        except ValueError:
            payload = text
        return {"status_code": response.status_code, "response": payload}
