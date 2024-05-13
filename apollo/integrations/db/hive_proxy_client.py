import time
from base64 import standard_b64encode
from typing import (
    Any,
    Dict,
    Optional,
)

from pyhive import hive
from thrift.transport import THttpClient
from TCLIService.ttypes import TOperationState

from apollo.agent.models import AgentError
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class HiveProxyCursor(hive.Cursor):
    def async_execute(self, query: str, timeout: int, **kwargs: Any) -> None:  # noqa
        start_time = time.time()

        self.execute(query, async_=True)

        pending_states = (
            TOperationState.INITIALIZED_STATE,
            TOperationState.PENDING_STATE,
            TOperationState.RUNNING_STATE,
        )
        time_passed = 0
        while self.poll().operationState in pending_states:
            time_passed = time.time() - start_time
            if time_passed > timeout:
                self.cancel()
                break
            time.sleep(10)

        resp = self.poll()
        if resp.operationState == TOperationState.ERROR_STATE:
            msg = "Query failed, see cluster logs for details"
            if time_passed > 0:
                msg += f" (runtime: {time_passed}s)"
            raise AgentError(msg, query, resp)
        elif resp.operationState == TOperationState.CANCELED_STATE:
            raise AgentError(f"Time out executing query: {time_passed}s", query, resp)


class HiveProxyConnection(hive.Connection):
    def cursor(self, *args: Any, **kwargs: Any):
        return HiveProxyCursor(self, *args, **kwargs)


class HiveProxyClient(BaseDbProxyClient):
    """
    Proxy client for Hive. Credentials are expected to be supplied under "connect_args" and
    will be passed directly to `hive.Connection`, so only attributes supported as parameters by
    `hive.Connection` should be passed. If "mode" is not set to "binary", then the "connect_args"
    will be used to create a new thrift transport that will be passed to `hive.Connection`.
    """

    _MODE_BINARY = "binary"

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):  # noqa
        super().__init__(connection_type="hive")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Hive agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        if credentials.get("mode") != self._MODE_BINARY:
            self._connection = self._create_http_connection(
                **credentials[_ATTR_CONNECT_ARGS]
            )
        else:
            self._connection = HiveProxyConnection(**credentials[_ATTR_CONNECT_ARGS])

    @classmethod
    def _create_http_connection(
        cls,
        url: str,
        username: str,
        password: str,
        user_agent: Optional[str] = None,
        **kwargs: Any,  # noqa
    ) -> hive.Connection:
        transport = THttpClient.THttpClient(url)

        auth = standard_b64encode(f"{username}:{password}".encode()).decode()
        headers = dict(Authorization=f"Basic {auth}")
        if user_agent:
            headers["User-Agent"] = user_agent

        transport.setCustomHeaders(headers)

        try:
            return HiveProxyConnection(thrift_transport=transport)
        except EOFError or MemoryError:
            raise AgentError("Error creating connection - credentials might be invalid")

    @property
    def wrapped_client(self):
        return self._connection
