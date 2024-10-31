from typing import (
    Any,
    Dict,
    Optional,
)

from impala import dbapi
from impala.hiveserver2 import HiveServer2Connection

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class HiveProxyConnection(HiveServer2Connection):
    def cursor(self):
        # If close_finished_queries is true, impala will close every query once a DDL/DML query execution is finished
        # or all rows are fetched. It will also call GetLog() before closing the query to get query metadata from Hive.
        # GetLog() is not available for spark databricks causing this to break.
        #
        # Setting close_finished_queries to false  will only close queries when execute() is called again
        # or the cursor is closed. GetLog() is not automatically called so spark databricks works.
        # With False the cursor will not have a rowcount for DML statements, this is fine for MC.
        # https://github.com/cloudera/impyla/blob/e4c76169f7e5765c09b11c92fceb862dbb9b72be/impala/hiveserver2.py#L122
        return super().cursor(self, close_finished_queries=False)


class HiveProxyClient(BaseDbProxyClient):
    """
    Proxy client for Hive. Credentials are expected to be supplied under "connect_args" and
    will be passed directly to `hive.Connection`, so only attributes supported as parameters by
    `hive.Connection` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):  # noqa
        super().__init__(connection_type="hive")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Hive agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        self._connection = HiveProxyConnection(
            dbapi.connect(**credentials[_ATTR_CONNECT_ARGS])
        )

    @property
    def wrapped_client(self):
        return self._connection
