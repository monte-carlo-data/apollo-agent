from typing import (
    Any,
    Dict,
    List,
    Optional,
)

import duckdb

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class MotherDuckProxyClient(BaseDbProxyClient):
    """
    Proxy client for Motherduck client. Credentials are expected to be supplied under "connect_args" and will
    be passed directly to `duckdb.connect`. 'duckdb` accepts a connection string with the connection details,
    so "connect_args" will be a string.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Motherduck agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = duckdb.connect(credentials[_ATTR_CONNECT_ARGS])  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection
