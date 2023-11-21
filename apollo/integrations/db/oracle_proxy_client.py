from typing import (
    Any,
    Dict,
    Optional,
)

import oracledb

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class OracleProxyClient(BaseDbProxyClient):
    """
    Proxy client for Oracle DB Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `oracledb.connect`, so only attributes supported as parameters
    by `oracledb.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Oracle DB agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = oracledb.connect(**credentials[_ATTR_CONNECT_ARGS])  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection
