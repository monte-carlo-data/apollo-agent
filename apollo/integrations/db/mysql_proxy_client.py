from typing import (
    Any,
    Dict,
    Optional,
)

import pymysql

from apollo.integrations.base_proxy_client import BaseProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class MysqlProxyClient(BaseProxyClient):
    """
    Proxy client for MySQL Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `pymysql.connect`, so only attributes supported as parameters
    by `pymysql.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Mysql agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = pymysql.connect(**credentials[_ATTR_CONNECT_ARGS])

    @property
    def wrapped_client(self):
        return self._connection
