from typing import Optional, Dict, Any, List

import pyodbc

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class AzureDedicatedSqlPoolProxyClient(BaseDbProxyClient):
    """
    Proxy client for Azure Dedicated SQL Pool Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `pyodbc.connect`. 'pyodbc` accepts a connection string with the connection details,
    so "connect_args" will be a string.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Azure Dedicated SQL Pool agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = pyodbc.connect(credentials[_ATTR_CONNECT_ARGS])  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection

    @classmethod
    def _process_description(cls, col: List) -> List:
        # pyodbc cursor returns the column type as <class 'str'> instead of a type_code which
        # we expect. Here we are converting this type to a string of the type so the description
        # can be serialized. So <class 'str'> will become just 'str'
        return [col[0], col[1].__name__, col[2], col[3], col[4], col[5], col[6]]
