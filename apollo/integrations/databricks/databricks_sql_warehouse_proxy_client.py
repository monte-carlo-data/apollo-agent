import os
from typing import Dict

from databricks import sql

from apollo.integrations.base_proxy_client import BaseProxyClient


class DatabricksSqlWarehouseProxyClient(BaseProxyClient):
    """
    Proxy client for Databricks SQL Warehouse Client.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `sql.connect`, so
    only attributes supported as parameters by `sql.connect` should be passed.
    """

    def __init__(self, credentials: Dict, **kwargs):
        self._connection = sql.connect(**credentials["connect_args"])

    @property
    def wrapped_client(self):
        return self._connection
