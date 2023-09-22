import os
from typing import Dict

from databricks import sql

from apollo.integrations.base_proxy_client import BaseProxyClient


class DatabricksSqlWarehouseProxyClient(BaseProxyClient):
    def __init__(self, credentials: Dict, **kwargs):
        self._connection = sql.connect(**credentials["connect_args"])

    @property
    def wrapped_client(self):
        return self._connection
