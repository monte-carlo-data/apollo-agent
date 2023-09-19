import os
from typing import Dict

from databricks import sql

from apollo.integrations.base_proxy_client import BaseProxyClient


class DatabricksSqlWarehouseProxyClient(BaseProxyClient):
    def __init__(self, credentials: Dict, **kwargs):
        # used for testing, we configure "INVALID_host_name" in the connection
        # so we're sure there's no connection established from the data collector
        # and it connects only through the agent
        credentials["connect_args"]["server_hostname"] = credentials["connect_args"][
            "server_hostname"
        ].replace("INVALID_", "")

        self._connection = sql.connect(**credentials["connect_args"])

    @property
    def wrapped_client(self):
        return self._connection
