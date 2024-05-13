from typing import Optional, Dict, Any

from hdbcli import dbapi

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class SAPHanaProxyClient(BaseDbProxyClient):
    """
    Proxy client for SAP Hana. Credentials are expected to be supplied under "connect_args" and
    will be passed directly to `dbapi.connect`, so only attributes supported as parameters by
    `dbapi.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="sap-hana")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"SAP HANA database agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = dbapi.connect(**credentials[_ATTR_CONNECT_ARGS])  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection
