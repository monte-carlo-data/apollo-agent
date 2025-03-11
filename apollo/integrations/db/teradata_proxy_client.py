from typing import (
    Any,
    Dict,
    List,
    Optional,
)

import teradatasql

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient, SslOptions

_ATTR_CONNECT_ARGS = "connect_args"


class TeradataProxyClient(BaseDbProxyClient):
    """
    Proxy client for Teradata Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `teradatasql.connect`, so only attributes supported as parameters
    by `teradatasql.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="teradata")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Teradata agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = credentials[_ATTR_CONNECT_ARGS]
        ssl_options = SslOptions(**(credentials.get("ssl_options") or {}))

        if ssl_options.ca_data and not ssl_options.disabled:
            # Purposely a quoted boolean per teradatasql documentation
            connect_args["encryptdata"] = "true"

            # Path to PEM file that contains Certificate Authority (CA) certificates
            connect_args["sslca"] = ssl_options.write_ca_data_to_temp_file(
                "/tmp/teradata_ca.pem", upsert=True
            )

            # Teradatasql has 2 port connection parameters depending on
            # https vs http connections. If we are making an encrypted connection,
            # assume that credentials.port is the HTTPS port, typically 443.
            connect_args["https_port"] = connect_args.pop("dbs_port")

        self._connection = teradatasql.connect(**connect_args)  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection

    @classmethod
    def _process_description(cls, col: List) -> List:
        # Teradata cursor returns the column type as <class 'str'> instead of a type_code which
        # we expect. Here we are converting this type to a string of the type so the description
        # can be serialized. So <class 'str'> will become just 'str'
        return [col[0], col[1].__name__, col[2], col[3], col[4], col[5], col[6]]
