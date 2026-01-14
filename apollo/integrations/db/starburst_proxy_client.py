import hashlib
import logging
from typing import Optional, Dict, Any

import trino

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient, SslOptions

_ATTR_CONNECT_ARGS = "connect_args"

logger = logging.getLogger(__name__)


class StarburstProxyClient(BaseDbProxyClient):
    """
    Proxy client for Starburst "database" Client which is used by Starburst connections. Credentials
    are expected to be supplied under "connect_args" and will be passed directly to
    `trino.dbapi.connect`.
    """

    def __init__(self, credentials: Optional[Dict], platform: str, **kwargs: Any):
        super().__init__(connection_type="starburst")
        self._platform = platform
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Starburst agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args: Dict[str, Any] = {**credentials[_ATTR_CONNECT_ARGS]}

        # Handle SSL options for Starburst connections
        ssl_options = SslOptions(**(connect_args.pop("ssl_options", {}), or {}))

        if ssl_options.ca_data and not ssl_options.disabled:
            # Trino requires verify to point to a certificate file
            # Create a temporary file for the CA certificate
            host_hash = hashlib.sha256(
                connect_args.get("host", "temp").encode()
            ).hexdigest()[:12]
            cert_file = f"/tmp/{host_hash}_starburst_ca.pem"
            ssl_options.write_ca_data_to_temp_file(cert_file, upsert=True)

            connect_args["verify"] = cert_file

            logger.info("Starburst SSL configured")

        if ssl_options.disabled:
            connect_args["verify"] = False

        # Extract user/password for BasicAuthentication
        if "user" not in connect_args or "password" not in connect_args:
            raise ValueError(
                "Starburst agent client requires 'user' and 'password' in connect_args"
            )
        user = connect_args.pop("user")
        password = connect_args.pop("password")
        connect_args["auth"] = trino.auth.BasicAuthentication(user, password)

        self._connection = trino.dbapi.connect(**connect_args)

    @property
    def wrapped_client(self):
        return self._connection
