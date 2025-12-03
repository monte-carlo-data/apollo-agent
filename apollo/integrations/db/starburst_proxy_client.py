from typing import Optional, Dict, Any

import trino

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


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

