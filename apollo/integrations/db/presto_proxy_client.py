import logging
from typing import Optional, Dict, Any

import prestodb

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"
_PRESTO_DIRECTORY = "presto/certs"

logger = logging.getLogger(__name__)


class PrestoProxyClient(BaseDbProxyClient):
    """
    Proxy client for Presto "database" Client which is used by Presto connections. Credentials
    are expected to be supplied under "connect_args" and will be passed directly to
    `prestodb.dbapi.connect`.
    """

    def __init__(self, credentials: Optional[Dict], platform: str, **kwargs: Any):
        self._platform = platform
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Presto agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args: Dict[str, Any] = {**credentials[_ATTR_CONNECT_ARGS]}
        if auth := connect_args.pop("auth"):
            connect_args.update({"auth": prestodb.auth.BasicAuthentication(**auth)})

        self._connection = prestodb.dbapi.connect(**connect_args)

        ssl_options = credentials.get("ssl_options") or {}
        if bool(ssl_options.get("skip_verification")):
            logger.info("Skipping certificate validation")
            self._connection._http_session.verify = False
        elif ssl_options.get("mechanism") and ssl_options.get("cert"):
            cert_path = self.get_cert_path(
                platform=platform,
                remote_location=ssl_options["cert"],
                retrieval_mechanism=ssl_options["mechanism"],
                sub_folder=_PRESTO_DIRECTORY,
            )
            if cert_path:
                self._connection._http_session.verify = cert_path

    @property
    def wrapped_client(self):
        return self._connection
