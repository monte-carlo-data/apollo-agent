import logging
import socket
import sys
from typing import (
    Any,
    Dict,
    Optional,
)

import pymysql

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"
_MYSQL_DIRECTORY = "mysql/certs"

logger = logging.getLogger(__name__)


class MysqlProxyClient(BaseDbProxyClient):
    """
    Proxy client for MySQL Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `pymysql.connect`, so only attributes supported as parameters
    by `pymysql.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], platform: str, **kwargs: Any):
        super().__init__(connection_type="mysql")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Mysql agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = credentials[_ATTR_CONNECT_ARGS]
        ssl_options = credentials.get("ssl_options") or {}
        if ssl_options.get("ca"):
            cert_path = self.get_cert_path(
                platform=platform,
                remote_location=ssl_options["ca"],
                sub_folder=_MYSQL_DIRECTORY,
            )
            if cert_path:
                connect_args["ssl"] = {"ca": cert_path}
        self._connection = pymysql.connect(**connect_args)

        # we were having tcp keep alive issues in Azure, so we're forcing it to 30 secs
        sock: Optional[socket.socket] = (
            getattr(self._connection, "_sock")
            if hasattr(self._connection, "_sock")
            else None
        )
        if sock:
            # enables tcp keep alive messages
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if sys.platform == "darwin":
                # for macos, send tcp keep-alive packets every 30 secs
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 30)
            else:
                # start sending keep-alive packets after 30 seconds of inactivity.
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                # re-send keep-alive messages not acknowledged after 10 secs.
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                # 5 keep-alive messages lost before considering connection lost
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
        else:
            logger.warning("No _sock attribute found in mysql connection")

    @property
    def wrapped_client(self):
        return self._connection
