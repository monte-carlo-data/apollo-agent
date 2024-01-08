import logging
from typing import Optional, Dict, Any
from urllib.request import urlretrieve

import prestodb

from apollo.agent.utils import AgentUtils
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient
from apollo.integrations.storage.base_storage_client import BaseStorageClient
from apollo.integrations.storage.storage_proxy_client import StorageProxyClient

_ATTR_CONNECT_ARGS = "connect_args"
_CERT_RETRIEVAL_METHOD_URL = "url"
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

        connect_args = credentials[_ATTR_CONNECT_ARGS]
        if auth := connect_args.pop("auth"):
            connect_args.update({"auth": prestodb.auth.BasicAuthentication(**auth)})

        self._connection = prestodb.dbapi.connect(**connect_args)

        ssl_options = credentials.get("ssl_options", {})
        if bool(ssl_options.get("skip_verification")):
            logger.info("Skipping certificate validation")
            self._connection._http_session.verify = False
        elif ssl_options.get("mechanism") and ssl_options.get("cert"):
            cert_path = self._get_cert_path(
                ssl_options["mechanism"], ssl_options["cert"]
            )
            if cert_path:
                self._connection._http_session.verify = cert_path

    @property
    def wrapped_client(self):
        return self._connection

    def _get_cert_path(
        self, retrieval_mechanism: str, remote_location: str
    ) -> Optional[str]:
        download_path = AgentUtils.temp_file_path(sub_folder=_PRESTO_DIRECTORY)
        if retrieval_mechanism == _CERT_RETRIEVAL_METHOD_URL:
            urlretrieve(url=remote_location, filename=download_path)
        else:
            storage_client = StorageProxyClient(self._platform).wrapped_client
            try:
                storage_client.download_file(
                    key=remote_location, download_path=download_path
                )
            except BaseStorageClient.NotFoundError as exc:
                logger.warning("Certificate not found in storage bucket", exc_info=exc)
                return None
        return download_path
