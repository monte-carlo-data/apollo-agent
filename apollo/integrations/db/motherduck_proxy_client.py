import os
import duckdb
from typing import (
    Any,
    Dict,
    Optional,
)

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class MotherDuckProxyClient(BaseDbProxyClient):
    """
    Proxy client for Motherduck client. Credentials are expected to be supplied under "connect_args" and will
    be passed directly to `duckdb.connect`. 'duckdb` accepts a connection string with the connection details,
    so "connect_args" will be a string.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="motherduck")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Motherduck agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        # Duckdb can be used in-memory or to connect to Motherduck, a serverless duckdb.
        # Duckdb isn't smart enough for us to specify we only care about connecting to
        # the cloud. It looks for the env var 'HOME' to know where to set up local files
        # in case you use it in-memory. Lambda functions don't have a HOME env var so we
        # must tell Duckdb where to create the local files. This also means that function
        # memory likely needs to be increased to >1050mb
        # https://github.com/duckdb/duckdb/issues/3855
        if not os.environ.get("HOME"):
            path = "/tmp"
            os.environ["HOME"] = path
            os.makedirs(path, exist_ok=True)
        self._connection = duckdb.connect(credentials[_ATTR_CONNECT_ARGS])  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection
