from typing import Dict, Optional, Any

import psycopg2
from psycopg2 import DatabaseError
from psycopg2.errors import QueryCanceled, InsufficientPrivilege

from apollo.integrations.base_proxy_client import BaseProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class RedshiftProxyClient(BaseProxyClient):
    """
    Proxy client for Redshift.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `psycopg2.connect`, so
    only attributes supported as parameters by `psycopg2.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Redshift agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        self._connection = psycopg2.connect(
            **credentials[_ATTR_CONNECT_ARGS],
        )
        if credentials.get("autocommit", False):
            self._connection.autocommit = True

    @property
    def wrapped_client(self):
        return self._connection

    def process_result(self, value: Any) -> Any:
        """
        Converts "Column" objects in the description into a list of objects that can be serialized to JSON.
        From the DBAPI standard, description is supposed to return tuples with 7 elements, so we're returning
        those 7 elements back for each element in description.
        """
        if isinstance(value, Dict) and "description" in value:
            description = value["description"]
            value["description"] = [
                [col[0], col[1], col[2], col[3], col[4], col[5], col[6]]
                for col in description
            ]
        return value

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Convert PG errors QueryCanceled and DatabaseError to error types that can be converted back to PG errors
        client side.
        """
        if isinstance(error, QueryCanceled):
            return "QueryCanceled"
        elif isinstance(error, InsufficientPrivilege):
            return "InsufficientPrivilege"
        elif isinstance(error, DatabaseError):
            return "DatabaseError"
        return super().get_error_type(error)
