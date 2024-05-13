import struct
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

import pyodbc

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class AzureDatabaseProxyClient(BaseDbProxyClient):
    """
    Proxy client for Azure "database" Client which is used by the Azure Dedicated SQL Pool and Azure SQL Database connections.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `pyodbc.connect`.
    'pyodbc` accepts a connection string with the connection details, so "connect_args" will be a string.
    """

    _DATETIMEOFFSET_SQL_TYPE_CODE = -155
    _DEFAULT_LOGIN_TIMEOUT_IN_SECONDS = 15
    _DEFAULT_QUERY_TIMEOUT_IN_SECONDS = 60 * 14  # 14 minutes

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="azure-database")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Azure database agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = pyodbc.connect(
            credentials[_ATTR_CONNECT_ARGS],
            # Set timeout for establishing connection to db
            timeout=credentials.get(
                "login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS
            ),
        )  # type: ignore

        # Add output converter to handle datetimeoffset data types that are not supported by pyodbc
        self._connection.add_output_converter(
            self._DATETIMEOFFSET_SQL_TYPE_CODE, self._handle_datetimeoffset
        )

        # Set timeout for any query executed through this connection
        self._connection.timeout = credentials.get(
            "query_timeout_in_seconds", self._DEFAULT_QUERY_TIMEOUT_IN_SECONDS
        )

    @property
    def wrapped_client(self):
        return self._connection

    @classmethod
    def _process_description(cls, col: List) -> List:
        # pyodbc cursor returns the column type as <class 'str'> instead of a type_code which
        # we expect. Here we are converting this type to a string of the type so the description
        # can be serialized. So <class 'str'> will become just 'str'
        return [col[0], col[1].__name__, col[2], col[3], col[4], col[5], col[6]]

    @staticmethod
    def _handle_datetimeoffset(dto_value: bytes) -> datetime:
        """
        Input: a bytes representation of SQL server's 'datetimeoffset' date type (a timezone aware datetime)
        Output: a timezone-aware datetime object

        Unpacks the binary value into a tuple of (year, month, day, hour, minute, second, microsecond, hour-offset, minute-offset)
        then uses the tuple to create a datetime() with timezone

        "<6hI2h" is a format string to describe the layout for unpacking (https://docs.python.org/3/library/struct.html#struct-format-strings)
        What each part does:
            <: This specifies that the data should be interpreted in little-endian byte order. (least significant byte (LSB) is stored first).
            6h: This specifies that there should be 6 signed short integers (16-bit) present in the binary data.
                (year, month, day, hour, minute, second)
            I: This specifies that there should be 1 unsigned integer (32-bit) present in the binary data.
                (microsecond)
            2h: This specifies that there should be 2 signed short integers (16-bit) present in the binary data.
                (hour and minute of timezone difference)
        """
        tup = struct.unpack("<6hI2h", dto_value)

        # Use the tuple to create a datetime() with timezone
        return datetime(
            tup[0],
            tup[1],
            tup[2],
            tup[3],
            tup[4],
            tup[5],
            tup[6] // 1000,
            timezone(timedelta(hours=tup[7], minutes=tup[8])),
        )
