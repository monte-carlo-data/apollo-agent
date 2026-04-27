import struct
from datetime import datetime, timezone, timedelta
from typing import List

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient


def odbc_escape(value: str) -> str:
    """Escape an ODBC connection string value by wrapping in braces if it contains special chars.

    Values containing ``;``, ``{``, ``}``, or ``=`` are wrapped in curly braces.  Any literal
    ``}`` inside the value is doubled (``}}``) per the ODBC spec.  Values already wrapped in a
    matching ``{...}`` pair (e.g. driver names) are left unchanged.
    """
    if value.startswith("{") and value.endswith("}"):
        return value
    if any(c in value for c in (";", "{", "}", "=")):
        return "{" + value.replace("}", "}}") + "}"
    return value


def odbc_string_from_dict(connect_args: dict) -> str:
    """Serialize a dict of ODBC key-value pairs to a connection string."""
    return ";".join(f"{k}={odbc_escape(str(v))}" for k, v in connect_args.items())


class TSqlBaseDbProxyClient(BaseDbProxyClient):
    """Base class for pyodbc-based T-SQL clients (SQL Server, Azure SQL, MS Fabric).

    Provides datetimeoffset binary decoding and pyodbc cursor description normalization,
    which are identical across all T-SQL pyodbc clients.
    """

    _DATETIMEOFFSET_SQL_TYPE_CODE = -155

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
