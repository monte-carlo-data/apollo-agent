import functools
import struct
from datetime import datetime, timezone, timedelta
from typing import FrozenSet, List

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ODBC_DRIVER_17 = "ODBC Driver 17 for SQL Server"
_ODBC_DRIVER_18 = "ODBC Driver 18 for SQL Server"


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


@functools.cache
def _installed_odbc_drivers() -> FrozenSet[str]:
    import pyodbc

    return frozenset(pyodbc.drivers())


def _split_odbc_pairs(connection_string: str) -> List[str]:
    """Split a connection string on ``;``, except inside ``{...}`` values (``}}`` is an escaped brace)."""
    pairs: List[str] = []
    current: List[str] = []
    in_braces = False
    i = 0
    while i < len(connection_string):
        char = connection_string[i]
        if in_braces and connection_string.startswith("}}", i):
            current.append("}}")
            i += 2
            continue
        if char == "{":
            in_braces = True
        elif char == "}":
            in_braces = False
        elif char == ";" and not in_braces:
            pairs.append("".join(current))
            current = []
            i += 1
            continue
        current.append(char)
        i += 1
    if current:
        pairs.append("".join(current))
    return pairs


def normalize_odbc_driver(connection_string: str) -> str:
    """Run a Driver 17 connection string on Driver 18 when only 18 is installed.

    Stored and self-hosted connection strings name Driver 17, which has no arm64 build.
    Driver 18 defaults to ``Encrypt=yes``, so ``Encrypt=no`` (Driver 17's default) is
    added when the string doesn't set it. Returned unchanged whenever Driver 17 is installed.
    """
    pairs = _split_odbc_pairs(connection_string)
    keys = [pair.split("=", 1)[0].strip().lower() for pair in pairs]
    if "driver" not in keys:
        return connection_string
    driver_index = keys.index("driver")
    driver = pairs[driver_index].split("=", 1)[1].strip().strip("{}").strip()
    if driver.lower() != _ODBC_DRIVER_17.lower():
        return connection_string

    installed = _installed_odbc_drivers()
    if _ODBC_DRIVER_17 in installed or _ODBC_DRIVER_18 not in installed:
        return connection_string

    pairs[driver_index] = f"DRIVER={{{_ODBC_DRIVER_18}}}"
    if "encrypt" not in keys:
        pairs.append("Encrypt=no")
    return ";".join(pairs)


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
