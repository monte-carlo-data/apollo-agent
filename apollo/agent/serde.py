import base64
import dataclasses
import ipaddress
import json
import uuid
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from decimal import Decimal
from typing import Any, Dict, List, Tuple, Union

from apollo.agent.constants import (
    ATTRIBUTE_NAME_DATA,
    ATTRIBUTE_NAME_TYPE,
    ATTRIBUTE_VALUE_TYPE_DATE,
    ATTRIBUTE_VALUE_TYPE_DATETIME,
    ATTRIBUTE_VALUE_TYPE_DECIMAL,
    ATTRIBUTE_VALUE_TYPE_BYTES,
)


class AgentSerializer(json.JSONEncoder):
    @classmethod
    def serialize(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_DATETIME,
                ATTRIBUTE_NAME_DATA: value.isoformat(),
            }
        elif isinstance(value, date):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_DATE,
                ATTRIBUTE_NAME_DATA: value.isoformat(),
            }
        elif isinstance(value, Decimal):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_DECIMAL,
                ATTRIBUTE_NAME_DATA: str(value),
            }
        elif isinstance(value, bytes) or isinstance(value, bytearray):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_BYTES,
                ATTRIBUTE_NAME_DATA: base64.b64encode(value).decode("utf-8"),
            }
        elif dataclasses.is_dataclass(value):
            return dataclasses.asdict(value)

        return value

    def default(self, obj: Any):
        serialized = self.serialize(obj)
        if serialized is not obj:  # serialization happened
            return serialized
        return super().default(obj)


def row_value_encoder(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, time):
        return value.isoformat()
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, timedelta):
        return str(value)
    elif isinstance(value, uuid.UUID):
        return str(value)
    elif isinstance(value, ipaddress.IPv4Address):
        return str(value)
    elif isinstance(value, ipaddress.IPv6Address):
        return str(value)
    return value


def rows_encoder(
    rows: Union[List[List[Any]], List[Tuple], List[Dict]]
) -> List[List[Any]]:
    return [[row_value_encoder(value) for value in row] for row in rows]


def decode_dict_value(value: Dict) -> Any:
    if value.get(ATTRIBUTE_NAME_TYPE) == ATTRIBUTE_VALUE_TYPE_BYTES:
        return base64.b64decode(value.get(ATTRIBUTE_NAME_DATA))  # type: ignore
    return value


def decode_dictionary(dict_value: Dict) -> Dict:
    def decode_deep(value: Any) -> Any:
        if isinstance(value, Dict):
            return (
                decode_dict_value(value)
                if ATTRIBUTE_NAME_TYPE in value
                else decode_dictionary(value)
            )
        else:
            return value

    return {key: decode_deep(value) for key, value in dict_value.items()}
