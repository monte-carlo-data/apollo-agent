import base64
import dataclasses
import gzip
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
from io import BytesIO
from typing import Any, Dict, List, Tuple, Union

from botocore.response import StreamingBody

from apollo.agent.constants import (
    ATTRIBUTE_NAME_DATA,
    ATTRIBUTE_NAME_TYPE,
    ATTRIBUTE_VALUE_TYPE_DATE,
    ATTRIBUTE_VALUE_TYPE_DATETIME,
    ATTRIBUTE_VALUE_TYPE_DECIMAL,
    ATTRIBUTE_VALUE_TYPE_BYTES,
    ATTRIBUTE_VALUE_TYPE_TIME,
    ATTRIBUTE_VALUE_TYPE_STREAMING_BODY,
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
        elif isinstance(value, time):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_TIME,
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
        elif isinstance(value, StreamingBody):
            # convert body to base64 in chunks
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_STREAMING_BODY,
                ATTRIBUTE_NAME_DATA: encode_streaming_body_gzip_base64(value),
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


def encode_dictionary(dict_value: Dict) -> Dict:
    def encode_deep(value: Any) -> Any:
        if isinstance(value, Dict):
            return encode_dictionary(value)
        else:
            return AgentSerializer.serialize(value)

    return {key: encode_deep(value) for key, value in dict_value.items()}


def encode_streaming_body_gzip_base64(
    streaming_body: StreamingBody, chunk_size: int = 4096
) -> str:
    """
    Compresses a boto3 StreamingBody object using gzip and then encodes the compressed
    content to base64. Reads the body in chunks to minimize memory usage.

    Args:
        streaming_body: The boto3 StreamingBody object.
        chunk_size: The size of the chunks to read from the body (in bytes). Defaults to 4096.

    Returns:
        A base64 encoded string representing the gzip-compressed content of the streaming body.
    """

    if not isinstance(streaming_body, StreamingBody):
        raise TypeError("streaming_body must be a boto3 StreamingBody object")
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")

    compressed_chunks = BytesIO()
    with gzip.GzipFile(fileobj=compressed_chunks, mode="wb") as gz:
        while True:
            chunk = streaming_body.read(chunk_size)
            if not chunk:
                break
            gz.write(chunk)

    compressed_chunks.seek(0)
    encoded_data = base64.b64encode(compressed_chunks.read()).decode("ascii")
    return encoded_data
