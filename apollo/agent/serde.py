import base64
import json
from datetime import (
    date,
    datetime,
)
from decimal import Decimal
from typing import Any

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
        elif isinstance(value, bytes):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_BYTES,
                ATTRIBUTE_NAME_DATA: base64.b64encode(value).decode("utf-8"),
            }

        return value

    def default(self, obj: Any):
        serialized = self.serialize(obj)
        if serialized is not obj:  # serialization happened
            return serialized
        return super().default(obj)
