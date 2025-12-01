import gzip
import json
import logging
from typing import Dict, Any

from apollo.egress.agent.config.config_keys import (
    CONFIG_PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS,
)
from apollo.egress.agent.config.config_manager import ConfigurationManager
from apollo.egress.agent.service.operation_result import OperationAttributes
from apollo.egress.agent.service.storage_service import StorageService
from apollo.common.agent.constants import ATTRIBUTE_NAME_RESULT
from apollo.common.agent.serde import AgentSerializer

logger = logging.getLogger(__name__)

_ATTR_NAME_RESULT_LOCATION = "__mcd_result_location__"
_ATTR_NAME_RESULT_COMPRESSED = "__mcd_result_compressed__"

_DEFAULT_PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS = 60 * 60 * 1  # 1 hour


class ResultsProcessor:
    """
    Takes care of uploading the response to the internal stage (and compress it) if needed, based
    on the settings received in the operation attributes.
    """

    def __init__(self, config_manager: ConfigurationManager, storage: StorageService):
        self._storage = storage
        self._config_manager = config_manager

    def process_result(
        self, result: Dict[str, Any], operation_attrs: OperationAttributes
    ) -> Dict[str, Any]:
        size = self._calculate_result_size(result)

        if self._must_use_pre_signed_url(operation_attrs, size):
            key = f"responses/{operation_attrs.trace_id}"
            contents = self._serialize_result(result)
            compressed = False
            if operation_attrs.compress_response_file:
                contents = gzip.compress(contents.encode("utf-8"))
                compressed = True
            logger.info(
                f"Uploading large result for operation: {operation_attrs.operation_id}, trace_id: {operation_attrs.trace_id}, compressed: {compressed}"
            )
            self._storage.write(
                key=key,
                obj_to_write=contents,
            )
            expiration_seconds = self._config_manager.get_int_value(
                CONFIG_PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS,
                _DEFAULT_PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS,
            )
            url = self._storage.generate_presigned_url(key, expiration_seconds)
            result[_ATTR_NAME_RESULT_LOCATION] = url
            result[_ATTR_NAME_RESULT_COMPRESSED] = compressed
            del result[ATTRIBUTE_NAME_RESULT]
            logger.info(
                f"{'Compressed result' if compressed else 'Result'} uploaded, operation: {operation_attrs.operation_id}"
            )
        return result

    @staticmethod
    def _must_use_pre_signed_url(
        operation_attrs: OperationAttributes, size: int
    ) -> bool:
        return 0 < operation_attrs.response_size_limit_bytes < size

    @classmethod
    def _calculate_result_size(cls, result: Dict[str, Any]) -> int:
        return len(cls._serialize_result(result).encode())

    @staticmethod
    def _serialize_result(result: Dict[str, Any]) -> str:
        return json.dumps(result, cls=AgentSerializer)
