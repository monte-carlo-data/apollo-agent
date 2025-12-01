import logging
from collections.abc import Callable
from datetime import timedelta
from typing import Dict, Any, Optional, Union

from apollo.common.integrations.storage.base_storage_client import BaseStorageClient
from apollo.common.agent.serde import AgentSerializer
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_ERROR,
)

logger = logging.getLogger(__name__)
_ERROR_TYPE_NOTFOUND = "NotFound"
_ERROR_TYPE_PERMISSIONS = "Permissions"


class StorageService:
    """
    Storage service class, used to implement the storage operations received by
    the MC backend, by interacting with the storage client.
    """

    def __init__(self, client: BaseStorageClient):
        self._client = client
        self._mapping: Dict[str, Callable[[Dict[str, Any]], Any]] = {
            "storage_read": self._read,
            "storage_read_json": self._read_json,
            "storage_write": self._write,
            "storage_delete": self._delete,
            "storage_generate_presigned_url": self._pre_signed_url,
            "storage_is_bucket_private": self._is_bucket_private,
        }

    def execute_operation(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Called by the SNA service when a "storage" operation is received.
        @return: a dictionary, following the agent format, with the result of the operation
            or an error message.
        """
        operation = event.get("operation", {})
        operation_type = operation.get("type")
        method = self._mapping.get(operation_type) if operation_type else None
        if not method:
            return {
                ATTRIBUTE_NAME_ERROR: f"Invalid operation type: {operation_type}",
            }

        try:
            storage_result = method(operation)
            result = {
                ATTRIBUTE_NAME_RESULT: storage_result,
            }
        except Exception as ex:
            # don't log error messages for idempotent files not found
            is_idempotent_file = operation.get("key", "").startswith("idempotent/")
            if (
                not isinstance(ex, BaseStorageClient.NotFoundError)
                or not is_idempotent_file
            ):
                logger.error(f"Storage operation failed: {ex}")
            result = {
                ATTRIBUTE_NAME_ERROR_TYPE: self._get_error_type(ex),
                ATTRIBUTE_NAME_ERROR: str(ex),
            }
        return result

    def write(self, key: str, obj_to_write: Union[bytes, str]):
        """
        Saves the specified contents to the file at `key`.
        """
        self._client.write(key=key, obj_to_write=obj_to_write)

    def generate_presigned_url(self, key: str, expiration_seconds: int) -> str:
        """
        Generates a pre-signed URL for the specified key.
        """
        return self._client.generate_presigned_url(
            key=key,
            expiration=timedelta(seconds=expiration_seconds),
        )

    @staticmethod
    def _get_error_type(error: Exception) -> Optional[str]:
        """
        Returns an error type string for the given exception, this is used client side to create
         again the required exception type.
        :param error: the exception occurred.
        :return: an error type if the exception is mapped to an error type for this client,
            `None` otherwise.
        """
        if isinstance(error, BaseStorageClient.PermissionsError):
            return _ERROR_TYPE_PERMISSIONS
        elif isinstance(error, BaseStorageClient.NotFoundError):
            return _ERROR_TYPE_NOTFOUND
        return type(error).__name__

    def _read(self, operation: Dict[str, Any]) -> Any:
        contents = self._client.read(
            key=self._get_key(operation),
            decompress=operation.get("decompress", False),
            encoding=operation.get("encoding"),
        )
        if isinstance(contents, bytes):
            return AgentSerializer.serialize(contents)
        return contents

    def _read_json(self, operation: Dict[str, Any]) -> Any:
        return self._client.read_json(key=self._get_key(operation))

    def _write(self, operation: Dict[str, Any]) -> Any:
        obj_to_write = operation.get("obj_to_write")
        if isinstance(obj_to_write, str) or isinstance(obj_to_write, bytes):
            self._client.write(key=self._get_key(operation), obj_to_write=obj_to_write)
        elif obj_to_write:
            raise ValueError(
                f"Invalid type for obj_to_write parameter: {type(obj_to_write).__name__}"
            )
        return {}

    def _delete(self, operation: Dict[str, Any]) -> Any:
        self._client.delete(key=self._get_key(operation))
        return {}

    def _pre_signed_url(self, operation: Dict[str, Any]) -> Any:
        return self._client.generate_presigned_url(
            key=self._get_key(operation),
            expiration=timedelta(seconds=operation.get("expiration", 300)),
        )

    def _is_bucket_private(self, operation: Dict[str, Any]) -> Any:
        return self._client.is_bucket_private()

    @staticmethod
    def _get_key(operation: Dict[str, Any]) -> str:
        key = operation.get("key")
        if not key:
            raise ValueError("Key is required")
        return key
