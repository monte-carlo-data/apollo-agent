import os
from typing import Optional, Any

from google.api_core.exceptions import Forbidden
from google.cloud import storage
from google.cloud.storage import transfer_manager
from google.oauth2.service_account import Credentials

from apollo.agent.models import AgentWrappedError
from apollo.integrations.base_proxy_client import BaseProxyClient

_API_SERVICE_NAME = "storage"
_API_VERSION = "v1"

_ERROR_TYPE_NOTFOUND = "NotFound"
_ERROR_TYPE_PERMISSIONS = "Permissions"


class GcsProxyClient(BaseProxyClient):
    def __init__(self, **kwargs):
        gcs_credentials: Optional[Credentials] = None
        creds_file = os.getenv("GCS_CREDS_FILE")
        if creds_file:
            gcs_credentials = Credentials.from_service_account_file(creds_file)

        self._client = storage.Client(credentials=gcs_credentials)

    @staticmethod
    def download_chunks_concurrently(**kwargs):
        transfer_manager.download_chunks_concurrently(**kwargs)

    @staticmethod
    def check_blob(blob: Any, key: str):
        if not blob:
            raise AgentWrappedError(f"blob with key {key} does not exist", "NotFound")

    @property
    def wrapped_client(self):
        return self._client

    def get_error_type(self, error: Exception) -> Optional[str]:
        if isinstance(error, Forbidden):
            return _ERROR_TYPE_PERMISSIONS
        return super().get_error_type(error)
