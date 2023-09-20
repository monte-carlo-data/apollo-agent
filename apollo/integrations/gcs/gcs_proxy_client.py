import os
from typing import Optional

from google.cloud import storage
from google.oauth2.service_account import Credentials

from apollo.integrations.base_proxy_client import BaseProxyClient

_API_SERVICE_NAME = "storage"
_API_VERSION = "v1"


class GcsProxyClient(BaseProxyClient):
    def __init__(self, **kwargs):
        gcs_credentials: Optional[Credentials] = None
        creds_file = os.getenv("GCS_CREDS_FILE")
        if creds_file:
            gcs_credentials = Credentials.from_service_account_file(creds_file)

        self._client = storage.Client(credentials=gcs_credentials)

    @property
    def wrapped_client(self):
        return self._client
