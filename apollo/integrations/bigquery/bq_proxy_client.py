import os
from typing import Optional, Dict

import googleapiclient.discovery
from google.oauth2.service_account import Credentials

_API_SERVICE_NAME = "bigquery"
_API_VERSION = "v2"


class BqProxyClient:
    def __init__(self, **kwargs):
        bq_credentials: Optional[Credentials] = None
        sa_file = os.getenv("SA_FILE")
        if sa_file:
            bq_credentials = Credentials.from_service_account_file(sa_file)

        self._client = googleapiclient.discovery.build(
            _API_SERVICE_NAME,
            _API_VERSION,
            credentials=bq_credentials,
            cache_discovery=False,
        )

    @property
    def wrapped_client(self):
        return self._client
