import os
from typing import Optional, Dict

import googleapiclient.discovery
from google.oauth2.service_account import Credentials

from apollo.integrations.base_proxy_client import BaseProxyClient

_API_SERVICE_NAME = "bigquery"
_API_VERSION = "v2"


class BqProxyClient(BaseProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs):
        bq_credentials: Optional[Credentials] = None
        if credentials:
            bq_credentials = Credentials.from_service_account_info(credentials)
        else:
            creds_file = os.getenv("BQ_CREDS_FILE")
            if creds_file:
                bq_credentials = Credentials.from_service_account_file(creds_file)

        # if no credentials are specified then ADC (app default credentials) will be used
        # in the context of Cloud Run it comes from the service account used to run the service
        self._client = googleapiclient.discovery.build(
            _API_SERVICE_NAME,
            _API_VERSION,
            credentials=bq_credentials,
            cache_discovery=False,
        )

    @property
    def wrapped_client(self):
        return self._client
