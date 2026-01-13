from typing import Optional, Dict

import googleapiclient.discovery
from google.oauth2.service_account import Credentials

from apollo.integrations.base_proxy_client import BaseProxyClient

_API_SERVICE_NAME = "bigquery"
_API_VERSION = "v2"
_ATTR_CONNECT_ARGS = "connect_args"


class BqProxyClient(BaseProxyClient):
    """
    BigQuery Proxy Client, simple class that uses the received credentials to create a BigQuery connection.
    This connection is returned as the `wrapped_client` attribute and the agent will take care of executing methods
    there.
    If no credentials are specified in the constructor (received in the request) the ADC (Application Default
    Credentials) will be used.
    When running in a CloudRun environment, ADC is derived from the environment (the service account running the
    CloudRun service), in a local dev environment `gcloud` CLI can be used to set ADC.

    Credentials can be provided in two formats:
    1. Direct service account JSON (legacy format)
    2. Wrapped in 'connect_args' (for self-hosted credentials)
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        bq_credentials: Optional[Credentials] = None
        if credentials:
            # Support both direct credentials and connect_args format (for self-hosted credentials)
            service_account_info = credentials.get(_ATTR_CONNECT_ARGS, credentials)
            bq_credentials = Credentials.from_service_account_info(service_account_info)

        # if no credentials are specified then ADC (app default credentials) will be used
        # in the context of Cloud Run it comes from the service account used to run the service
        # in local dev environments you can use gcloud CLI to set ADC.
        self._client = googleapiclient.discovery.build(
            _API_SERVICE_NAME,
            _API_VERSION,
            credentials=bq_credentials,
            cache_discovery=False,
        )

    @property
    def wrapped_client(self):
        return self._client
