import os
from typing import Optional, Dict

import googleapiclient.discovery
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

from apollo.integrations.base_proxy_client import BaseProxyClient

_API_SERVICE_NAME = "bigquery"
_API_VERSION = "v2"


class BqProxyClient(BaseProxyClient):
    """
    BigQuery Proxy Client, simple class that uses the received credentials to create a BigQuery connection.
    This connection is returned as the `wrapped_client` attribute and the agent will take care of executing methods
    there.
    If no credentials are specified in the constructor (received in the request) the ADC (Application Default
    Credentials) will be used.
    When running in a CloudRun environment, ADC is derived from the environment (the service account running the
    CloudRun service), in a local dev environment `gcloud` CLI can be used to set ADC.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        bq_credentials: Optional[Credentials] = None
        if credentials:
            bq_credentials = Credentials.from_service_account_info(credentials)

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

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Convert Google API errors to error types that can be converted back to Google API errors client side.
        """
        if isinstance(error, HttpError):
            return "HttpError"
        return super().get_error_type(error)

    def get_error_extra_attributes(self, error: Exception) -> Optional[Dict]:
        """
        Return a dictionary with `resp` and `content` for Google API Errors.
        """
        if isinstance(error, HttpError):
            return {
                "resp": error.resp,
                "content": error.content,
            }
        return super().get_error_extra_attributes(error)
