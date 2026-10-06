from typing import Any, Dict, Optional

import boto3

from apollo.integrations.aws.aws_utils import AwsSession, assume_role
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.db.base_db_proxy_client import SslOptions


class BaseAwsProxyClient(BaseProxyClient):
    """
    A generic Proxy Client for AWS service APIs. This is a simple class that uses the received
    credentials to create an AWS session and a service client from the session. This created
    client is returned as the `wrapped_client` attribute and the agent will take care of
    executing methods there.

    If no credentials are specified in the constructor (received in the request) then the
    client and resource are created using the default settings supported by the boto3 library,
    which means env vars need to be set with the correct credentials to use.
    """

    def __init__(self, service_type: str, credentials: Optional[Dict], **kwargs: Any):
        creds = credentials.get("connect_args") if credentials else None
        self._client = self.create_boto_client(
            service_type=service_type,
            assumable_role=creds.get("assumable_role") if creds else None,
            aws_region=creds.get("aws_region") if creds else None,
            external_id=creds.get("external_id") if creds else None,
            ssl_options=creds.get("ssl_options") if creds else None,
        )

    @property
    def wrapped_client(self):
        return self._client

    def create_boto_client(
        self,
        service_type: str,
        aws_region: Optional[str] = None,
        assumable_role: Optional[str] = None,
        external_id: Optional[str] = None,
        ssl_options: Optional[dict] = None,
    ):
        ssl_config = SslOptions(**(ssl_options or {}))
        if assumable_role:
            assumed_role = self._assume_role(
                assumable_role=assumable_role, external_id=external_id
            )
            session = boto3.Session(
                aws_access_key_id=assumed_role.access_key_id,
                aws_secret_access_key=assumed_role.secret_key,
                aws_session_token=assumed_role.session_token,
                region_name=aws_region,
            )
        else:
            session = boto3.Session(region_name=aws_region)
        ca_bundle_path = None
        if ssl_config.ca_data:
            # Write the CA bundle to a unique temp file and register it for
            # deletion when the client is closed.
            ca_bundle_path = ssl_config.write_ca_data_to_temp_file(
                suffix="_ca_bundle.pem"
            )
            self.register_temp_files([ca_bundle_path])
        return session.client(service_type, verify=ca_bundle_path)

    @staticmethod
    def _assume_role(
        assumable_role: str, external_id: Optional[str] = None
    ) -> AwsSession:
        return assume_role(assumable_role, external_id=external_id)
