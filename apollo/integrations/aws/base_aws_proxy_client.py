import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3
from dataclasses_json import DataClassJsonMixin

from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.db.base_db_proxy_client import SslOptions


@dataclass
class AwsSession(DataClassJsonMixin):
    access_key_id: str
    secret_key: str
    session_token: str


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
        self._client = self.create_boto_client(
            service_type=service_type,
            assumable_role=credentials.get("assumable_role") if credentials else None,
            aws_region=credentials.get("aws_region") if credentials else None,
            external_id=credentials.get("external_id") if credentials else None,
            ssl_options=credentials.get("ssl_options") if credentials else None,
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
        return session.client(
            service_type,
            verify=ssl_config.write_ca_data_to_temp_file(
                f"/tmp/{service_type}_ca_bundle.pem", upsert=True
            )
            if ssl_config.ca_data
            else None,
        )

    @staticmethod
    def _assume_role(
        assumable_role: str, external_id: Optional[str] = None
    ) -> AwsSession:
        session_name = f"mcd_{AgentUtils.generate_random_str(rand_len=5)}_{time.time()}"
        assume_role_params = {
            "RoleArn": assumable_role,
            "RoleSessionName": session_name,
        }

        if external_id:
            assume_role_params["ExternalId"] = external_id

        assumed_role = boto3.client("sts").assume_role(**assume_role_params)
        return AwsSession(
            assumed_role["Credentials"]["AccessKeyId"],
            assumed_role["Credentials"]["SecretAccessKey"],
            assumed_role["Credentials"]["SessionToken"],
        )
