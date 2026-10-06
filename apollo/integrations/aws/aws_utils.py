import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3
from botocore.config import Config
from dataclasses_json import DataClassJsonMixin

from apollo.agent.utils import AgentUtils


@dataclass
class AwsSession(DataClassJsonMixin):
    access_key_id: str
    secret_key: str
    session_token: str


def assume_role(
    role_arn: str,
    external_id: Optional[str] = None,
    session_name: Optional[str] = None,
) -> AwsSession:
    """
    Assumes `role_arn` with the agent's own credentials and returns the temporary
    credentials. STS errors (e.g. AccessDenied) propagate to the caller.
    :param session_name: RoleSessionName to use, a unique random name by default.
    """
    params: Dict[str, Any] = {
        "RoleArn": role_arn,
        "RoleSessionName": session_name
        or f"mcd_{AgentUtils.generate_random_str(rand_len=5)}_{time.time()}",
    }
    if external_id:
        params["ExternalId"] = external_id

    credentials = boto3.client("sts").assume_role(**params)["Credentials"]
    return AwsSession(
        credentials["AccessKeyId"],
        credentials["SecretAccessKey"],
        credentials["SessionToken"],
    )


def get_boto_config(connect_timeout: int, max_attempts: int = 3) -> Config:
    """
    Returns a boto3 client configuration with the specified connection timeout and a single
    retry in standard mode.
    By default, connect_timeout is 60 seconds and legacy retry mode uses 4 attempts, so it takes
    around 5 minutes to fail if connectivity is not allowed (for example Lambda function configured
    with no external network access and no VPC endpoints).
    """
    return Config(
        connect_timeout=connect_timeout,
        retries=dict(
            mode="standard",
            max_attempts=max_attempts,
        ),
    )
