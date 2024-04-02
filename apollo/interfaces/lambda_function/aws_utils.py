from botocore.config import Config


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
