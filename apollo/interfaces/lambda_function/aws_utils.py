from botocore.config import Config


def get_retrieve_current_image_boto_config(connection_timeout: int) -> Config:
    """
    Configured the boto3 client (lambda or cloudformation) used to retrieve the current image
    with 10 seconds timeout and a single retry, so it would take 20 seconds to fail instead
    of 5 minutes (with the default settings).
    """
    return Config(
        connect_timeout=connection_timeout,
        retries=dict(
            mode="standard",
            max_attempts=1,
        ),
    )
