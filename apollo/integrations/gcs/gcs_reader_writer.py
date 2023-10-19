import os
from typing import Optional

from apollo.agent.env_vars import STORAGE_BUCKET_NAME_ENV_VAR
from apollo.agent.models import AgentConfigurationError
from apollo.integrations.gcs.gcs_base_reader_writer import GcsBaseReaderWriter


class GcsReaderWriter(GcsBaseReaderWriter):
    """
    GCS Storage client implementation used in the agent, it initializes the client using the bucket name
    specified through `MCD_STORAGE_BUCKET_NAME` environment variable and with no credentials, as the default
    credentials from the environment will be used.
    """

    def __init__(self, prefix: Optional[str] = None, **kwargs):  # type: ignore
        bucket_name = os.getenv(STORAGE_BUCKET_NAME_ENV_VAR)
        if not bucket_name:
            raise AgentConfigurationError(
                f"Bucket not configured, {STORAGE_BUCKET_NAME_ENV_VAR} env var expected"
            )
        super().__init__(
            bucket_name=bucket_name, credentials=None, prefix=prefix, **kwargs
        )
