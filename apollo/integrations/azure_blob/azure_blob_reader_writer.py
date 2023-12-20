import os
from typing import Optional

from azure.identity import DefaultAzureCredential

from apollo.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_ACCOUNT_NAME_ENV_VAR,
)
from apollo.agent.models import AgentConfigurationError
from apollo.integrations.azure_blob.azure_blob_base_reader_writer import (
    AzureBlobBaseReaderWriter,
)


class AzureBlobReaderWriter(AzureBlobBaseReaderWriter):
    """
    Azure Storage client implementation used in the agent, it initializes the client using the
    bucket name specified through `MCD_STORAGE_BUCKET_NAME` environment variable and with an empty
    connection string.
    """

    def __init__(self, prefix: Optional[str] = None, **kwargs):  # type: ignore
        bucket_name = os.getenv(STORAGE_BUCKET_NAME_ENV_VAR)
        if not bucket_name:
            raise AgentConfigurationError(
                f"Bucket not configured, {STORAGE_BUCKET_NAME_ENV_VAR} env var expected"
            )
        account_name = os.getenv(STORAGE_ACCOUNT_NAME_ENV_VAR)
        if account_name:
            connection_string = account_name
            credential = DefaultAzureCredential()
        else:
            connection_string = kwargs.get(
                "connection_string", os.getenv("AzureWebJobsStorage", "")
            )
            credential = None
            # raise AgentConfigurationError(
            #     f"Storage account not configured, {STORAGE_ACCOUNT_NAME_ENV_VAR} env var expected"
            # )
        super().__init__(
            bucket_name=bucket_name,
            connection_string=connection_string,
            prefix=prefix,
            credential=credential,
            **kwargs,
        )
