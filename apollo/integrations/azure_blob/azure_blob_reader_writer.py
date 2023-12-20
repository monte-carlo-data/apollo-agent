import os
from datetime import datetime
from typing import Optional, cast

from azure.identity import DefaultAzureCredential
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
    BlobServiceClient,
)

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
        connection_string = kwargs.get("connection_string", "")

        account_name = os.getenv(STORAGE_ACCOUNT_NAME_ENV_VAR)
        if account_name:
            account_url = f"https://{account_name}.blob.core.windows.net"
            credential = DefaultAzureCredential()
        else:
            account_url = None
            credential = None
        super().__init__(
            bucket_name=bucket_name,
            connection_string=connection_string,
            prefix=prefix,
            account_url=account_url,
            credential=credential,
            **kwargs,
        )

    def _generate_sas_token(
        self, blob_client: BlobClient, expiry: datetime, permission: BlobSasPermissions
    ):
        account_name = os.getenv(STORAGE_ACCOUNT_NAME_ENV_VAR)
        if account_name:
            return generate_blob_sas(
                account_name=account_name,
                user_delegation_key=self._client.get_user_delegation_key(
                    key_start_time=datetime.utcnow(),
                    key_expiry_time=expiry,
                ),
                container_name=blob_client.container_name,
                blob_name=blob_client.blob_name,
                expiry=expiry,
                permission=permission,
            )
        else:
            return super()._generate_sas_token(blob_client, expiry, permission)

    def _get_client_to_get_access_policy(self) -> BlobServiceClient:
        st_client = self._get_storage_management_client()
        account_name = os.getenv(STORAGE_ACCOUNT_NAME_ENV_VAR)
        if account_name:
            resource_group = os.getenv("WEBSITE_RESOURCE_GROUP", "")
            keys = st_client.storage_accounts.list_keys(
                resource_group_name=resource_group,
                account_name=account_name,
            )
            key: str = keys.keys[0].value  # type: ignore
            return BlobServiceClient(
                f"https://{account_name}.blob.core.windows.net",
                {
                    "account_name": account_name,
                    "account_key": key,
                },
            )
        else:
            return super()._get_client_to_get_access_policy()

    @staticmethod
    def _get_storage_management_client():
        owner_name = cast(
            str, os.getenv("WEBSITE_OWNER_NAME")
        )  # subscription_id+resource_group_region_etc
        subscription_id = owner_name.split("+")[0]

        # this code requires AZURE_CLIENT_ID to be set if a user managed identity is used
        return StorageManagementClient(DefaultAzureCredential(), subscription_id)
