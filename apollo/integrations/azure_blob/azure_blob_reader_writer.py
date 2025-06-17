import os
from datetime import datetime
from typing import Optional

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
from apollo.integrations.azure_blob.utils import AzureUtils


class AzureBlobReaderWriter(AzureBlobBaseReaderWriter):
    """
    Azure Storage client implementation used in the agent, it initializes the client using the
    account and container names specified through `MCD_STORAGE_ACCOUNT_NAME` and `MCD_STORAGE_BUCKET_NAME`
    environment variables.
    For authentication a `DefaultAzureCredential` object is used which requires the Azure Function to be running
    with a managed identity. If the identity is user-managed (instead of system-managed) the env variable
    AZURE_CLIENT_ID needs to be set with the client-id from the identity.
    Additionally, the identity needs to have access to the storage account, for example by having the
    `Storage Blob Data Contributor` role assigned at the storage account level.
    For checking if public access is disabled to the container, we need to authenticate with a shared key
    and thus the identity needs to have the `Storage Account Key Operator Service Role` role assigned, also at the
    storage account level.
    """

    def __init__(self, prefix: Optional[str] = None, **kwargs):  # type: ignore
        bucket_name = os.getenv(STORAGE_BUCKET_NAME_ENV_VAR)
        if not bucket_name:
            raise AgentConfigurationError(
                f"Bucket not configured, {STORAGE_BUCKET_NAME_ENV_VAR} env var expected"
            )
        self._account_name = os.getenv(STORAGE_ACCOUNT_NAME_ENV_VAR, "")
        if not self._account_name:
            raise AgentConfigurationError(
                f"Storage account not configured, {STORAGE_ACCOUNT_NAME_ENV_VAR} env var expected"
            )

        self._account_url = f"https://{self._account_name}.blob.core.windows.net"
        super().__init__(
            bucket_name=bucket_name,
            prefix=prefix,
            account_url=self._account_url,
            credential=AzureUtils.get_default_credential(),
            **kwargs,
        )
        self.ensure_container_exists()

    def _generate_sas_token(
        self, blob_client: BlobClient, expiry: datetime, permission: BlobSasPermissions
    ):
        # the code in super() uses the account_key from the credentials, as we're using
        # a token here we need to pass a user_delegation_key
        return generate_blob_sas(
            account_name=self._account_name,
            user_delegation_key=self._client.get_user_delegation_key(
                key_start_time=datetime.utcnow(),
                key_expiry_time=expiry,
            ),
            container_name=blob_client.container_name,
            blob_name=blob_client.blob_name,
            expiry=expiry,
            permission=permission,
        )

    def _get_client_to_get_access_policy(self) -> BlobServiceClient:
        # the client created with a token cannot be used to get the access policy according to:
        # https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-azure-active-directory#
        # permissions-for-blob-service-operations ("Get Container ACL" not supported).

        # first get the shared keys for the storage account, this requires one of
        # "Storage Account Key Operator Service Role" or "Storage Account Contributor" roles.
        st_client = self._get_storage_management_client()
        resource_group = AzureUtils.get_resource_group()
        keys = st_client.storage_accounts.list_keys(
            resource_group_name=resource_group,
            account_name=self._account_name,
        )

        # now create a new BlobServiceClient with the first key
        key: str = keys.keys[0].value  # type: ignore
        return BlobServiceClient(
            self._account_url,
            {
                "account_name": self._account_name,
                "account_key": key,
            },
        )

    @classmethod
    def _get_storage_management_client(cls):
        # this code requires AZURE_CLIENT_ID to be set if a user-managed identity is used
        return StorageManagementClient(
            AzureUtils.get_default_credential(), AzureUtils.get_subscription_id()
        )
