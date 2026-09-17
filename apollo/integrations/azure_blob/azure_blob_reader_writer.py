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

from apollo.common.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_ACCOUNT_NAME_ENV_VAR,
    AGENT_WRAPPER_TYPE_ENV_VAR,
)
from apollo.common.agent.models import AgentConfigurationError
from apollo.integrations.azure_blob.azure_blob_base_reader_writer import (
    AzureBlobBaseReaderWriter,
)
from apollo.integrations.azure_blob.utils import AzureUtils

_WRAPPER_TYPE_KUBERNETES = "KUBERNETES"

# Connection string for the storage account, as labeled on the portal's "Access keys" blade. It
# carries the account name and a shared key, and is the only authentication mode that doesn't
# reach Entra, so it is the only option when the agent has no public internet egress.
_ENV_VAR_STORAGE_CONNECTION_STRING = "MCD_STORAGE_CONNECTION_STRING"


class AzureBlobReaderWriter(AzureBlobBaseReaderWriter):
    """
    Azure Storage client implementation used in the agent, it initializes the client using the
    container name specified through `MCD_STORAGE_BUCKET_NAME` and one of two authentication modes:

    - A connection string in `MCD_STORAGE_CONNECTION_STRING`, which carries the account name and a
      shared key. No Entra access is needed, so this is the mode for agents that can't reach
      `login.microsoftonline.com`.
    - A `DefaultAzureCredential` against the account named by `MCD_STORAGE_ACCOUNT_NAME`. This
      resolves a managed identity (set `AZURE_CLIENT_ID` when it is user-assigned) or a service
      principal (set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET`), the latter
      being the option for agents running outside Azure, where there is no IMDS. The identity needs
      the `Storage Blob Data Contributor` role at the storage account level. Checking whether public
      access to the container is disabled requires a shared key, so it additionally needs the
      `Storage Account Key Operator Service Role` role.
    """

    def __init__(self, prefix: Optional[str] = None, **kwargs):  # type: ignore
        bucket_name = os.getenv(STORAGE_BUCKET_NAME_ENV_VAR)
        if not bucket_name:
            raise AgentConfigurationError(
                f"Bucket not configured, {STORAGE_BUCKET_NAME_ENV_VAR} env var expected"
            )

        self._connection_string = os.getenv(_ENV_VAR_STORAGE_CONNECTION_STRING)
        if self._connection_string:
            # The account name comes from the connection string, so `MCD_STORAGE_ACCOUNT_NAME` is
            # not required. `_account_name` and `_account_url` are left unset: every method that
            # reads them authenticates with a token, and those all defer to the base class here.
            super().__init__(
                bucket_name=bucket_name,
                prefix=prefix,
                connection_string=self._connection_string,
                **kwargs,
            )
            return

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

    def is_bucket_private(self) -> bool:
        # for Kubernetes deployments we don't have access to the storage account key, so we
        # skip the check and assume the container is private
        if os.getenv(AGENT_WRAPPER_TYPE_ENV_VAR) == _WRAPPER_TYPE_KUBERNETES:
            return True
        return super().is_bucket_private()

    def _generate_sas_token(
        self, blob_client: BlobClient, expiry: datetime, permission: BlobSasPermissions
    ):
        if self._connection_string:
            # `from_connection_string` builds a shared-key credential, which is what super() signs
            # with.
            return super()._generate_sas_token(
                blob_client=blob_client, expiry=expiry, permission=permission
            )

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
        if self._connection_string:
            # The connection string already authenticates with a shared key, so the client can
            # read the container ACL without going through the management API.
            return super()._get_client_to_get_access_policy()

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
