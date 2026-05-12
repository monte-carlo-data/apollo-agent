import os
from datetime import datetime
from typing import Optional, cast

from azure.identity import ClientSecretCredential, DefaultAzureCredential
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
    AUTH_TYPE_AZURE_SERVICE_PRINCIPAL,
    AZURE_STORAGE_AUTH_TYPE_ENV_VAR,
    AZURE_SP_TENANT_ID_ENV_VAR,
    AZURE_SP_CLIENT_ID_ENV_VAR,
    AZURE_SP_CLIENT_SECRET_ENV_VAR,
    AZURE_STORAGE_ACCOUNT_URL_ENV_VAR,
)
from apollo.common.agent.models import AgentConfigurationError
from apollo.integrations.azure_blob.azure_blob_base_reader_writer import (
    AzureBlobBaseReaderWriter,
)
from apollo.integrations.azure_blob.utils import AzureUtils

_WRAPPER_TYPE_KUBERNETES = "KUBERNETES"


class AzureBlobReaderWriter(AzureBlobBaseReaderWriter):
    """
    Azure Storage client implementation used in the agent, it initializes the client using the
    account and container names specified through `MCD_STORAGE_ACCOUNT_NAME` and `MCD_STORAGE_BUCKET_NAME`
    environment variables.

    Supports two authentication modes:
    - **Managed identity (default):** Uses `DefaultAzureCredential`, requires the Azure Function to run
      with a managed identity. If the identity is user-managed, set `AZURE_CLIENT_ID`.
    - **Service principal:** When `MCD_AZURE_STORAGE_AUTH_TYPE` is set to `"service_principal"`,
      uses `ClientSecretCredential` with tenant_id, client_id, and client_secret from env vars.

    The identity needs access to the storage account, for example by having the
    `Storage Blob Data Contributor` role assigned at the storage account level.
    For managed identity, the `Storage Account Key Operator Service Role` is also needed for the
    bucket privacy check. For service principal auth the privacy check is skipped.
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

        self._auth_type = os.getenv(AZURE_STORAGE_AUTH_TYPE_ENV_VAR, "")

        if self._auth_type == AUTH_TYPE_AZURE_SERVICE_PRINCIPAL:
            credential = self._build_service_principal_credential()
            self._account_url = self._resolve_account_url()
        else:
            credential = AzureUtils.get_default_credential()
            self._account_url = f"https://{self._account_name}.blob.core.windows.net"

        super().__init__(
            bucket_name=bucket_name,
            prefix=prefix,
            account_url=self._account_url,
            credential=credential,
            **kwargs,
        )

    def _build_service_principal_credential(self) -> ClientSecretCredential:
        required_vars = {
            AZURE_SP_TENANT_ID_ENV_VAR: os.getenv(AZURE_SP_TENANT_ID_ENV_VAR, ""),
            AZURE_SP_CLIENT_ID_ENV_VAR: os.getenv(AZURE_SP_CLIENT_ID_ENV_VAR, ""),
            AZURE_SP_CLIENT_SECRET_ENV_VAR: os.getenv(
                AZURE_SP_CLIENT_SECRET_ENV_VAR, ""
            ),
        }
        for env_var, value in required_vars.items():
            if not value:
                raise AgentConfigurationError(
                    f"Service principal auth requires {env_var} env var"
                )
        return ClientSecretCredential(
            tenant_id=required_vars[AZURE_SP_TENANT_ID_ENV_VAR],
            client_id=required_vars[AZURE_SP_CLIENT_ID_ENV_VAR],
            client_secret=required_vars[AZURE_SP_CLIENT_SECRET_ENV_VAR],
        )

    def _resolve_account_url(self) -> str:
        account_url = os.getenv(AZURE_STORAGE_ACCOUNT_URL_ENV_VAR, "")
        if account_url:
            if not account_url.startswith("https://"):
                raise AgentConfigurationError(
                    f"{AZURE_STORAGE_ACCOUNT_URL_ENV_VAR} must use https:// scheme"
                )
            return account_url
        return f"https://{self._account_name}.blob.core.windows.net"

    def is_bucket_private(self) -> bool:
        # For Kubernetes deployments and service principal auth we don't have access to
        # the storage management API, so we skip the check and assume the container is private.
        if os.getenv(AGENT_WRAPPER_TYPE_ENV_VAR) == _WRAPPER_TYPE_KUBERNETES:
            return True
        if self._auth_type == AUTH_TYPE_AZURE_SERVICE_PRINCIPAL:
            return True
        return super().is_bucket_private()

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
