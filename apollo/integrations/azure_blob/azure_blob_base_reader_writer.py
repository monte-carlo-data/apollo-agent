import gzip
import logging
from datetime import (
    datetime,
    timedelta,
)
from functools import wraps
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import urljoin

from azure.core.credentials import TokenCredential
from azure.core.exceptions import (
    ClientAuthenticationError,
    ResourceNotFoundError,
)
from azure.storage.blob import (
    BlobPrefix,
    BlobSasPermissions,
    BlobServiceClient,
    generate_blob_sas,
    BlobClient,
)

from apollo.integrations.storage.base_storage_client import BaseStorageClient

logger = logging.getLogger(__name__)


def convert_azure_errors(func: Callable):
    """
    Decorator used to convert Azure specific errors into BaseStorageClient errors
    """

    @wraps(func)
    def _impl(*args, **kwargs):  # type: ignore
        try:
            return func(*args, **kwargs)
        except ResourceNotFoundError as e:
            logger.exception(e)
            raise BaseStorageClient.NotFoundError(str(e)) from e
        except ClientAuthenticationError as e:
            raise BaseStorageClient.PermissionsError(str(e)) from e

    return _impl


class AzureBlobBaseReaderWriter(BaseStorageClient):
    """
    Base class implementing a storage client for Azure (Azure Storage Blob), the class must be
    initialized with a required bucket name and a required connection string.
    All operations in this class, like `read`, `write` or `delete` are relative to the bucket
    specified by `bucket_name` in the constructor.
    """

    def __init__(
        self,
        bucket_name: str,
        connection_string: str,
        prefix: Optional[str] = None,
        account_url: Optional[str] = None,
        credential: Optional[TokenCredential] = None,
        **kwargs,  # type: ignore
    ):
        super().__init__(prefix=prefix)
        self._bucket_name = bucket_name
        if account_url and credential:
            self._client = BlobServiceClient(account_url, credential)
        else:
            self._client = BlobServiceClient.from_connection_string(
                conn_str=connection_string
            )

    @property
    def bucket_name(self) -> str:
        """
        Returns the bucket name referenced by this client
        """
        return self._bucket_name

    @convert_azure_errors
    def write(self, key: str, obj_to_write: Union[bytes, str]) -> None:
        """
        Writes a file in the given key, contents are included as bytes or string.
        :param key: path to the file, for example /dir/name.ext
        :param obj_to_write: contents for the file, specified as a bytes array or string
        """
        container_client = self._client.get_container_client(self._bucket_name)
        container_client.upload_blob(
            name=self._apply_prefix(key), data=obj_to_write, overwrite=True  # type: ignore
        )

    @convert_azure_errors
    def read(
        self,
        key: str,
        decompress: Optional[bool] = False,
        encoding: Optional[str] = None,
    ) -> Union[bytes, str]:
        """
        Returns the contents of the specified file.
        :param key: path to the file, for example /dir/name.ext
        :param decompress: flag indicating if `gzip` contents should be decompressed automatically
        :param encoding: if set binary content will be decoded using this encoding and a string
            will be returned
        :return: a bytes object, unless encoding is set, in which case it returns a string.
        """
        blob_client = self._client.get_blob_client(
            container=self._bucket_name, blob=self._apply_prefix(key)  # type: ignore
        )
        downloader = blob_client.download_blob()
        content = downloader.readall()
        if decompress and self._is_gzip(content):
            content = gzip.decompress(content)
        if encoding is not None:
            content = content.decode(encoding)
        return content

    @convert_azure_errors
    def read_many_json(self, prefix: str) -> Dict:
        """
        Reads all JSON files under `prefix` and returns a dictionary where the key is the file path
        and the value is the dictionary loaded from the JSON file.
        :param prefix: Prefix for the files to load, for example: `/dir/`
        :return: a dictionary where the key is the file path and the value is the dictionary loaded
            from the JSON file.
        """
        temp_dict = {}
        container_client = self._client.get_container_client(self._bucket_name)
        for blob_name in container_client.list_blob_names(
            name_starts_with=self._apply_prefix(prefix)
        ):
            key = self._remove_prefix(blob_name)
            if not key or key.endswith("/"):  # root folder or sub-folder
                continue
            temp_dict[blob_name] = self.read_json(key)
        return temp_dict

    @convert_azure_errors
    def download_file(self, key: str, download_path: str) -> None:
        """
        Downloads the file at `key` to the local file indicated by `download_path`.
        :param key: path to the file, for example /dir/name.ext
        :param download_path: local path to the file where the contents of `key` will be stored.
        """
        blob_client = self._client.get_blob_client(
            container=self._bucket_name, blob=self._apply_prefix(key)  # type: ignore
        )
        with open(file=download_path, mode="wb") as stream:
            downloader = blob_client.download_blob()
            downloader.readinto(stream)

    @convert_azure_errors
    def upload_file(self, key: str, local_file_path: str) -> None:
        """
        Uploads the file at `local_file_path` to `key` in the associated bucket.
        :param key: path to the file, for example /dir/name.ext
        :param local_file_path: local path to the file to upload.
        """
        container_client = self._client.get_container_client(self._bucket_name)
        with open(file=local_file_path, mode="r") as blob:
            container_client.upload_blob(
                name=self._apply_prefix(key), data=blob, overwrite=True  # type: ignore
            )

    @convert_azure_errors
    def managed_download(self, key: str, download_path: str):
        """
        Performs a managed transfer that might be multipart, downloads the file at `key` to the
        local file at `download_path`.
        :param key: path to the file, for example /dir/name.ext
        :param download_path: local path to the file where the contents of `key` will be stored.
        """
        blob_client = self._client.get_blob_client(
            container=self._bucket_name, blob=self._apply_prefix(key)  # type: ignore
        )
        with open(file=download_path, mode="wb") as stream:
            downloader = blob_client.download_blob()
            downloader.readinto(stream)

    @convert_azure_errors
    def delete(self, key: str) -> None:
        """
        Deletes the file at `key`
        :param key: path to the file, for example /dir/name.ext
        """
        blob_client = self._client.get_blob_client(
            container=self._bucket_name, blob=self._apply_prefix(key)  # type: ignore
        )
        blob_client.delete_blob()

    @convert_azure_errors
    def list_objects(
        self,
        prefix: Optional[str] = None,
        batch_size: Optional[int] = None,
        continuation_token: Optional[str] = None,
        delimiter: Optional[str] = None,
        *args,  # type: ignore
        **kwargs,  # type: ignore
    ) -> Tuple[Union[List, None], Union[str, None]]:
        """
        List objects (files and folder) under the specified prefix.
        Delimiter is set to "/" to return sub-folders, documentation for delimiter in Azure Storage:
        https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-list-python#use-a-hierarchical-listing
        Prefix can be used to return contents of folders.
        :param prefix: Prefix to use for listing, it can be used to list folders,
            for example: `prefix=/dir/`
        :param batch_size: Used to page the result
        :param continuation_token: Used to page the result, the second value in the resulting tuple
            is the continuation token for the next call.
        :param delimiter: Set to "/" to return sub-folders, when set the result will include the
            list of prefixes returned by GCS instead of metadata for the objects.
        :return: A tuple with the result list and the continuation token. The result list includes
            the following attributes (when no delimiter is set): ETag, Key, Size, LastModified,
            StorageClass. If delimiter is specified only Prefix is included in the result for each
            listed folder.
        """
        params_dict = {}
        if prefix or self._prefix:
            params_dict["name_starts_with"] = self._apply_prefix(prefix)
        if batch_size:
            params_dict["results_per_page"] = batch_size

        container_client = self._client.get_container_client(self._bucket_name)

        if delimiter:
            # specifying a delimiter results in a common prefix collection rather than any
            # contents but, can be utilized to roll up "sub-folders"
            params_dict["delimiter"] = delimiter
            blobs = container_client.walk_blobs(**params_dict).by_page(
                continuation_token
            )
            page = next(blobs)
            return (
                self._remove_prefix_from_prefixes(
                    [
                        {"Prefix": blob.name}
                        for blob in page
                        if isinstance(blob, BlobPrefix)
                    ]
                ),
                blobs.continuation_token,  # type: ignore
            )

        blobs = container_client.list_blobs(**params_dict).by_page(continuation_token)
        page = next(blobs)
        result_list = self._remove_prefix_from_entries(
            [
                {
                    "ETag": blob.etag,
                    "Key": blob.name,
                    "Size": blob.size,
                    "LastModified": blob.last_modified,
                    "StorageClass": blob.blob_tier,
                }
                for blob in list(page)
            ]
        )
        return result_list, blobs.continuation_token  # type: ignore

    @convert_azure_errors
    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        """
        Generates a pre-signed url for the given file with the specified expiration.
        :param key: path to the file, for example /dir/name.ext
        :param expiration: time for the generated link to expire, expressed as a timedelta object.
        :return: a pre-signed url to access the specified file.
        """
        blob_client = self._client.get_blob_client(
            container=self._bucket_name, blob=self._apply_prefix(key)  # type: ignore
        )
        sas_token = self._generate_sas_token(
            blob_client=blob_client,
            expiry=datetime.utcnow() + expiration,
            permission=BlobSasPermissions(read=True),
        )
        return urljoin(blob_client.url, f"?{sas_token}")

    @convert_azure_errors
    def is_bucket_private(self) -> bool:
        """
        Checks if the bucket has public access disallowed according to definitions in:
        https://learn.microsoft.com/en-us/azure/storage/blobs/anonymous-read-access-configure?tabs=portal#about-anonymous-read-access.

        :return: True if public access is off and False otherwise.
        """

        container_client = self._client.get_container_client(self._bucket_name)
        access_policy = container_client.get_container_access_policy()
        return (
            "public_access" in access_policy and access_policy["public_access"] is None
        )

    def _generate_sas_token(
        self, blob_client: BlobClient, expiry: datetime, permission: BlobSasPermissions
    ):
        return generate_blob_sas(
            account_name=blob_client.credential.account_name,
            account_key=blob_client.credential.account_key,
            container_name=blob_client.container_name,
            blob_name=blob_client.blob_name,
            expiry=expiry,
            permission=permission,
        )
