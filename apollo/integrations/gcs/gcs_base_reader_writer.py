import gzip
from datetime import timedelta
from functools import wraps
from typing import List, Dict, Optional, Union, Tuple

from google.api_core.exceptions import (
    ClientError,
    Forbidden,
    NotFound,
)
from google.auth.compute_engine import IDTokenCredentials
from google.auth.transport.requests import Request
from google.cloud.storage import (
    Bucket,
    Client,
    transfer_manager,
)
from google.oauth2.service_account import Credentials
from apollo.integrations.storage.base_storage_client import BaseStorageClient


def _extract_error_message(error: ClientError) -> str:
    try:
        payload = error.response.json()
    except ValueError:
        payload = {"error": {"message": error.response.text or "unknown error"}}

    return payload.get("error", {}).get("message", "unknown error")


def convert_gcs_errors(func):
    """
    Decorator used to convert GCS specific errors into BaseStorageClient errors
    """

    @wraps(func)
    def _impl(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NotFound as e:
            raise BaseStorageClient.NotFoundError(_extract_error_message(e)) from e
        except Forbidden as e:
            raise BaseStorageClient.PermissionsError(_extract_error_message(e)) from e

    return _impl


class GcsBaseReaderWriter(BaseStorageClient):
    """
    Base class implementing a storage client for GCS (Google Cloud Storage), the class must be initialized with a
    required bucket name and an optional Credentials object.
    If no credentials are specified the default credentials from the environment (called ADC) will be used.
    All operations in this class, like `read`, `write` or `delete` are relative to the bucket specified by
    `bucket_name` in the constructor.
    """

    def __init__(
        self,
        bucket_name: str,
        credentials: Optional[Credentials] = None,
        **kwargs,
    ):
        self._bucket_name = bucket_name
        self._using_default_credentials = credentials is None
        self._client = Client(credentials=credentials)

    @property
    def bucket_name(self) -> str:
        """
        Returns the bucket name referenced by this client
        """
        return self._bucket_name

    @convert_gcs_errors
    def write(self, key: str, obj_to_write: Union[bytes, str]) -> None:
        """
        Writes a file in the given key, contents are included as bytes or string.
        :param key: path to the file, for example /dir/name.ext
        :param obj_to_write: contents for the file, specified as a bytes array or string
        """
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        bucket.blob(key).upload_from_string(obj_to_write)

    @convert_gcs_errors
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
        :param encoding: if set binary content will be decoded using this encoding and a string will be returned
        :return: a bytes object, unless encoding is set, in which case it returns a string.
        """
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        content = bucket.blob(blob_name=key).download_as_bytes()
        if decompress and self._is_gzip(content):
            content = gzip.decompress(content)
        if encoding is not None:
            content = content.decode(encoding)
        return content

    @convert_gcs_errors
    def read_many_json(self, prefix: str) -> Dict:
        """
        Reads all JSON files under `prefix` and returns a dictionary where the key is the file path and the value
        is the dictionary loaded from the JSON file.
        :param prefix: Prefix for the files to load, for example: `/dir/`
        :return: a dictionary where the key is the file path and the value is the dictionary loaded from the JSON file.
        """
        temp_dict = {}
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        for blob in bucket.list_blobs(prefix=prefix):
            temp_dict[blob.name] = self.read_json(blob.name)
        return temp_dict

    @convert_gcs_errors
    def download_file(self, key: str, download_path: str) -> None:
        """
        Downloads the file at `key` to the local file indicated by `download_path`.
        :param key: path to the file, for example /dir/name.ext
        :param download_path: local path to the file where the contents of `key` will be stored.
        """
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        bucket.blob(key).download_to_filename(download_path)

    @convert_gcs_errors
    def managed_download(self, key: str, download_path: str):
        """
        Performs a managed transfer that might be multipart, downloads the file at `key` to the local file at
        `download_path`.
        :param key: path to the file, for example /dir/name.ext
        :param download_path: local path to the file where the contents of `key` will be stored.
        """
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        blob = bucket.blob(key)
        transfer_manager.download_chunks_concurrently(blob=blob, filename=download_path)

    @convert_gcs_errors
    def delete(self, key: str) -> None:
        """
        Deletes the file at `key`
        :param key: path to the file, for example /dir/name.ext
        """
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        bucket.blob(key).delete()

    @convert_gcs_errors
    def list_objects(
        self,
        prefix: Optional[str] = None,
        batch_size: Optional[int] = None,
        continuation_token: Optional[str] = None,
        delimiter: Optional[str] = None,
        *args,
        **kwargs,
    ) -> Tuple[Union[List, None], Union[str, None]]:
        """
        List objects (files and folder) under the specified prefix.
        Delimiter is set to "/" to return sub-folders, documentation about delimiter in GCS requests available here:
        https://cloud.google.com/storage/docs/json_api/v1/objects/list
        Prefix can be used to return contents of folders.
        :param prefix: Prefix to use for listing, it can be used to list folders, for example: `prefix=/dir/`
        :param batch_size: Used to page the result
        :param continuation_token: Used to page the result, the second value in the resulting tuple is the continuation
            token for the next call.
        :param delimiter: Set to "/" to return sub-folders, when set the result will include the list of prefixes
            returned by GCS instead of metadata for the objects.
        :return: A tuple with the result list and the continuation token. The result list includes the following
            attributes (when no delimiter is set): ETag, Key, Size, LastModified, StorageClass. If delimiter is
            specified only Prefix is included in the result for each listed folder.
        """
        params_dict = {"bucket_or_name": self._bucket_name}
        if prefix:
            params_dict["prefix"] = prefix
        if delimiter:
            params_dict["delimiter"] = delimiter
        if batch_size:
            params_dict["max_results"] = batch_size
        if continuation_token:
            params_dict["page_token"] = continuation_token

        # The way list_blobs() work is that it returns an iterator that iterates through all the
        # results doing paging behind the scenes.
        # The resulting iterator iterates through multiple pages. What page_token actually
        # represents is which page the iterator should START at. It no page_token is provided it
        # will start at the first page.
        iterator = self._client.list_blobs(**params_dict)
        page = next(iterator.pages)

        # specifying a delimiter results in a common prefix collection rather than any
        # contents but, can be utilized to roll up "sub-folders"
        # we're returning the same exact format returned by S3ReaderWriter for compatibility reasons
        if delimiter:
            result_list = [{"Prefix": prefix} for prefix in page.prefixes]
        else:
            result_list = [
                {
                    "ETag": blob.etag,
                    "Key": blob.name,
                    "Size": blob.size,
                    "LastModified": blob.updated,
                    "StorageClass": blob.storage_class,
                }
                for blob in list(page)
            ]
        return result_list, iterator.next_page_token

    @convert_gcs_errors
    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        """
        Generates a pre-signed url for the given file with the specified expiration.
        :param key: path to the file, for example /dir/name.ext
        :param expiration: time for the generated link to expire, expressed as a timedelta object.
        :return: a pre-signed url to access the specified file.
        """
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        blob = bucket.get_blob(blob_name=key)
        if not blob:
            raise self.NotFoundError(f"blob with key {key} does not exist")
        if self._using_default_credentials:
            # workaround needed to sign urls when running in CloudRun, more information:
            # https://gist.github.com/jezhumble/91051485db4462add82045ef9ac2a0ec
            # https://github.com/googleapis/google-cloud-python/issues/922
            signing_credentials = IDTokenCredentials(Request(), "")
            return blob.generate_signed_url(
                expiration=expiration,
                credentials=signing_credentials,
            )
        else:
            return blob.generate_signed_url(expiration=expiration)

    @convert_gcs_errors
    def is_bucket_private(self) -> bool:
        """
        Checks if the bucket has public access prevention according to definitions in:
        https://cloud.google.com/storage/docs/public-access-prevention.

        :return: True if public access prevention is on and False otherwise.
        """
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        if not bucket.iam_configuration.uniform_bucket_level_access_enabled:
            # access is subject to object ACLs (fine-grained)
            return False
        if bucket.iam_configuration.public_access_prevention == "enforced":
            # public access is explicitly and uniformly prevented
            return True

        policy = bucket.get_iam_policy()
        for binding in policy.bindings:
            for member in binding["members"]:
                if member == "allUsers" or member == "allAuthenticatedUsers":
                    # public to the internet
                    return False
        return True
