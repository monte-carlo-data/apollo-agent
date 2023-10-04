import gzip
import os
from datetime import timedelta
from typing import List, Dict, Optional, Union, Tuple

from google.api_core.exceptions import (
    ClientError,
    Forbidden,
    NotFound,
)
from google.cloud.storage import (
    Bucket,
    Client,
    transfer_manager,
)
from google.oauth2.service_account import Credentials

from apollo.integrations.storage.base_storage_client import BaseStorageClient

CONFIGURATION_BUCKET = os.getenv("CONFIGURATION_BUCKET", "data-collector-configuration")


class GcsReaderWriter(BaseStorageClient):
    def __init__(self, credentials: Optional[Dict], **kwargs):
        gcs_credentials = (
            Credentials.from_service_account_info(credentials) if credentials else None
        )
        self._bucket_name = CONFIGURATION_BUCKET
        self._client = Client(credentials=gcs_credentials)

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def write(self, key: str, obj_to_write) -> None:
        try:
            bucket: Bucket = self._client.get_bucket(self._bucket_name)
            bucket.blob(key).upload_from_string(obj_to_write)
        except Forbidden as e:
            raise self.PermissionsError(extract_error_message(e)) from e

    def read(
        self,
        key: str,
        decompress: Optional[bool] = False,
        encoding: Optional[str] = None,
    ) -> Union[bytes, str]:
        try:
            bucket: Bucket = self._client.get_bucket(self._bucket_name)
            content = bucket.blob(blob_name=key).download_as_bytes()
            if decompress and self._is_gzip(content):
                content = gzip.decompress(content)
            if encoding is not None:
                content = content.decode(encoding)
            return content
        except NotFound as e:
            raise self.NotFoundError(extract_error_message(e)) from e
        except Forbidden as e:
            raise self.PermissionsError(extract_error_message(e)) from e

    def read_many_json(self, prefix: str) -> Dict:
        temp_dict = {}
        bucket: Bucket = self._client.get_bucket(self._bucket_name)
        for blob in bucket.list_blobs(prefix=prefix):
            temp_dict[blob.name] = self.read_json(blob.name)
        return temp_dict

    def download_file(self, key: str, download_path: str) -> None:
        try:
            bucket: Bucket = self._client.get_bucket(self._bucket_name)
            bucket.blob(key).download_to_filename(download_path)
        except NotFound as e:
            raise self.NotFoundError(extract_error_message(e)) from e
        except Forbidden as e:
            raise self.PermissionsError(extract_error_message(e)) from e

    def managed_download(self, key: str, download_path: str):
        # performs a managed transfer that might be multipart
        try:
            bucket: Bucket = self._client.get_bucket(self._bucket_name)
            blob = bucket.blob(key)
            transfer_manager.download_chunks_concurrently(
                blob=blob, filename=download_path
            )
        except NotFound as e:
            raise self.NotFoundError(extract_error_message(e)) from e
        except Forbidden as e:
            raise self.PermissionsError(extract_error_message(e)) from e

    def delete(self, key: str) -> None:
        try:
            bucket: Bucket = self._client.get_bucket(self._bucket_name)
            bucket.blob(key).delete()
        except NotFound as e:
            raise self.NotFoundError(extract_error_message(e)) from e
        except Forbidden as e:
            raise self.PermissionsError(extract_error_message(e)) from e

    def list_objects(
        self,
        prefix: Optional[str] = None,
        batch_size: Optional[int] = None,
        continuation_token: Optional[str] = None,
        delimiter: Optional[str] = None,
        *args,
        **kwargs,
    ) -> Tuple[Union[List, None], Union[str, None]]:
        params_dict = {"bucket_or_name": self._bucket_name}
        if prefix:
            params_dict["prefix"] = prefix
        if delimiter:
            params_dict["delimiter"] = delimiter
        if batch_size:
            params_dict["max_results"] = batch_size
        if continuation_token:
            params_dict["page_token"] = continuation_token

        try:
            # The way list_blobs() work is that it returns an iterator that iterates through all the
            # results doing paging behind the scenes.
            # The resulting iterator iterates through multiple pages. What page_token actually
            # represents is which page the iterator should START at. It no page_token is provided it
            # will start at the first page.
            iterator = self._client.list_blobs(**params_dict)
            page = next(iterator.pages)

            # specifying a deliminator results in a common prefix collection rather than any
            # contents but, can be utilized to roll up "sub-folders"
            return (
                page.prefixes if delimiter else [blob.name for blob in list(page)],
                iterator.next_page_token,
            )
        except Forbidden as e:
            raise self.PermissionsError(extract_error_message(e)) from e

    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        try:
            bucket: Bucket = self._client.get_bucket(self._bucket_name)
            blob = bucket.get_blob(blob_name=key)
            if not blob:
                raise self.NotFoundError(f"blob with key {key} does not exist")
            return blob.generate_signed_url(expiration=expiration)
        except Forbidden as e:
            raise self.PermissionsError(extract_error_message(e)) from e

    def is_bucket_private(self) -> bool:
        """
        Checks if the bucket has public access prevention according to definitions in:
        https://cloud.google.com/storage/docs/public-access-prevention.

        :return: True if public access prevention is on and False otherwise.
        """
        try:
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
        except Forbidden as e:
            raise self.PermissionsError(extract_error_message(e)) from e


def extract_error_message(error: ClientError) -> str:
    try:
        payload = error.response.json()
    except ValueError:
        payload = {"error": {"message": error.response.text or "unknown error"}}

    return payload.get("error", {}).get("message", "unknown error")
