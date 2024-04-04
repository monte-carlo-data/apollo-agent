import gzip
import logging
from abc import abstractmethod
from dataclasses import (
    dataclass,
    field,
)
from datetime import timedelta
from functools import wraps
from typing import List, Dict, Optional, Union, Tuple, Any, Callable

from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from dataclasses_json import (
    DataClassJsonMixin,
    LetterCase,
    config,
    dataclass_json,
)

from apollo.integrations.storage.base_storage_client import BaseStorageClient
from apollo.interfaces.lambda_function.aws_utils import get_boto_config

_ACL_GRANTEE_TYPE_GROUP = "Group"
_ACL_GRANTEE_URI_ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"
_ACL_GRANTEE_URI_AUTH_USERS = (
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)
_ACL_GRANTEE_PUBLIC_GROUPS = [_ACL_GRANTEE_URI_ALL_USERS, _ACL_GRANTEE_URI_AUTH_USERS]

logger = logging.getLogger(__name__)


def convert_s3_errors(func: Callable):
    """
    Decorator used to convert S3 specific errors into BaseStorageClient errors
    """

    @wraps(func)
    def _impl(*args, **kwargs):  # type: ignore
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise BaseStorageClient.NotFoundError(str(e)) from e
            raise BaseStorageClient.GenericError(str(e)) from e

    return _impl


@dataclass_json(letter_case=LetterCase.PASCAL)  # type: ignore
@dataclass
class S3PublicAccessBlockConfiguration(DataClassJsonMixin):
    ignore_public_acls: bool
    restrict_public_buckets: bool


@dataclass_json(letter_case=LetterCase.PASCAL)  # type: ignore
@dataclass
class S3PolicyStatus(DataClassJsonMixin):
    is_public: bool


@dataclass_json(letter_case=LetterCase.PASCAL)  # type: ignore
@dataclass
class S3AclGrantee(DataClassJsonMixin):
    type: str
    uri: Optional[str] = field(metadata=config(field_name="URI"), default=None)


@dataclass_json(letter_case=LetterCase.PASCAL)  # type: ignore
@dataclass
class S3AclGrant(DataClassJsonMixin):
    grantee: S3AclGrantee


@dataclass_json(letter_case=LetterCase.PASCAL)  # type: ignore
@dataclass
class S3Acls(DataClassJsonMixin):
    grants: Optional[List[S3AclGrant]] = None


class S3BaseReaderWriter(BaseStorageClient):
    """
    The base class for the S3 storage client, a subclass must provide the S3 client and resource by
    implementing the abstract properties: `s3_client` and `s3_resource`.
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
    ):
        """
        Creates a new base storage client for S3.
        :param bucket_name: the name of the bucket to use, all operations will be performed in this bucket.
        :param prefix: if present it will be prefixed to all file operations and will be removed from result paths.
        """
        super().__init__(prefix=prefix)
        self._bucket_name = bucket_name

    @property
    @abstractmethod
    def s3_client(self):
        """
        Needs to be implemented by subclasses to provide a client for S3, for example: `boto3.client("s3")`
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def s3_regional_client(self):
        """
        Needs to be implemented by subclasses to provide a client for S3 initialized with
        the regional endpoint, required for pre-signed urls, see:
        https://github.com/boto/boto3/issues/3015
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def s3_resource(self):
        """
        Needs to be implemented by subclasses to provide a client for S3, for example: `boto3.resource("s3")`
        """
        raise NotImplementedError()

    def _get_s3_client_with_config(self, config: Config) -> BaseClient:
        """
        Returns a client for S3 with the provided configuration, used to create clients
        with a different connect_timeout and max_attempts when testing connectivity to
        S3 endpoints.
        """
        raise NotImplementedError()

    @property
    def bucket_name(self) -> str:
        """
        Returns the bucket name referenced by this client
        """
        return self._bucket_name

    def write(self, key: str, obj_to_write: Union[bytes, str]) -> None:
        """
        Writes a file in the given key, contents are included as bytes or string.
        :param key: path to the file, for example /dir/name.ext
        :param obj_to_write: contents for the file, specified as a bytes array or string
        """
        try:
            self.s3_client.put_object(
                Bucket=self._bucket_name,
                Key=self._apply_prefix(key),
                Body=obj_to_write,
                ServerSideEncryption="AES256",
            )
        except ClientError as e:
            raise self.GenericError(str(e)) from e

    @convert_s3_errors
    def read(
        self,
        key: str,
        decompress: Optional[bool] = False,
        encoding: Optional[str] = None,
    ) -> Union[bytes, str]:
        """
        Read a file from S3.

        Decompress the file if the "decompress" flag is set and the file is in a known compressed
        format (currently only GZIP is supported). If the file is not compressed, return the
        content.

        :param key: path to the file, for example /dir/name.ext
        :param decompress: flag indicating if `gzip` contents should be decompressed automatically
        :param encoding: if set binary content will be decoded using this encoding and a string will be returned
        :return: a bytes object, unless encoding is set, in which case it returns a string.
        """
        retrieved_obj = self.s3_client.get_object(
            Bucket=self._bucket_name, Key=self._apply_prefix(key)
        )
        content = retrieved_obj["Body"].read()
        if decompress and self._is_gzip(content):
            content = gzip.decompress(content)
        if encoding is not None:
            content = content.decode(encoding)
        return content

    def read_many_json(self, prefix: str) -> Dict:
        """
        Reads all JSON files under `prefix` and returns a dictionary where the key is the file path and the value
        is the dictionary loaded from the JSON file.
        :param prefix: Prefix for the files to load, for example: `/dir/`
        :return: a dictionary where the key is the file path and the value is the dictionary loaded from the JSON file.
        """
        temp_dict = {}
        for config_file_obj in self.s3_resource.Bucket(
            self._bucket_name
        ).objects.filter(Prefix=self._apply_prefix(prefix)):
            key = self._remove_prefix(config_file_obj.key)
            if not key or key.endswith("/"):
                continue
            temp_dict[key] = self.read_json(key)
        return temp_dict

    @convert_s3_errors
    def download_file(self, key: str, download_path: str) -> None:
        """
        Downloads the file at `key` to the local file indicated by `download_path`.
        :param key: path to the file, for example /dir/name.ext
        :param download_path: local path to the file where the contents of `key` will be stored.
        """
        self.s3_resource.meta.client.download_file(
            self._bucket_name, self._apply_prefix(key), download_path
        )

    def upload_file(self, key: str, local_file_path: str) -> None:
        """
        Uploads the file at `local_file_path` to `key` in the associated bucket.
        :param key: path to the file, for example /dir/name.ext
        :param local_file_path: local path to the file to upload.
        """
        self.s3_client.upload_file(
            local_file_path, self._bucket_name, self._apply_prefix(key)
        )

    @convert_s3_errors
    def managed_download(self, key: str, download_path: str):
        """
        Performs a managed transfer that might be multipart, downloads the file at `key` to the local file at
        `download_path`.
        :param key: path to the file, for example /dir/name.ext
        :param download_path: local path to the file where the contents of `key` will be stored.
        """
        with open(download_path, "wb") as data:
            self.s3_client.download_fileobj(
                self._bucket_name, self._apply_prefix(key), data
            )

    @convert_s3_errors
    def delete(self, key: str) -> None:
        """
        Deletes the file at `key`
        :param key: path to the file, for example /dir/name.ext
        """
        self.s3_client.delete_object(
            Bucket=self._bucket_name, Key=self._apply_prefix(key)
        )

    @convert_s3_errors
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
        Delimiter is set to "/" to return sub-folders, documentation about delimiter in S3 requests available here:
        https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_RequestSyntax
        Prefix can be used to return contents of folders.
        :param prefix: Prefix to use for listing, it can be used to list folders, for example: `prefix=/dir/`
        :param batch_size: Used to page the result
        :param continuation_token: Used to page the result, the second value in the resulting tuple is the continuation
            token for the next call.
        :param delimiter: Set to "/" to return sub-folders, when set the result will include the list of prefixes
            returned by S3 instead of metadata for the objects.
        :return: A tuple with the result list and the continuation token. The result list includes the following
            attributes (when no delimiter is set): ETag, Key, Size, LastModified, StorageClass. If delimiter is
            specified only Prefix is included in the result for each listed folder.
        """
        params_dict: Dict[str, Any] = {"Bucket": self._bucket_name}
        if prefix or self._prefix:
            params_dict["Prefix"] = self._apply_prefix(prefix)
        if delimiter:
            params_dict["Delimiter"] = delimiter
        if batch_size:
            params_dict["MaxKeys"] = batch_size
        if continuation_token:
            params_dict["ContinuationToken"] = continuation_token

        objects_dict = self.s3_client.list_objects_v2(**params_dict)
        # specifying a delimiter results in a common prefix collection rather than any
        # contents but, can be utilized to roll up "sub-folders"
        return (
            (
                self._remove_prefix_from_prefixes(objects_dict.get("CommonPrefixes"))
                if delimiter
                else self._remove_prefix_from_entries(objects_dict.get("Contents"))
            ),
            objects_dict.get("NextContinuationToken"),
        )

    @convert_s3_errors
    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        """
        Generates a pre-signed url for the given file with the specified expiration.
        :param key: path to the file, for example /dir/name.ext
        :param expiration: time for the generated link to expire, expressed as a timedelta object.
        :return: a pre-signed url to access the specified file.
        """
        return self.s3_regional_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self._bucket_name,
                "Key": self._apply_prefix(key),
            },
            ExpiresIn=int(expiration.total_seconds()),
        )

    @convert_s3_errors
    def check_storage_access(self):
        # this method is intended to check connectivity with S3 endpoints
        # when the agent is configured in a VPC with no external access (and without
        # the required VPC endpoints) it takes minutes to time out
        # this method performs a head_bucket operation in the configured bucket
        # (with 10 seconds connection timeout) just to confirm we're able to access
        # S3 endpoints
        logger.info("Checking storage access")
        self._get_s3_client_with_config(
            config=get_boto_config(connect_timeout=10, max_attempts=1)
        ).head_bucket(BucketName=self._bucket_name)
        logger.info("Storage access checked")

    def is_bucket_private(self) -> bool:
        """
        Read about the "meaning of public" here:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html
        Based on the definition there, we follow these steps to check if a bucket is public:

        1) we get the PublicAccessBlockConfiguration

        2) if PublicAccessBlockConfiguration.IgnorePublicAcls=false, this means that ACLs giving
           public access won't be ignored, and they must be considered. So, we request the acls and
           check for public access as defined in that page (if AllUsers or AuthenticatedUsers are
           granted access)

        3) if PublicAccessBlockConfiguration.RestrictPublicBuckets=false, this means that a
           Bucket Policy granting public access won't be ignored, so we need to check the Policy
           Status for the bucket (that takes care of implementing the logic described on the page),
           if PolicyStatus.IsPublic=true then the bucket is public.

        4) If both PublicAccessBlockConfiguration.IgnorePublicAcls and
           PublicAccessBlockConfiguration.RestrictPublicBuckets are true, then we know the bucket is
           private. Please consider the result in PublicAccessBlockConfiguration takes into account
           both the settings for the account and the bucket, so no need to check for the account
           settings.

        See: https://docs.aws.amazon.com/cli/latest/reference/s3api/get-public-access-block.html
        :return: True if public access is disabled for the bucket and False if the bucket is publicly available.
        """

        # as is_bucket_private is the first operation called when validating
        # storage access from the DC, we're calling check_storage_access
        # here to check connectivity to S3 endpoints without having to
        # migrate DCs
        self.check_storage_access()

        public_access_block = self._get_public_access_block()
        if not public_access_block:
            return False

        acls_private = public_access_block.ignore_public_acls  # public acls are ignored
        policy_private = (
            public_access_block.restrict_public_buckets
        )  # public policy is ignored
        if not acls_private:  # public acls not ignored, check if there's any public acl
            acls = self._get_bucket_acls()
            if acls is None or not self._contains_public_acl(acls):
                acls_private = True

        if (
            not policy_private
        ):  # public policy is not ignored, check if policy is public
            policy_status = self._get_bucket_policy_status()
            if policy_status is None or not policy_status.is_public:
                policy_private = True

        return acls_private and policy_private

    def _get_public_access_block(self) -> Optional[S3PublicAccessBlockConfiguration]:
        try:
            response = self.s3_client.get_public_access_block(Bucket=self._bucket_name)
            public_access_block_dict = response.get(
                "PublicAccessBlockConfiguration", {}
            )
            return S3PublicAccessBlockConfiguration.from_dict(public_access_block_dict)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration":
                return None
            raise self.GenericError(str(e)) from e

    def _get_bucket_acls(self) -> Optional[S3Acls]:
        try:
            return S3Acls.from_dict(
                self.s3_client.get_bucket_acl(Bucket=self._bucket_name) or {}
            )
        except ClientError as e:
            raise self.GenericError(str(e)) from e

    def _get_bucket_policy_status(self) -> Optional[S3PolicyStatus]:
        """
        The implementation of get_bucket_policy_status takes care of checking what's described in
        the "Meaning of Public" doc for bucket policies and returns a flag indicating if the
        bucket is public or not. We use get_bucket_policy_status to check for public buckets when
        PublicAccessBlockConfiguration is not configured to ignore public policies.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_policy_status.html
        """
        try:
            response = (
                self.s3_client.get_bucket_policy_status(Bucket=self._bucket_name) or {}
            )
            return S3PolicyStatus.from_dict(response.get("PolicyStatus", {}))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
                return None
            raise self.GenericError(str(e)) from e

    @classmethod
    def _contains_public_acl(cls, acls: S3Acls) -> bool:
        """
        Amazon S3 considers a bucket or object ACL public if it grants any permissions to members
        of the predefined AllUsers or AuthenticatedUsers groups.

        https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html
        """
        if acls.grants:
            return any(cls._is_public_grant(grant) for grant in acls.grants)
        return False

    @classmethod
    def _is_public_grant(cls, grant: S3AclGrant) -> bool:
        if grant.grantee.type == _ACL_GRANTEE_TYPE_GROUP:
            return grant.grantee.uri in _ACL_GRANTEE_PUBLIC_GROUPS
        return False
