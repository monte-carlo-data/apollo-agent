import gzip
from abc import abstractmethod
from dataclasses import (
    dataclass,
    field,
)
from datetime import timedelta
from typing import List, Dict, Optional, Union, Tuple, Any

from botocore.exceptions import ClientError
from dataclasses_json import (
    DataClassJsonMixin,
    LetterCase,
    config,
    dataclass_json,
)

from apollo.integrations.storage.base_storage_client import BaseStorageClient

_ACL_GRANTEE_TYPE_GROUP = "Group"
_ACL_GRANTEE_URI_ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"
_ACL_GRANTEE_URI_AUTH_USERS = (
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)
_ACL_GRANTEE_PUBLIC_GROUPS = [_ACL_GRANTEE_URI_ALL_USERS, _ACL_GRANTEE_URI_AUTH_USERS]


@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class S3PublicAccessBlockConfiguration(DataClassJsonMixin):
    ignore_public_acls: bool
    restrict_public_buckets: bool


@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class S3PolicyStatus(DataClassJsonMixin):
    is_public: bool


@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class S3AclGrantee(DataClassJsonMixin):
    type: str
    uri: Optional[str] = field(metadata=config(field_name="URI"), default=None)


@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class S3AclGrant(DataClassJsonMixin):
    grantee: S3AclGrantee


@dataclass_json(letter_case=LetterCase.PASCAL)
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
    ):
        self._bucket_name = bucket_name

    @property
    @abstractmethod
    def s3_client(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def s3_resource(self):
        raise NotImplementedError()

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def write(self, key: str, obj_to_write) -> None:
        try:
            self.s3_client.put_object(
                Bucket=self._bucket_name,
                Key=key,
                Body=obj_to_write,
                ServerSideEncryption="AES256",
            )
        except ClientError as e:
            raise self.GenericError(str(e)) from e

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

        Return a bytes object, unless encoding is set, in which case it returns a string.
        """
        try:
            retrieved_obj = self.s3_client.get_object(Bucket=self._bucket_name, Key=key)
            content = retrieved_obj["Body"].read()
            if decompress and self._is_gzip(content):
                content = gzip.decompress(content)
            if encoding is not None:
                content = content.decode(encoding)
            return content
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise self.NotFoundError(str(e)) from e
            raise self.GenericError(str(e)) from e

    def read_many_json(self, prefix: str) -> Dict:
        temp_dict = {}
        for config_file_obj in self.s3_client.list_objects(self._bucket_name).filter(
            Prefix=prefix
        ):
            temp_dict[config_file_obj.key] = self.read_json(config_file_obj.key)
        return temp_dict

    def download_file(self, key: str, download_path: str) -> None:
        self.s3_resource.meta.client.download_file(
            self._bucket_name, key, download_path
        )

    def managed_download(self, key: str, download_path: str):
        # performs a managed transfer that might be multipart
        with open(download_path, "wb") as data:
            self.s3_client.download_fileobj(self._bucket_name, key, data)

    def delete(self, key: str) -> None:
        try:
            self.s3_client.delete_object(Bucket=self._bucket_name, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise self.NotFoundError(str(e)) from e
            raise self.GenericError(str(e)) from e

    def list_objects(
        self,
        prefix: Optional[str] = None,
        batch_size: Optional[int] = None,
        continuation_token: Optional[str] = None,
        delimiter: Optional[str] = None,
        *args,
        **kwargs,
    ) -> Tuple[Union[List, None], Union[str, None]]:
        params_dict = {"Bucket": self._bucket_name}
        if prefix:
            params_dict["Prefix"] = prefix
        if delimiter:
            params_dict["Delimiter"] = delimiter
        if batch_size:
            params_dict["MaxKeys"] = batch_size
        if continuation_token:
            params_dict["ContinuationToken"] = continuation_token

        try:
            objects_dict = self.s3_client.list_objects_v2(**params_dict)
            # specifying a deliminator results in a common prefix collection rather than any
            # contents but, can be utilized to roll up "subfolders"
            return (
                objects_dict.get("CommonPrefixes")
                if delimiter
                else objects_dict.get("Contents"),
                objects_dict.get("NextContinuationToken"),
            )
        except ClientError as e:
            raise self.GenericError(str(e)) from e

    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        try:
            return self.s3_client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": self._bucket_name,
                    "Key": key,
                },
                ExpiresIn=expiration.total_seconds(),
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise self.NotFoundError(str(e)) from e
            raise self.GenericError(str(e)) from e

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
        """

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
