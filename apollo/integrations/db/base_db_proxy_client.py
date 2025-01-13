import logging
import ssl
import tempfile
from abc import ABC
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Optional,
)
from urllib.request import urlretrieve

from apollo.agent.serde import AgentSerializer
from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.storage.base_storage_client import BaseStorageClient
from apollo.integrations.storage.storage_proxy_client import StorageProxyClient

logger = logging.getLogger(__name__)


@dataclass
class SslOptions:
    """
    Represents the SSL options for connecting to a database.

    This class contains various configuration options related to SSL certificates
    and verification that are used when establishing a secure connection to a database.

    For ca_data, cert_data and key_data the content of the certificate should be base64 encoded.
    Example of an expected format can be found here: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html#UsingWithRDS.SSL.CertificatesAllRegions

    Attributes:
        ca (str | None): The path to a CA PEM file that can be downloaded.
        ca_data (str | None): The certificate authority data for SSL verification.
        cert_data (str | None): The client certificate data for SSL connection.
        key_data (str | None): The private key data for SSL connection.
        key_password (str | None): The password for the private key (if applicable).
        skip_cert_verification (bool, optional): Whether to skip the validity of the certificate and identity of the server.
        verify_cert (bool, optional): Whether to verify the validity of the server SSL certificate.
        verify_identity (bool, optional): Whether to verify the identity of the remote server.
        disabled (bool, optional): Whether the SSL connection is disabled.
    """

    ca: str | None = None
    ca_data: str | None = None
    cert_data: str | None = None
    key_data: str | None = None
    key_password: str | None = None
    skip_cert_verification: bool = False
    verify_cert: bool = True
    verify_identity: bool = True
    disabled: bool = False
    mechanism: str | None = None

    def __post_init__(self):
        # Validation for conflicting flags
        if self.disabled and (self.cert_data or self.key_data or self.ca_data):
            raise ValueError("disabled is set, but SSL certificate data is provided.")

        if not self.disabled:
            if self.key_data and not self.cert_data:
                raise ValueError("key_data is provided, but ssl_cert_data is missing.")

            # If cert or key is provided, the ca data should also be available
            if (self.cert_data or self.key_data) and not self.ca_data:
                raise ValueError(
                    "cert_data or ssl_key_data is provided, but ca_data is missing"
                )

            # If the password is provided, at least the cert data must also be provided
            if self.key_password and (not self.cert_data):
                raise ValueError("key_password is provided but cert_data is missing.")

            # If a CA file path is provided, there should not also be CA data.
            if self.ca and self.ca_data:
                raise ValueError(
                    "Path to CA authority and CA authority data were provided. Provide one or the other."
                )

    def get_ssl_context(self) -> ssl.SSLContext | None:
        if self.disabled or not (self.cert_data or self.key_data or self.ca_data):
            return None

        ssl_context = ssl.create_default_context()
        ssl_context.verify_mode = (
            ssl.CERT_NONE
            if self.skip_cert_verification
            else (ssl.CERT_REQUIRED if self.verify_cert else ssl.CERT_NONE)
        )
        ssl_context.check_hostname = (
            not self.skip_cert_verification and self.verify_identity
        )

        if self.ca_data:
            ssl_context.load_verify_locations(cadata=self.ca_data)

        if self.cert_data:
            # We need to create temporary files for the certificate and key data because the function
            # `load_cert_chain` requires a file path as input and does not accept the data directly.
            # More info here: https://docs.python.org/3/library/ssl.html#ssl.SSLContext.load_cert_chain
            self._set_cert_and_key_to_context(ssl_context)

        return ssl_context

    def _set_cert_and_key_to_context(self, ssl_context: ssl.SSLContext) -> None:
        """Check if temp file exists, if not create it from certificate or key data."""
        with tempfile.NamedTemporaryFile(
            delete=True
        ) as cert_file, tempfile.NamedTemporaryFile(delete=True) as key_file:
            cert_file.write(self.cert_data.encode("ascii"))
            cert_file.flush()

            if self.key_data:
                key_file.write(self.key_data.encode("ascii"))
                key_file.flush()

            ssl_context.load_cert_chain(
                certfile=cert_file.name,
                keyfile=key_file.name if self.key_data else None,
                password=self.key_password,
            )


class BaseDbProxyClient(BaseProxyClient, ABC):
    def __init__(self, connection_type: str):
        self._connection = None
        self._connection_type = connection_type

    # On delete make sure we close the connection
    def __del__(self) -> None:
        self.close()

    def close(self):
        if self._connection:
            logger.info(f"Closing connection to {self._connection_type}")
            self._connection.close()
            self._connection = None

    def process_result(self, value: Any) -> Any:
        """
        Converts "Column" objects in the description into a list of objects that can be serialized to JSON.
        From the DBAPI standard, description is supposed to return tuples with 7 elements, so we're returning
        those 7 elements back for each element in description.
        Results are serialized using `AgentUtils.serialize_value`, this allows us to properly serialize
        date, datetime and any other data type that requires a custom serialization in the future.
        """
        if isinstance(value, Dict):
            if "description" in value:
                description = value["description"]
                value["description"] = [
                    self._process_description(
                        [col[0], col[1], col[2], col[3], col[4], col[5], col[6]]
                    )
                    for col in description
                ]
            if "all_results" in value:
                all_results: List = value["all_results"]
                value["all_results"] = [self._process_row(r) for r in all_results]

        return value

    @staticmethod
    def _process_row(row: List) -> List:
        return [AgentSerializer.serialize(v) for v in row]

    @classmethod
    def _process_description(cls, description: List) -> List:
        return [AgentSerializer.serialize(v) for v in description]

    @classmethod
    def get_cert_path(
        cls,
        platform: str,
        remote_location: str,
        retrieval_mechanism: str = "url",
        sub_folder: Optional[str] = None,
    ) -> Optional[str]:
        download_path = AgentUtils.temp_file_path(sub_folder)
        if retrieval_mechanism == "url":
            urlretrieve(url=remote_location, filename=download_path)
        else:
            storage_client = StorageProxyClient(platform).wrapped_client
            try:
                storage_client.download_file(
                    key=remote_location, download_path=download_path
                )
            except BaseStorageClient.NotFoundError as exc:
                logger.warning("Certificate not found in storage bucket", exc_info=exc)
                return None
        return download_path
