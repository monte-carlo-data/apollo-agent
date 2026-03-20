import os
import ssl
import tempfile
from dataclasses import dataclass


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

    def write_ca_data_to_temp_file(self, temp_cert_path: str, upsert: bool) -> str:
        """
        Some clients require CA data be passed as a path to a file.
        This method writes the ca_data to the provided path, can optionally
        upsert what is already there.
        """
        if os.path.isfile(temp_cert_path) and not upsert:
            raise ValueError("File already exists at this path.")
        if not self.ca_data:
            raise ValueError("No CA data to write to file.")
        with open(temp_cert_path, "w") as temp_cert_file:
            temp_cert_file.write(self.ca_data)
        return temp_cert_path

    def _set_cert_and_key_to_context(self, ssl_context: ssl.SSLContext) -> None:
        """Check if temp file exists, if not create it from certificate or key data."""
        if not self.cert_data:
            return

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
