import logging
import os
import secrets
import shutil
import ssl
import tempfile
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

import oracledb
from oracledb.base_impl import DbType
from cryptography import x509
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    load_pem_private_key,
    pkcs12,
)

from apollo.common.agent.serde import AgentSerializer
from apollo.agent.utils import AgentUtils
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient, SslOptions

_ATTR_CONNECT_ARGS = "connect_args"
# Agent-level thick-mode switch. Thick mode (Oracle Instant Client) is a
# process-global, one-way setting that cannot coexist with thin connections in
# the same process, so it is configured per-agent via this env var rather than
# per-connection. Enable it only on an agent dedicated to thick-mode Oracle.
_ENV_VAR_THICK_MODE = "MCD_ORACLE_THICK_MODE"

# Optional override for the thick-mode TLS cipher suites (comma-separated Oracle
# cipher names). The default below intentionally includes RSA key-exchange
# suites: Oracle Client omits them from its defaults, but some servers (e.g. AWS
# RDS Oracle, which offers AES256-GCM-SHA384 / SSL_RSA_WITH_AES_256_GCM_SHA384)
# require them — the thin path handles this via `@SECLEVEL=1`, but thick has no
# such knob and needs an explicit SSL_CIPHER_SUITES list. Modern ECDHE suites
# are listed first so they are preferred where the server supports them.
_ENV_VAR_SSL_CIPHER_SUITES = "MCD_ORACLE_SSL_CIPHER_SUITES"
_DEFAULT_SSL_CIPHER_SUITES = ",".join(
    [
        "SSL_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "SSL_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "SSL_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "SSL_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "SSL_RSA_WITH_AES_256_GCM_SHA384",
        "SSL_RSA_WITH_AES_256_CBC_SHA256",
        "SSL_RSA_WITH_AES_128_GCM_SHA256",
        "SSL_RSA_WITH_AES_128_CBC_SHA256",
    ]
)

logger = logging.getLogger(__name__)

# Process-wide config dir (with sqlnet.ora) passed to init_oracle_client; created
# once, reused by every thick connection, lives for the process lifetime.
_thick_config_dir: Optional[str] = None


def _thick_mode_enabled() -> bool:
    return os.getenv(_ENV_VAR_THICK_MODE, "false").strip().lower() == "true"


def _ensure_thick_config_dir() -> str:
    """Create (once) a config dir containing a sqlnet.ora that sets
    SSL_CIPHER_SUITES, and return its path for ``init_oracle_client(config_dir=)``.

    Thick mode has no equivalent of thin's ``@SECLEVEL=1``, so the cipher suites
    an older server requires (e.g. AWS RDS Oracle's RSA-kx suites) must be listed
    explicitly here or the TLS handshake fails with ORA-28860.
    """
    global _thick_config_dir
    if _thick_config_dir is not None:
        return _thick_config_dir
    suites = os.getenv(_ENV_VAR_SSL_CIPHER_SUITES) or _DEFAULT_SSL_CIPHER_SUITES
    config_dir = tempfile.mkdtemp(prefix="mcd_oracle_tns_")
    with open(os.path.join(config_dir, "sqlnet.ora"), "w") as sqlnet:
        sqlnet.write(f"SSL_CIPHER_SUITES = ({suites})\n")
    _thick_config_dir = config_dir
    return config_dir


def create_oracle_thick_wallet(ssl_options: SslOptions) -> Optional[Tuple[str, str]]:
    """Build a PKCS#12 wallet (``ewallet.p12``) for thick-mode TLS from SslOptions.

    Thick mode does not accept a Python ``ssl.SSLContext`` (thin-only); it reads a
    wallet directory. python-oracledb accepts a password-protected ``ewallet.p12``
    that ``cryptography`` can produce, so no Oracle ``orapki`` tooling is needed.

    The wallet holds the CA cert(s) from ``ca_data`` as trust anchors, plus — when
    ``cert_data``/``key_data`` are present — the client key+cert for mTLS.

    Returns ``(wallet_dir, wallet_password)`` (caller passes these to
    ``oracledb.connect`` and is responsible for removing ``wallet_dir``), or
    ``None`` when SSL is disabled / no CA data is provided.
    """
    if ssl_options.disabled or not ssl_options.ca_data:
        return None

    ca_certs = x509.load_pem_x509_certificates(ssl_options.ca_data.encode())

    client_key = None
    client_cert = None
    if ssl_options.cert_data and ssl_options.key_data:
        client_cert = x509.load_pem_x509_certificate(ssl_options.cert_data.encode())
        client_key = load_pem_private_key(
            ssl_options.key_data.encode(),
            password=(
                ssl_options.key_password.encode() if ssl_options.key_password else None
            ),
        )

    wallet_password = secrets.token_urlsafe(24)
    p12 = pkcs12.serialize_key_and_certificates(
        b"mcd-oracle",
        # load_pem_private_key's union includes key types PKCS12 rejects (e.g. DH);
        # a TLS client key is always RSA/EC, so this is safe.
        client_key,  # type: ignore[arg-type]
        client_cert,
        ca_certs,
        BestAvailableEncryption(wallet_password.encode()),
    )
    wallet_dir = tempfile.mkdtemp(prefix="mcd_oracle_wallet_")
    with open(os.path.join(wallet_dir, "ewallet.p12"), "wb") as wallet_file:
        wallet_file.write(p12)
    return wallet_dir, wallet_password


def create_oracle_ssl_context(ssl_options: SslOptions) -> ssl.SSLContext | None:
    """
    Create an SSL context for Oracle connections.

    Creates an SSLContext with relaxed cipher requirements to support older cipher suites
    used by some databases (e.g., AWS RDS Oracle uses AES256-GCM-SHA384).

    Args:
        ssl_options: SslOptions object containing CA data and optionally client cert/key

    Returns:
        Configured ssl.SSLContext for use with oracledb connections, or None if SSL is disabled
        or no CA data is provided.

    Note: this is the thin-mode path. ssl_context is thin-only; thick mode uses
    create_oracle_thick_wallet() instead.
    """
    if ssl_options.disabled or not ssl_options.ca_data:
        return None

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    # Respect SslOptions verification settings
    # - skip_cert_verification: skips ALL validation (cert + hostname)
    # - verify_cert: whether to validate the certificate chain
    # - verify_identity: whether to check hostname matches certificate
    ssl_context.check_hostname = (
        not ssl_options.skip_cert_verification and ssl_options.verify_identity
    )
    ssl_context.verify_mode = (
        ssl.CERT_NONE
        if ssl_options.skip_cert_verification
        else (ssl.CERT_REQUIRED if ssl_options.verify_cert else ssl.CERT_NONE)
    )

    # @SECLEVEL=1 allows older ciphers like AES256-GCM-SHA384 (plain RSA, no forward secrecy)
    ssl_context.set_ciphers("DEFAULT:@SECLEVEL=1")

    # Load CA certificate for server verification (if not skipping verification)
    if ssl_options.ca_data and not ssl_options.skip_cert_verification:
        ssl_context.load_verify_locations(cadata=ssl_options.ca_data)

    # Load client certificate if provided (for mTLS)
    # Note: load_cert_chain() only accepts file paths, not string data,
    # so we must use temp files (unlike load_verify_locations which accepts cadata)
    if ssl_options.cert_data and ssl_options.key_data:
        cert_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
        cert_file.write(ssl_options.cert_data)
        cert_file.close()

        key_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
        key_file.write(ssl_options.key_data)
        key_file.close()

        try:
            ssl_context.load_cert_chain(
                certfile=cert_file.name,
                keyfile=key_file.name,
                password=ssl_options.key_password,
            )
        finally:
            # Clean up temp files after loading into SSL context
            os.unlink(cert_file.name)
            os.unlink(key_file.name)

    # Log SSL context creation with options
    has_client_cert = bool(ssl_options.cert_data and ssl_options.key_data)
    logger.info(
        "Oracle SSL context created",
        extra={
            "ssl_options": {
                "has_ca_data": bool(ssl_options.ca_data),
                "has_client_cert": has_client_cert,
                "skip_cert_verification": ssl_options.skip_cert_verification,
                "verify_cert": ssl_options.verify_cert,
                "verify_identity": ssl_options.verify_identity,
                "check_hostname": ssl_context.check_hostname,
                "verify_mode": ssl_context.verify_mode,
            }
        },
    )

    return ssl_context


class OracleProxyClient(BaseDbProxyClient):
    """
    Proxy client for Oracle DB Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `oracledb.connect`, so only attributes supported as parameters
    by `oracledb.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="oracle")
        # Per-connection thick-mode TLS wallet dir; removed on close. Set before
        # connect() so cleanup runs even if the connection attempt fails.
        self._wallet_dir: Optional[str] = None
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Oracle DB agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = {**credentials[_ATTR_CONNECT_ARGS]}
        if "expire_time" not in connect_args:
            connect_args["expire_time"] = (
                1  # enable keep-alive and send packets every minute
            )

        # Thick mode (Oracle Instant Client) is process-global and one-way: once a
        # thin connection exists it can't be enabled (DPY-2019). So it's an
        # agent-level env var, not per-connection — enable it only on a dedicated
        # thick-mode Oracle agent.
        thick_mode = _thick_mode_enabled()
        if thick_mode and oracledb.is_thin_mode():
            # Runs once per process, on the first Oracle connection; config_dir
            # carries a sqlnet.ora with SSL_CIPHER_SUITES for TLS. is_thin_mode()
            # flips to False after a successful init, so later connections skip it.
            oracledb.init_oracle_client(config_dir=_ensure_thick_config_dir())
            logger.info("oracle: thick mode initialized")

        # Configure SSL. Thin mode uses a Python ssl.SSLContext; thick mode does
        # not support that, so it uses a PKCS#12 wallet directory instead.
        ssl_options = SslOptions(**(credentials.get("ssl_options") or {}))
        if thick_mode:
            wallet = create_oracle_thick_wallet(ssl_options)
            if wallet:
                self._wallet_dir, wallet_password = wallet
                connect_args["wallet_location"] = self._wallet_dir
                connect_args["wallet_password"] = wallet_password
                connect_args["ssl_server_dn_match"] = (
                    not ssl_options.skip_cert_verification
                    and ssl_options.verify_identity
                )
                logger.info("oracle: thick TLS wallet configured")
        elif ssl_context := create_oracle_ssl_context(ssl_options):
            connect_args["ssl_context"] = ssl_context
            logger.info("Oracle SSL context created")

        self._connection = oracledb.connect(**connect_args)  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection

    def _close_client(self):
        # Close the connection first (base), then remove the temp wallet dir.
        super()._close_client()
        if self._wallet_dir:
            shutil.rmtree(self._wallet_dir, ignore_errors=True)
            self._wallet_dir = None

    @classmethod
    def _process_description(cls, description: List) -> List:
        return [cls._serialize_description(v) for v in description]

    @classmethod
    def _serialize_description(cls, value: Any) -> Any:
        if isinstance(value, DbType):
            # Oracle cursor returns the column type as <DbType DB_TYPE_NUMBER> instead of a
            # type_code which we expect. Here we are converting this type to a string of the type
            # so the description can be serialized. So <DbType DB_TYPE_NUMBER> will become just
            # DB_TYPE_NUMBER.
            # This doesn't use the __type__/__data__ scheme because we don't have enough
            # information on the client side to reconstruct the type concretely, so instead we're
            # just returning the form the client expects.
            return value.name
        else:
            return AgentSerializer.serialize(value)
