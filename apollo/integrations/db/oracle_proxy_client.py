import logging
import os
import ssl
import tempfile
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

import oracledb
from oracledb.base_impl import DbType

from apollo.agent.serde import AgentSerializer
from apollo.agent.utils import AgentUtils
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient, SslOptions

_ATTR_CONNECT_ARGS = "connect_args"

logger = logging.getLogger(__name__)


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
        
    Note: Only thin mode supports ssl_context - thick mode does not.
    """
    if ssl_options.disabled or not ssl_options.ca_data:
        return None

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # Disable hostname checking for RDS compatibility
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    # @SECLEVEL=1 allows older ciphers like AES256-GCM-SHA384 (plain RSA, no forward secrecy)
    ssl_context.set_ciphers("DEFAULT:@SECLEVEL=1")
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
    
    return ssl_context


class OracleProxyClient(BaseDbProxyClient):
    """
    Proxy client for Oracle DB Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `oracledb.connect`, so only attributes supported as parameters
    by `oracledb.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="oracle")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Oracle DB agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = {**credentials[_ATTR_CONNECT_ARGS]}
        if "expire_time" not in connect_args:
            connect_args["expire_time"] = (
                1  # enable keep-alive and send packets every minute
            )

        # Handle SSL options for Oracle connections
        ssl_options = SslOptions(**(credentials.get("ssl_options") or {}))
        if ssl_context := create_oracle_ssl_context(ssl_options):
            connect_args["ssl_context"] = ssl_context
            logger.debug("Oracle SSL context created")

        self._connection = oracledb.connect(**connect_args)  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection

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
