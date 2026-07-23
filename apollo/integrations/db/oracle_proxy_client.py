import logging
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

import oracledb
from oracledb.base_impl import DbType

from apollo.common.agent.serde import AgentSerializer
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient, SslOptions
from apollo.integrations.db import oracle_client_config

# create_oracle_ssl_context is used below (thin path) and also imported from here
# by data-collector; keep it importable from this module.
from apollo.integrations.db.oracle_client_config import create_oracle_ssl_context

_ATTR_CONNECT_ARGS = "connect_args"

logger = logging.getLogger(__name__)


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

        # ssl_options can arrive two ways: inside connect_args (the CTP maps it
        # there — it is NOT an oracledb.connect arg, so pop it out) or as a
        # top-level sibling (direct construction / legacy pre-CTP path). Use an
        # explicit None check (not `or`) so an explicitly-empty connect_args
        # ssl_options isn't silently overridden by the top-level sibling.
        ssl_options_data = connect_args.pop("ssl_options", None)
        if ssl_options_data is None:
            ssl_options_data = credentials.get("ssl_options")
        ssl_options = SslOptions(**(ssl_options_data or {}))

        # SSL differs by driver mode: thin uses a Python ssl.SSLContext; thick
        # (Oracle Instant Client) validates against an Oracle wallet configured in
        # sqlnet.ora. Thick mode is process-global and one-way, enabled per-agent
        # via MCD_ORACLE_THICK_MODE (see oracle_client_config). Both paths are handled there.
        if oracle_client_config.thick_mode_enabled():
            oracle_client_config.configure_thick_connection(ssl_options)
        elif ssl_context := create_oracle_ssl_context(ssl_options):
            connect_args["ssl_context"] = ssl_context

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
