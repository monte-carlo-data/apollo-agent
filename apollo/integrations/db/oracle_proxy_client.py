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
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class OracleProxyClient(BaseDbProxyClient):
    """
    Proxy client for Oracle DB Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `oracledb.connect`, so only attributes supported as parameters
    by `oracledb.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Oracle DB agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = credentials[_ATTR_CONNECT_ARGS]
        if "expire_time" not in connect_args:
            connect_args["expire_time"] = (
                1  # enable keep-alive and send packets every minute
            )

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
