from typing import Optional, Dict, Any, List, Union, Tuple

from pyarrow.flight import FlightDescriptor, FlightCallOptions

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

from pyarrow import flight, Schema, types as pyarrow_types

_ATTR_CONNECT_ARGS = "connect_args"


class DremioProxyClient(BaseDbProxyClient):
    """
    Proxy client for Dremio. Credentials are expected to be supplied under "connect_args" and
    will be passed directly to `pyarrow.flight.connect`, so only attributes supported as parameters by
    `flight.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="dremio")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Dremio agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = flight.connect(**credentials[_ATTR_CONNECT_ARGS])  # type: ignore
        self._headers = [
            (
                b"authorization",
                f"bearer {credentials.get('token')}".encode("utf-8"),
            )
        ]

    @property
    def wrapped_client(self):
        return self._connection

    def execute(self, query: str) -> Dict:
        # Send the query to the server and save the ticket identifier
        flight_info = self._connection.get_flight_info(
            FlightDescriptor.for_command(command=query),
            FlightCallOptions(headers=self._headers),
        )
        ticket = flight_info.endpoints[0].ticket

        # create a FlightStreamReader from the ticket
        reader = self._connection.do_get(
            ticket, FlightCallOptions(headers=self._headers)
        )

        # read all results of the query
        results = reader.read_all()

        # turn results into a serializable list
        records = [
            list(row)
            for row in zip(*[column.to_pylist() for column in results.itercolumns()])
        ]

        return {
            "records": records,
            "description": self._get_dbapi_description(reader.schema),
            "rowcount": len(records),
        }

    def _get_dbapi_description(self, schema: Schema) -> List[Tuple]:
        """
        Create a DB API compliant description object from the FlightStreamReader schema
        """

        def arrow_type_to_cursor_type(arrow_type: Any) -> Union[int, str]:
            if pyarrow_types.is_string(arrow_type):
                return 1
            elif pyarrow_types.is_int32(arrow_type):
                return 2
            elif pyarrow_types.is_float32(arrow_type):
                return 3
            # Add more type mappings as needed
            return "unknown"

        return [
            (
                field.name,
                arrow_type_to_cursor_type(field.type),
                None,
                None,
                None,
                None,
                None,
            )
            for field in schema
        ]
