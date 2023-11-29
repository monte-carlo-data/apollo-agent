import os
import uuid
from enum import Enum
from typing import Dict, Any, Sequence, Optional, List, TextIO

import attr
from attr import asdict
from looker_sdk import init40
from looker_sdk.rtl.transport import TransportOptions
from looker_sdk.sdk.api40.models import (
    Category,
)

from apollo.agent.constants import (
    ATTRIBUTE_NAME_TYPE,
    ATTRIBUTE_NAME_DATA,
    ATTRIBUTE_VALUE_TYPE_LOOKER_CATEGORY,
)
from apollo.agent.serde import AgentSerializer
from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient

_LOOKER_DIRECTORY = "looker"


class LookerProxyClient(BaseProxyClient):
    """
    Looker Proxy Client, simple class that uses the received credentials to create a Looker connection.
    This connection is returned as the `wrapped_client` attribute and the agent will take care of executing methods
    there.
    All result objects are converted to dictionaries using `attr.asdict`.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        """
        initializing the looker client. The credentials dictionary should include the following:
        base_url (string)
        client_id (string)
        client_secret (string)
        verify_ssl (boolean) (default is true)
        """
        if not credentials:
            raise ValueError("Credentials are required for Looker")

        self._temp_file_path = AgentUtils.temp_file_path(
            sub_folder=_LOOKER_DIRECTORY, extension="ini"
        )

        self._client_id = credentials.get("client_id")
        self._client_secret = credentials.get("client_secret")

        with open(self._temp_file_path, "w") as output_file:
            self._write_connection_to_file(output_file, credentials)
        self._client = init40(self._temp_file_path)
        # we cannot remove temp_file_path here, it's used when we call the first method, like all_dashboards

    def __del__(self):
        if self._temp_file_path:
            os.remove(self._temp_file_path)

    def login(self, transport_options: Optional[Dict]):
        """
        Perform login using the connection to Looker, this is used only by validators, there's no need to call
        `login` before other operations like `all_dashboards`.
        """
        self._client.login(
            client_id=self._client_id,
            client_secret=self._client_secret,
            transport_options=TransportOptions(**transport_options)
            if transport_options
            else None,
        )

    @staticmethod
    def _write_connection_to_file(output_file: TextIO, credentials: Dict) -> None:
        output_file.write("[Looker]\n")
        for key in credentials.keys():
            output_file.write(f"{key}={credentials[key]}\n")

    @property
    def wrapped_client(self):
        return self._client

    def all_dashboards(self, fields: Optional[str], *args, **kwargs):  # type: ignore
        """
        Optimization method used to return only the requested fields, by default we're returning the objects
        serialized to dictionaries using attrs.asdict, but that includes some attributes not requested.
        Here we're using the fields parameter if present to call `asdict` with the list of fields requested.
        """
        result = self._client.all_dashboards(fields=fields, *args, **kwargs)
        if bool(result) and bool(fields):
            return self._as_dict_with_fields(result, fields)
        return result

    def all_looks(self, fields: Optional[str], *args, **kwargs):  # type: ignore
        """
        Optimization method used to return only the requested fields, by default we're returning the objects
        serialized to dictionaries using attrs.asdict, but that includes some attributes not requested.
        Here we're using the fields parameter if present to call `asdict` with the list of fields requested.
        """
        result = self._client.all_looks(fields=fields, *args, **kwargs)
        if bool(result) and bool(fields):
            return self._as_dict_with_fields(result, fields)
        return result

    def process_result(self, value: Any) -> Any:
        """
        Process the result of the methods on this client before being serialized to JSON.
        As all Looker API objects use `attrs` framework, we're using `attrs.asdict` to convert them
        to dictionaries, we still need to process enums that are not converted automatically to string.
        We use `_filter_result` to filter out objects we don't want to send back, mainly because they are not
        serializable to JSON.
        """
        if isinstance(value, Sequence):
            return [self.process_result(e) for e in value]
        elif value is not None and attr.has(type(value)):
            return asdict(
                value,
                filter=self._filter_result,
                value_serializer=self._serialize_value,
            )
        else:
            return value

    @staticmethod
    def _filter_result(attribute: attr.Attribute, value: Any):
        """
        Used to filter out some objects we don't want to include in the response, for example
        `user_attribute_filter_types` and all enums, except Category that is needed client side.
        """
        if attribute.name == "user_attribute_filter_types":
            return False
        elif isinstance(value, Enum) and not isinstance(value, Category):
            return False
        return True

    @staticmethod
    def _serialize_value(instance: Any, field: Any, value: Any):
        """
        Implements the serialization for `attrs.asdict`, it serializes Category enums and datetime objects in the
        format expected client side.
        For special types we send a dictionary with a "__type__" attribute indicating the "special" type and
        __data__ containing the serialized value.
        """
        if isinstance(value, Category):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_LOOKER_CATEGORY,
                ATTRIBUTE_NAME_DATA: value.name,
            }
        else:
            return AgentSerializer.serialize(value)

    @staticmethod
    def _as_dict_with_fields(values: Sequence, fields: str) -> List:
        """
        Optimization method that includes only some attributes when serializing Looker objects to dictionaries.
        Used by `all_dashboards` and `all_looks` to return only the `id` attribute (or any other requested attribute),
        by default even when we request only `id` all the other attributes are serialized back with `null` value,
        so we do this to reduce the size of the response.
        """
        field_list = [f.strip() for f in fields.split(",")]

        def field_filter(attribute: attr.Attribute, value: Any) -> bool:
            return attribute.name in field_list

        return [asdict(v, filter=field_filter) for v in values]
