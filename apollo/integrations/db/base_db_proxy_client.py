import logging
from abc import ABC
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


class BaseDbProxyClient(BaseProxyClient, ABC):
    def __init__(self):
        self._connection = None

    # On delete make sure we close the connection
    def __del__(self) -> None:
        self.close()

    def close(self):
        if self._connection:
            logger.info("Closing DB Proxy connection")
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
