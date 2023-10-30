from typing import Any, Dict, List

from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient


class BaseDbProxyClient(BaseProxyClient):
    def process_result(self, value: Any) -> Any:
        """
        Converts "Column" objects in the description into a list of objects that can be serialized to JSON.
        From the DBAPI standard, description is supposed to return tuples with 7 elements, so we're returning
        those 7 elements back for each element in description.
        """
        if isinstance(value, Dict):
            if "description" in value:
                description = value["description"]
                value["description"] = [
                    [col[0], col[1], col[2], col[3], col[4], col[5], col[6]]
                    for col in description
                ]
            if "all_results" in value:
                all_results: List = value["all_results"]
                value["all_results"] = [self._process_row(r) for r in all_results]

        return value

    @staticmethod
    def _process_row(row: List) -> List:
        return [AgentUtils.serialize_value(v) for v in row]
