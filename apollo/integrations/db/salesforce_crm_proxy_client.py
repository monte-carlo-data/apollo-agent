from typing import Optional, Dict, Any, List, Union, Tuple

from simple_salesforce.api import Salesforce

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class SalesforceCRMProxyClient(BaseDbProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="salesforce-crm")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Salesforce CRM agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = Salesforce(**credentials[_ATTR_CONNECT_ARGS])  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection

    def close(self):
        pass

    def execute(self, query: str) -> Dict:
        description = []
        records = []

        results = self._connection.query_all(query)
        rowcount = results["totalSize"]
        records_raw = results["records"]

        if records_raw:
            description = self._infer_cursor_description(records_raw[0])
            for row in records_raw:
                record = [v for k, v in row.items() if k != "attributes"]
                records.append(record)

        return {"description": description, "records": records, "rowcount": rowcount}

    def execute_count_query(self, query: str) -> Dict:
        # The minimum batch size for the REST API is 200.
        headers = {
            "Sforce-Query-Options": "batchSize=200",
        }
        results = self._connection.query(query, headers=headers)
        description = [("ROW_COUNT", "int", None, None, None, None, None)]
        records = [[results["totalSize"]]]

        return {
            "description": description,
            "records": records,
            "rowcount": len(records),
        }

    def execute_row_limit(self, query: str, limit: int) -> Dict:
        description = []
        records = []

        # Return early, if we're not going to return any rows.
        if limit < 1:
            return {
                "description": description,
                "records": records,
                "rowcount": 0,
            }

        # REST API docs say batch size must be between 200 and 2000.
        if limit < 200:
            batch_size = 200
        elif 200 <= limit <= 2000:
            batch_size = limit
        else:
            batch_size = 2000

        headers = {
            "Sforce-Query-Options": f"batchSize={batch_size}",
        }
        results_iter = self._connection.query_all_iter(query, headers=headers)

        try:
            first_row = next(results_iter)
        except StopIteration:
            return {
                "description": description,
                "records": records,
                "rowcount": 0,
            }

        # Set the description from the first row
        description = self._infer_cursor_description(first_row)

        # Store first row
        records.append([v for k, v in first_row.items() if k != "attributes"])
        row_count = 1

        # Fetch remaining rows up to the limit.
        while row_count < limit:
            try:
                row = next(results_iter)
                records.append([v for k, v in row.items() if k != "attributes"])
                row_count += 1
            except StopIteration:
                break

        return {
            "description": description,
            "records": records,
            "rowcount": row_count,
        }

    def describe_global(self) -> Dict:
        sobjects = []
        results = self._connection.describe()
        if results:
            sobjects = results["sobjects"]
        return {"objects": sobjects}

    def describe_object(self, object_name: str) -> Dict:
        salesforce_object = getattr(self._connection, object_name)
        results = salesforce_object.describe()
        return {"object_description": results}

    def _infer_cursor_description(self, row: dict) -> List[Tuple]:
        def infer_type(value: Any) -> str:
            if value is None:
                return "str"
            return type(value).__name__

        return [
            (
                k,
                infer_type(v),
                None,  # display_size
                None,  # internal_size
                None,  # precision
                None,  # scale
                None,  # null_ok
            )
            for k, v in row.items()
            if k != "attributes"
        ]
