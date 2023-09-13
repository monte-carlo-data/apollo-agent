from typing import Optional, List, Dict


class SampleQueryResult:
    def __init__(self, query: str):
        self._query = query

    def query_results(self) -> List[Dict]:
        return [
            {
                "query": self._query,
                "data": [
                    {
                        "name": "abc",
                        "value": 123,
                    },
                    {
                        "name": "def",
                        "value": 456,
                    },
                ],
            }
        ]


class SampleCursor:
    def __init__(self):
        self._last_query_results: Optional[SampleQueryResult] = None

    def cursor_execute_query(self, query: str) -> SampleQueryResult:
        self._last_query_results = SampleQueryResult(query)
        return self._last_query_results

    def cursor_fetch_results(self) -> List[Dict]:
        return self._last_query_results.query_results()


class SampleInternalClient:
    def __init__(self):
        self._cursor = SampleCursor()

    def execute_query(self, query: str):
        self._cursor.cursor_execute_query(query)

    def fetch_results(self) -> List[Dict]:
        return self._cursor.cursor_fetch_results()

    def cursor(self):
        return self._cursor


class SampleProxyClient:
    def __init__(self):
        self._client = SampleInternalClient()

    @property
    def wrapped_client(self):
        return self._client

    def execute_and_fetch(self, query: str) -> List[Dict]:
        self._client.execute_query(query)
        return self._client.fetch_results()
