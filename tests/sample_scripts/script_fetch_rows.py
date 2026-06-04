from typing import Any


def execute_script_handler(client: Any, context: Any, sql_query: str):
    if client is None:
        raise Exception("is none")
    with client.cursor() as cursor:
        cursor.execute(sql_query)
        return {"rows": cursor.fetchmany(10)}
