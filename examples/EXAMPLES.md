# Health related endpoints
The following health related endpoints are exposed by the agent:

- `GET api/v1/test/health` that returns information about the agent:
  ```json
  {
      "platform": "GCP",
      "version": "0.0.1"
      "build": "82",
      "env": {
          "python_version": "3.11.5",
          "server": "gunicorn/21.2.0"
      },
  }
  ```

- `GET /api/v1/test/network/open?host=www.google.com&port=80&timeout=10`: that checks if a connection can be opened to the given host:port (`POST` is also supported by passing a JSON object with the same attributes):
  ```json
  {
      "message": "Port 80 is open on www.google.com"
  }
  ```
- `GET /api/v1/test/network/telnet?host=www.google.com&port=80&timeout=10`: that tries to open a Telnet connection to the given host:port (`POST` is also supported by passing a JSON object with the same attributes):
  ```json
  {
      "message": "Telnet connection for www.google.com:80 is usable."
  }
  ```
  
# Execute endpoint
`api/v1/agent/execute/<connection_type>/<operation_name>` can be used to execute operations in clients:

## Cursor related operations
The following code to execute a query and fetch results:
```python
_cursor = connection.cursor()
_cursor.execute("SHOW CATALOGS")
return {
  "all_results": _cursor.fetchall(),
  "description": _cursor.description,
  "rowcount": _cursor.rowcount
}
```

would be sent to the agent with the following operation:
```json
{
    "commands": [
        {
            "method": "cursor",  
            "store": "_cursor"
        },
        {
            "target": "_cursor",  
            "method": "execute",
            "args": [
                "SHOW CATALOGS"
            ]
        },
        {
            "target": "_cursor",
            "method": "fetchall",
            "store": "tmp_1"
        },
        {
            "target": "_cursor",
            "method": "description",
            "store": "tmp_2"
        },
        {
            "target": "_cursor",
            "method": "rowcount",
            "store": "tmp_3"
        },
        {
            "target": "__utils",
            "method": "build_dict",
            "kwargs": {
                "all_results": {
                    "__reference__": "tmp_1"
                },
                "description": {
                    "__reference__": "tmp_2"
                },
                "rowcount": {
                    "__reference__": "tmp_3"
                }
            }
        }
    ]
}

```

The same call can be performed without temp vars, calling `build_dict` this way (passing calls as arguments):
```json
{
    "target": "__utils",
    "method": "build_dict",
    "kwargs": {
        "all_results": {
            "__type__": "call",
            "target": "_cursor",
            "method": "fetchall",
        },
        "description": {
            "__type__": "call",
            "target": "_cursor",
            "method": "description",
        },
        "rowcount": {
            "__type__": "call",
            "target": "_cursor",
            "method": "rowcount",
        },
    },
}
```


## HTTP proxy client
A sample HTTP operation to get the status for a SQL Warehouse would look like:
```json
{
    "commands": [
        {
            "method": "do_request",
            "kwargs": {
                "url": "https://dbx_host/api/2.0/sql/warehouses/<warehouse_id>",
                "http_method": "GET",
                "user_agent": "user_agent_value",
            }
        }
    ]
}
```
