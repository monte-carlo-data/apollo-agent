# Health related endpoints
The following health related endpoints are exposed by the agent:

- `GET api/v1/test/health` that returns information about the agent:
  ```shell
  curl http://localhost:8081/api/v1/test/health
  ```
  ```json
  {
      "platform": "GCP",
      "version": "0.0.1",
      "build": "82",
      "env": {
          "python_version": "3.11.5",
          "server": "gunicorn/21.2.0"
      }
  }
  ```

- `GET /api/v1/test/network/open?host=www.google.com&port=80&timeout=10`: that checks if a connection can be opened to the given host:port (`POST` is also supported by passing a JSON object with the same attributes):
  ```shell
  curl "http://localhost:8081/api/v1/test/network/open?host=www.google.com&port=80&timeout=10"
  ```
  ```json
  {
      "message": "Port 80 is open on www.google.com"
  }
  ```
- `GET /api/v1/test/network/telnet?host=www.google.com&port=80&timeout=10`: that tries to open a Telnet connection to the given host:port (`POST` is also supported by passing a JSON object with the same attributes):
  ```shell
  curl "http://localhost:8081/api/v1/test/network/telnet?host=www.google.com&port=80&timeout=10"
  ```
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
```shell
curl http://localhost:8081/api/v1/agent/execute/databricks/show_catalogs -X POST -H "Content-Type: application/json" -d '
{
  "credentials": {
    "connect_args": {
        "server_hostname": "<databricks_host>",
        "http_path": "/sql/1.0/warehouses/<warehouse_id>",
        "access_token": "<databricks_tokem>",
        "_use_arrow_native_complex_types": false,
        "_user_agent_entry": "user_agent"
    }
  },
  "operation": {
    "trace_id": "1234",
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
}'
```

The same call can be performed without temp vars, calling `build_dict` this way (passing calls as arguments):
```shell
curl http://localhost:8081/api/v1/agent/execute/databricks/show_catalogs -X POST -H "Content-Type: application/json" -d '
{
  "credentials": {
    "connect_args": {
        "server_hostname": "<databricks_host>",
        "http_path": "/sql/1.0/warehouses/<warehouse_id>",
        "access_token": "<databricks_tokem>",
        "_use_arrow_native_complex_types": false,
        "_user_agent_entry": "user_agent"
    }
  },
  "operation": {
    "trace_id": "1234",
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
        "target": "__utils",
        "method": "build_dict",
        "kwargs": {
            "all_results": {
                "__type__": "call",
                "target": "_cursor",
                "method": "fetchall"
            },
            "description": {
                "__type__": "call",
                "target": "_cursor",
                "method": "description"
            },
            "rowcount": {
                "__type__": "call",
                "target": "_cursor",
                "method": "rowcount"
            }
        }
      }
    ]
  }
}'
```

## HTTP proxy client
A sample HTTP operation to get the status for a SQL Warehouse would look like:
```shell
curl http://localhost:8081/api/v1/agent/execute/http/do_request -X POST -H "Content-Type: application/json" -d '
{
  "operation": {
    "trace_id": "1234",
    "commands": [
        {
            "method": "do_request",
            "kwargs": {
                "url": "https://<dbx_host>/api/2.0/sql/warehouses/<warehouse_id>",
                "http_method": "GET",
                "user_agent": "user_agent_value"
            }
        }
    ]
  }, 
  "credentials": {
    "token": "<dbx_token>"
  }
}'
```

## Storage client
Storage operations like `list_objects` are regular operations returning a list of objects, but the storage client also
uses operations that returns binary data, when the result of an operation is a binary type like `bytes` or `BinaryIO`
the agent uses that content as the response (instead of wrapping the result in a JSON document) and sets the 
content type in the response to `application/octet-stream`.

For example, the following operation returns the contents of the `test.json` file:
```shell
curl http://localhost:8081/api/v1/agent/execute/storage/read -X POST -H "Content-Type: application/json" -i -d '{
  "operation": {
    "trace_id": "1234",
    "commands": [
      {
          "method": "read",
          "args": [
              "test.json"
          ]
      }
    ]
  }
}'
```
and these headers:
```
x-mcd-trace-id: 1234
content-type: application/octet-stream
```
