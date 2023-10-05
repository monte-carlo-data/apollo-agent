# Introduction
This folder contains various documents explaining how different features of the agent are implemented,
this document introduces some basic concepts that are needed to understand the rest of the examples.

# Basic Concepts
The agent basically exposes a framework to execute operations on connections (also called clients), similar to what
and RPC framework does.

A request to `api/v1/agent/execute/<connection_type>/<operation_name>` specifies:
- a `connection_type` as the first path component
- an `operation_name` as the second path component
- `credentials` to use when creating the client
- an `operation` to execute

For example, a request to `api/v1/agent/execute/bigquery/list-projects`, with the following body:
```json
{
  "credentials": {
    //credentials here
  },
  "operation": {
    "trace_id": "1234",
    "commands": [
      //commands here
    ]
  }
}
```
specifies:
- `bigquery` as the connection type
- `list-projects` as the name of the operation being called, this is used for logging purposes only
- `credentials` to use when connecting to BigQuery
- a list of `commands` to execute after the BigQuery connection is established, the connection object will be available as an variable with name `_client` that actually doesn't need to be specified as the target for method calls as it's the default value for `target`

## Commands
The schema for a command object is:
```json
{
  "target": "_client", //optional, name of the object where method should be called, defaults to "_client",
  "method": "method_name", //required, name of the method to invoke
  "args": [], //optional, positional args to pass to method
  "kwargs": {}, //optional, keyword args to pass to method
  "store": "var_name", //optional, name of the variable to store the result of method
  "next": {} //optional, a chained command to be called in the result of method
}
```

## Chained calls
We have support for "chained calls", so for a code like this:
```python
_client.projects().list(maxResults=100).execute()
```
We don't need to call each method (project, list and execute) individually and store the result in a variable, we 
can just chain the calls in the same way we do in code:
```json
{
    "operation": {
        "trace_id": "1234",
        "commands": [
            {
                "method": "projects",
                "next": {
                    "method": "list",
                    "kwargs": {
                        "maxResults": 100
                    },
                    "next": {
                        "method": "execute"
                    }
                }
            }
        ]
    }
}
```

## Utilities
There's an object with name `__utils` automatically available in the context that can be used in addition to `_client` 
for invoking methods.
To build a dictionary `_utils.build_dict` can be used, for example for the response of `fetchall` operation in 
the cursor object the following command is used:
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
As you can see first we're calling different methods in `_cursor` and storing the results in temporary variables that 
are later used to build the result dictionary using `__utils.build_dict`.

