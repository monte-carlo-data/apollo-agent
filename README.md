# Monte Carlo Data Collector - Apollo Agent

## Development environment
### Pre-requisites
- Python 3.8 or later
- gcloud CLI (if planning to deploy to CloudRun)

### Prepare your local environment
- Create a virtual env, for example: `python -m venv .venv` and activate it: `. .venv/bin/activate`
- Install the required libraries: `pip install -r requirements.txt -r requirements-dev.txt`

### Tests execution
- To run tests, use `pytest`: `pytest tests`, you might need to set PYTHONPATH env variable, like: `PYTHONPATH=. pytest tests`

### Local application execution
- Apollo Agent uses a Flask application
- To run it: `python apollo/interfaces/generic/main.py`
- The server will listen in port `8081` and you can execute commands with a POST to http://localhost:8081/api/v1/agent/execute/<connection_type>/<operation_name>
- The body is expected to contain:
```json
{
    "credentials": {
    },
    "operation": {
        "trace_id": "TRACE_ID_HERE",
        "commands": [
          //commands here
       ]
    }
}
```
- For example to list all projects in a BigQuery connection, send a POST to http://localhost:8081/api/v1/agent/execute/bigquery/list-projects with the following body:
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
### Cloud Run deployment
- Once your gcloud cli is configured you can deploy the application using:
```shell
gcloud run deploy CLOUD_RUN_SERVICE_NAME_HERE --source .
```
- If you enable authentication, you'll need to create a service account to use as the "invoker", the minimum required permission for that service account is "Cloud Run Invoker" for the deployed service
- Once you configured the service account to use as the client, you can create identity tokens using:
```shell
gcloud auth print-identity-token [service account email address]
```
- The identity token is sent as the Bearer authorization header: `Authorization: Bearer IDENTITY_TOKEN`
