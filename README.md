# Monte Carlo Data Collector - Apollo Agent

Monte Carlo's [containerized agent](https://hub.docker.com/r/montecarlodata/agent) (Beta).
See [here](https://docs.getmontecarlo.com/docs/platform-architecture) for architecture details and alternative
deployment options.

## Development environment
### Pre-requisites
- Python 3.8 or later
- gcloud CLI (if planning to deploy to CloudRun)

### Prepare your local environment
- Create a virtual env, for example: `python -m venv .venv` and activate it: `. .venv/bin/activate`
  - If you don't use the virtual env in `.venv` you must create a symbolic link: `ln -s VENV_DIR .venv` because pyright requires the virtual env to be in `.venv` directory.
- Install the required libraries: `pip install -r requirements.txt -r requirements-dev.txt -r requirements-cloudrun.txt`
- Install the pre-commit hooks: `pre-commit install`

### Tests execution
- To run tests, use `pytest` (the configuration for pytest in `pyproject.toml` configures `.` as the `pythonpath` and `tests` as the test folder).

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
- For example to list all projects in a BigQuery connection, send a POST to http://localhost:8081/api/v1/agent/execute/bigquery/list-projects with the following body (this is equivalent to run: `self._client.projects().list(maxResults=100).execute()`):
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

### Adding new integrations
You can use `BqProxyClient` as a reference, basically you just need to:
- Create a new class extending `BaseProxyClient` that:
  - Creates the wrapped client in the constructor using the credentials parameter.
  - Returns the wrapped client in `wrapped_client` property. 
  - Methods received in `operation.commands` with `target=_client` (or no target as that's the default value) will be 
    searched first as methods in the proxy client class and if not present there will be searched in the 
    wrapped client object. So, the wrapped client is automatically exposed and if that's all you need to expose, then 
    you're done.
  - If you need to create "extension" methods, like a single `execute_and_fetch_all` method that executes and fetches
    the result in a single call, you can create them in the proxy client class. Remember that "chained" calls are supported,
    and that's usually enough, for example `_client.projects().list().execute()` will be sent as a single chained call
    and there's no need to create an "extension" method for it.
  - You can return `None` in `wrapped_client` if there's no wrapped object and all operations are implemented as 
    methods in the proxy client 
    class.
- Register the new client in `ProxyClientFactory`.

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

### Dev environment
In order to test in our dev environment you need to merge your branch into `dev`, that will automatically trigger a 
dev build and upload the image to our `pre-release-agent` repository in DockerHub.
For now, that build is not updating the dev instance in CloudRun, in order to do that you need to run:
```shell
gcloud run deploy dev-apollo-agent --image montecarlodata/pre-release-agent:latest-cloudrun --region us-east1
```
The same build process publishes also the `generic` image with the tag `latest-generic`, so you can use it to run a
standalone agent.

### API Docs
API is documented using [flask-swagger](https://github.com/getsling/flask-swagger) and published automatically to
https://apollodocs.dev.getmontecarlo.com/ when a dev build completes and to https://apollodocs.getmontecarlo.com/ 
for production builds.
In order to get better documentation, endpoints supporting multiple methods (like `GET` and `POST`) are 
implemented using two methods to document the required parameters in the right way for each method. 
The response element is defined in one of them and re-used in the other one, see `test_health` 
in `generic/main.py` for an example.

When running the agent locally, docs can be accessed using http://localhost:8081/swagger/ allowing endpoints to
be tried out. You can also add the endpoints to Postman by importing http://localhost:8081/swagger/openapi.json.

### Release process
To release a new version:
- create the PR from your branch
- once approved merge to `main`
- create a new release in GitHub: 
  - Releases -> create a new release
  - Create a new tag using semantic versioning prefixed with "v", like: `v1.0.1`
  - Set the title for the release as the version number, like `v1.0.1`
  - Enter the release description 
  - Publish the release
- This will trigger another build process that will publish the images to our `agent` repository in DockerHub

## License

See [LICENSE](https://github.com/monte-carlo-data/apollo-agent/blob/main/LICENSE.md) for more information.

## Security

See [SECURITY](https://github.com/monte-carlo-data/apollo-agent/blob/main/SECURITY.md) for more information.
