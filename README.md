# Monte Carlo Data Collector - Apollo Agent

Monte Carlo's [containerized agent](https://hub.docker.com/r/montecarlodata/agent).
See [here](https://docs.getmontecarlo.com/docs/platform-architecture) for architecture details and alternative
deployment options.

## Local development environment
### Pre-requisites
- Python 3.12 or later
- If you are on a Mac, you'll need to [install a driver manager for ODBC](https://github.com/mkleehammer/pyodbc/wiki/Install#installing-on-macosx): `brew install unixodbc`.

### Prepare your local environment
- Create a virtual env, for example: `python -m venv .venv` and activate it: `. .venv/bin/activate`
  - If you don't use the virtual env in `.venv` you must create a symbolic link: `ln -s VENV_DIR .venv` because pyright requires the virtual env to be in `.venv` directory.
- Install the required libraries: `pip install -r requirements.txt -r requirements-dev.txt -r requirements-cloudrun.txt -r requirements-azure.txt -r requirements-lambda.txt`
- If you're on a Mac and the `pyodbc` version is < 5.1.0, [try installing by compiling from source](https://github.com/mkleehammer/pyodbc/wiki/Install#installing-on-macosx): `pip install --no-binary=pyodbc pyodbc`.
- Install the pre-commit hooks: `pre-commit install`

### Tests execution
- To run tests, use `pytest` (the configuration for pytest in `pyproject.toml` configures `.` as the `pythonpath` and `tests` as the test folder).

### Local application execution
- Apollo Agent uses a Flask application
- To run it: `python -m apollo.interfaces.generic.main`
- The server will listen in port `8081` and you can call the `health` endpoint by accessing: http://localhost:8081/api/v1/test/health:
  ```shell
  curl http://localhost:8081/api/v1/test/health | jq
  ```
- You can execute commands with a POST to http://localhost:8081/api/v1/agent/execute/<connection_type>/<operation_name>
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

#### Local execution using Docker
You can also execute the agent building and running a Docker image:
```shell
docker build -t local_agent --target generic --platform=linux/amd64 .
docker run --rm --name local_agent -p8081:8081 -ePORT=8081 -it local_agent
```
Or running the latest dev image from DockerHub:
```shell
docker run --rm --name dev_agent -p8081:8081 -ePORT=8081 -it montecarlodata/pre-release-agent:latest-generic
```

And you can run the unit tests in Docker:
```shell
docker build -t test_agent --target tests --platform=linux/amd64 --build-arg CACHEBUST="`date`" --progress=plain .
```
**Note**: `CACHEBUST` is used as a way to skip the cached layer for the tests execution and force them to run again.

#### Running storage operations locally
If you need to run storage (S3, GCS or Azure Blob) operations locally you can run the agent setting these 
environment variables: `MCD_STORAGE` and `MCD_STORAGE_BUCKET_NAME`, for example:
```shell
MCD_DEBUG=true MCD_STORAGE_BUCKET_NAME=agent-bucket MCD_STORAGE=GCS python -m apollo.interfaces.generic.main
```
**Note**: If you use `direnv` you can copy `.envrc.example` to `.envrc` and update the environment variables there.

Please note this needs your environment to be configured with credentials for the environment hosting the bucket, 
for GCS you need to login using `gcloud` and for AWS you need to specify the required environment variables for 
`boto3` to connect to the bucket.

Storage operations are executed using the special connection type: `storage`, for example to list all objects
in the bucket you can send a POST to http://localhost:8081/api/v1/agent/execute/storage/list-objects
with the following body:
```json
{
    "operation": {
        "trace_id": "1234",
        "commands": [
            {
                "method": "list_objects"
            }
        ]
    }
}
```

## Adding new integrations
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
    methods in the proxy client class.
- Register the new client in `ProxyClientFactory`.

## Dev environment
In order to test in our dev environment you need to merge your branch into `dev`, that will automatically trigger a 
dev build and upload the image to our `pre-release-agent` repository in DockerHub.

Then, you can follow the instructions in our public docs to update the agents:
- AWS: https://docs.getmontecarlo.com/docs/create-and-register-an-aws-agent#how-do-i-upgrade-the-agent
- GCP: https://docs.getmontecarlo.com/docs/create-and-register-a-gcp-agent#how-do-i-upgrade-the-agent

### Deploying new agents
You can also deploy new agents instead of using the existing dev agents, you can follow the instructions for each 
platform in our public docs: [AWS](https://docs.getmontecarlo.com/docs/create-and-register-an-aws-agent) and [GCP](https://docs.getmontecarlo.com/docs/create-and-register-a-gcp-agent),
by using a Terraform or CloudFormation template:
- For Azure and GCP: you need to use an image from our pre-release repo in DockerHub, for example: `montecarlodata/pre-release-agent:latest-cloudrun`
- For Lambda: you need to use our dev ECR repo, for example: `arn:aws:ecr:us-east-1:404798114945:repository/mcd-pre-release-agent:latest`

Additionally, you can check the example scripts provided with each template, as they help with tasks like deploying, testing and removing:
- Terraform AWS Template - [Makefile](https://github.com/monte-carlo-data/terraform-aws-mcd-agent/blob/main/examples/agent/Makefile) that can be used to deploy/test/destroy the agent.
- Terraform GCP Template - [Makefile](https://github.com/monte-carlo-data/terraform-google-mcd-agent/blob/main/examples/agent/Makefile) that can be used to deploy/test/destroy the agent.
- CloudFormation `test_execution` script: [test_execution.sh](https://github.com/monte-carlo-data/mcd-iac-resources/blob/main/examples/agent/test_execution.sh) that can be used to test the agent by invoking the health endpoint.

A DC will send all traffic (for the supported integrations) through the agent once configured, so it
is recommended to deploy a new DC to use with your agent.

For testing, you can also deploy the agent without registering it with a DC and invoke the endpoints manually,
for Azure and GCP you can use Postman and for Lambda you'll need to use `aws` CLI, check the
[Advanced Deployment](#advanced-deployment) section below for more information on invoking the 
health endpoint manually for each platform. 

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

## Release process
To release a new version:
- create the PR from your branch
- once approved merge to `main`
- create a new release in GitHub: 
  - Releases -> create a new release
  - Create a new tag using semantic versioning prefixed with "v", like: `v1.0.1`
  - Set the title for the release as the version number, like `v1.0.1`
  - Enter the release description 
  - Publish the release
- This will trigger another build process that will publish the images to our `agent` repository in DockerHub and
  the ECR repo (for Lambda images only).

## License

See [LICENSE](https://github.com/monte-carlo-data/apollo-agent/blob/main/LICENSE.md) for more information.

## Security

See [SECURITY](https://github.com/monte-carlo-data/apollo-agent/blob/main/SECURITY.md) for more information.

## Advanced deployment
This section is intended only for troubleshooting and advanced scenarios, using templates (Terraform or CloudFormation)
is the preferred way to deploy agents (even test agents as you can customize the image to use).

### Cloud Run deployment
- You need to have `gcloud` CLI configured, instructions [here](https://cloud.google.com/sdk/docs/install-sdk).
- Once your gcloud cli is configured you can deploy the application using:
```shell
gcloud run deploy CLOUD_RUN_SERVICE_NAME_HERE --image montecarlodata/pre-release-agent:latest-cloudrun
```
- The previous step assumes the code to deploy is the latest in the `dev` branch, you can also deploy 
  your local code (passing `--source .` instead of `--image`), but you also need to update the `Dockerfile` 
  to leave only the `cloudrun` image as there's no way to specify the target and it seems the last one
  is automatically selected.
- If you enable authentication, you'll need to create a service account to use as the "invoker", the minimum 
  required permission for that service account is "Cloud Run Invoker" for the deployed service
- Once you configured the service account to use as the client, you can create identity tokens using:
```shell
gcloud auth print-identity-token [service account email address]
```
- The identity token is sent as the Bearer authorization header: `Authorization: Bearer IDENTITY_TOKEN`

To update an agent in CloudRun you need to run the same command used to deploy it for the first time:
```shell
gcloud run deploy CLOUD_RUN_SERVICE_NAME_HERE --image montecarlodata/pre-release-agent:latest-cloudrun
```

### Azure deployment
You can check the README file for Azure [here](apollo/interfaces/azure/README.md).
For authentication, you need to get the app-key for the Azure App and pass it in the `x-functions-key` header.

### Lambda deployment
You can build the Docker image for the Lambda agent using:
```shell
docker build -t lambda_agent -f Dockerfile --target lambda --build-arg code_version=<version> --build-arg build_number=<build_number> --platform=linux/amd64 .
```
With version being a semantic version like `0.0.2` and build_number just an integer number like `110`.

If you have your own ECR repo used for testing you'll need to login first:
```shell
aws ecr get-login-password --region us-east-1 --profile <aws_profile> | docker login --username AWS --password-stdin <account_id>.dkr.ecr.us-east-1.amazonaws.com
```

And then tag/push your image:
```shell
docker tag lambda_agent:latest <account_id>.dkr.ecr.us-east-1.amazonaws.com/dev-agent:<build_number>
docker push <account_id>.dkr.ecr.us-east-1.amazonaws.com/dev-agent:<build_number> 
```

Deploy a new Lambda function using the image you pushed to your ECR repo and now you can
invoke the health endpoint using:
```shell
aws lambda invoke --profile <aws_profile> --function-name <lambda_arn> --cli-binary-format raw-in-base64-out --payload '{"path": "/api/v1/test/health", "httpMethod": "GET", "queryStringParameters": {"trace_id": "1234", "full": true}}' /dev/stdout | jq '.body | fromjson'
```

## Fixing Vulnerabilities and Verifying Library Upgrades

### 1. Updating Vulnerable Dependencies
If a vulnerability is reported in a dependency (e.g., via Aikido, Snyk, or another scanner), update the affected package in `requirements.in` to the latest secure version. For example, to update `teradatasql`:

```shell
# Edit requirements.in and set the desired version, e.g.:
teradatasql==20.0.0.30

# Recompile requirements.txt:
pip-compile requirements.in
```

### 2. Rebuilding the Docker Image
After updating dependencies, rebuild the Docker image to ensure the new versions are installed:

```shell
docker build -t local_agent --target generic --platform=linux/amd64 .
```

### 3. Verifying Library Versions in the Image
To verify that the correct library version is installed in the built image, run:

```shell
docker run --rm local_agent pip show teradatasql
```
Or, for system packages (e.g., libcap2):
```shell
docker run --rm local_agent dpkg -l | grep libcap2
```

This will print the installed version, which you can check against the required secure version.

### 4. Running Tests in Docker
You can also run the unit tests in Docker to ensure everything works as expected:
```shell
docker build -t test_agent --target tests --platform=linux/amd64 --build-arg CACHEBUST="`date`" --progress=plain .
```

**Note:** Always review the security advisories and package documentation for any additional upgrade steps or breaking changes.
