# Monte Carlo Data Collector - Apollo Agent

Monte Carlo's [containerized agent](https://hub.docker.com/r/montecarlodata/agent) (Beta).
See [here](https://docs.getmontecarlo.com/docs/platform-architecture) for architecture details and alternative
deployment options.

## Local development environment
### Pre-requisites
- Python 3.11 or later

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

#### Running storage operations locally
If you need to run storage (S3, GCS or Azure Blob) operations locally you can run the agent setting these 
environment variables: `MCD_STORAGE` and `MCD_STORAGE_BUCKET_NAME`, for example:
```shell
PYTHONPATH=. MCD_DEBUG=true MCD_STORAGE_BUCKET_NAME=agent-bucket MCD_STORAGE=GCS python apollo/interfaces/generic/main.py
```
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

### Adding new integrations
How to add new integrations is documented in Notion [here](https://www.notion.so/montecarlodata/Adding-support-for-Remote-Agents-to-integrations-3d5025ef36eb47de8488cdafdc39d42c?pvs=4). 
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

### Dev environment
In order to test in our dev environment you need to merge your branch into `dev`, that will automatically trigger a 
dev build and upload the image to our `pre-release-agent` repository in DockerHub.
For now, that build is not updating the dev agents for the different platforms, the easiest way to update 
them is to connect to MC Dev environment with `dev.apollo.agent` user (credentials in 1Pwd), go to
Settings -> Integrations -> Agents & Data Store and update them.

#### Deploying new agents
You can also deploy new agents instead of using the existing dev agents, you can follow the instructions for each 
platform linked from the Apollo Hub [here](https://www.notion.so/montecarlodata/Apollo-Hub-Agent-Architecture-for-Hybrid-Hosted-Collection-8ea81cccf3f04bc38179f4c7566607da?pvs=4),
by using a Terraform or CloudFormation template:
- For Azure and GCP: you need to use an image from our pre-release repo in DockerHub, for example: `montecarlodata/pre-release-agent:latest-cloudrun`
- For Lambda: you need to use our dev ECR repo, for example: `arn:aws:ecr:us-east-1:404798114945:repository/mcd-pre-release-agent:latest`

A DC will send all traffic (for the supported integrations) through the agent once configured, so it
is recommended to deploy a new DC to use with your agent.
For testing, you can also deploy the agent without registering it with a DC and invoke the endpoints manually,
for Azure and GCP you can use Postman and for Lambda you'll need to use `aws` CLI.

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
- This will trigger another build process that will publish the images to our `agent` repository in DockerHub and
  the ECR repo (for Lambda images only).

## License

See [LICENSE](https://github.com/monte-carlo-data/apollo-agent/blob/main/LICENSE.md) for more information.

## Security

See [SECURITY](https://github.com/monte-carlo-data/apollo-agent/blob/main/SECURITY.md) for more information.

### Advanced deployment

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

### Lambda deployment
You can build the Docker image for the Lambda agent using:
```shell
docker build -t lambda_agent -f Dockerfile --target lambda --build-arg code_version=0.0.2 --build-arg build_number=106 --platform=linux/amd64 .
```

If you have your own ECR repo used for testing you'll need to login first:
```shell
aws ecr get-login-password --region us-east-1 --profile <aws_profile> | docker login --username AWS --password-stdin <account_id>.dkr.ecr.us-east-1.amazonaws.com
```

And then tag/push your image:
```shell
docker tag lambda_agent:latest <account_id>.dkr.ecr.us-east-1.amazonaws.com/dev-agent:95
docker push <account_id>.dkr.ecr.us-east-1.amazonaws.com/dev-agent:95 
```

Deploy a new Lambda function using the image you pushed to your ECR repo and now you can
invoke the health endpoint using:
```shell
aws lambda invoke --profile <aws_profile> --function-name <lambda_arn> --cli-binary-format raw-in-base64-out --payload '{"path": "/api/v1/test/health", "httpMethod": "GET", "queryStringParameters": {"trace_id": "1234", "full": true}}' /dev/stdout | jq '.body | fromjson'
```
