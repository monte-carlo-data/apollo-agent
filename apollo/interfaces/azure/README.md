### Required steps to manually deploy a new Azure Function
Create resource group
```shell
az group create --name <RESOURCE_GROUP> --location <LOCATION>
```

Create storage (lowercase, no _- support)
```shell
az storage account create --name <STORAGE_NAME> --location <LOCATION> --resource-group <RESOURCE_GROUP> --sku Standard_LRS
```

Create Registry (lowercase, no _- support) and login (required only if not using dockerhub):
```shell
az acr create --resource-group <RESOURCE_GROUP> --name <REGISTRY> --sku Basic
az acr login --name <REGISTRY>
```

Docker build and tag
```shell
docker build -t agent -f Dockerfile_az --platform linux/amd64 .
docker tag agent <REGISTRY>.azurecr.io/mcd-agent/agent
```

Docker push:
- If using dockerhub: the easiest way is to push to dev and circleci will automatically push to dockerhub, but you can manually push it:
  ```shell
  docker push docker.io/montecarlodata/pre-release-agent:latest-azure
  ```
- If using Azure registry:
  ```shell
  docker push <REGISTRY>.azurecr.io/mcd-agent/agent
  ```

Create plan:
```shell
az functionapp plan create --resource-group <RESOURCE_GROUP> --name <PLAN_NAME> --location <LOCATION> --number-of-workers 1 --sku EP1 --is-linux
```

Create function:
- If using dockerhub:
  ```shell
  az functionapp create --name <FUNCTION_NAME> --storage-account <STORAGE_NAME> --resource-group <RESOURCE_GROUP> --image "docker.io/montecarlodata/pre-release-agent:latest-azure" --functions-version 4 --runtime python --runtime-version 3.12 --plan <PLAN_NAME> --os-type linux
  ```
- If using Azure Registry:
  ```shell
  az functionapp create --name <FUNCTION_NAME> --storage-account <STORAGE_NAME> --resource-group <RESOURCE_GROUP> --image "<REGISTRY>.azurecr.io/mcd-agent/agent:latest" --registry-password <REGISTRY_PWD> --registry-username <REGISTRY> --functions-version 4 --runtime python --runtime-version 3.12 --plan <PLAN_NAME> --os-type linux
  ```

Get key to access function:
```shell
func azure functionapp list-functions <FUNCTION_NAME> --show-keys
```

Assign a managed identity to the function, first create a user-managed identity:
```shell
az identity create --resource-group <RESOURCE_GROUP> --name <IDENTITY_NAME> 
```
and then assign it to the function:
```shell
az webapp identity assign --resource-group <RESOURCE_GROUP> --name <FUNCTION_NAME> --identities <IDENTITY_RESOURCE_FULL_ID>
```

After updating the docker image:
- If using dockerhub:
  ```shell
  az functionapp config container set --image "docker.io/montecarlodata/pre-release-agent:0.2.2rc564-azure" --resource-group <RESOURCE_GROUP> --name <FUNCTION_NAME>
  ```
- If using Azure Registry:
  ```shell
  az functionapp config container set --image "<REGISTRY>.azurecr.io/mcd-agent/agent:latest"  --registry-password <REGISTRY_PWD> --registry-username <REGISTRY>  --resource-group <RESOURCE_GROUP> --name <FUNCTION_NAME>
  ```

Updating the image using Azure Resource Manager
- First get a token using `az`:
  ```shell
  export TOKEN=`az account get-access-token --query accessToken --output tsv`
  ```
- Then use the ARM endpoint to update the `linuxFxVersion` attribute:  
  ```shell
  curl -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -X PATCH "https://management.azure.com/subscriptions/<subscription_id>/resourceGroups/<resource_group_name>/providers/Microsoft.Web/sites/<function_name>?api-version=2022-03-01" -d '{"properties": {"siteConfig": {"linuxFxVersion": "DOCKER|docker.io/montecarlodata/pre-release-agent:0.2.4rc706-azure"}}}'
  ```
- To update/add environment variables:
  ```shell
  curl -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -X PATCH "https://management.azure.com/subscriptions/<subscription_id>/resourceGroups/<resource_group_name>/providers/Microsoft.Web/sites/<function_name>/config/appsettings?api-version=2022-03-01" -d '{"properties": {"test_env_var": "1234"}}'
  ```

### Authentication Modes

The Azure Function supports two authentication modes, controlled by the `MCD_AUTH_TYPE` environment variable:

**Function Key (default)**

When `MCD_AUTH_TYPE` is unset or set to `AZURE_FUNCTION_APP_KEY`, the function requires a valid function key passed in the `x-functions-key` header. This is the default behavior.

**Service Principal (Easy Auth)**

When `MCD_AUTH_TYPE=AZURE_FUNCTION_SERVICE_PRINCIPAL`, the function sets `AuthLevel.ANONYMOUS` and relies on Azure Easy Auth (App Service Authentication) to validate Bearer tokens. The DC sends a Bearer token instead of a function key.

Before starting, the function validates that Easy Auth is actually configured by checking these platform-injected environment variables:
- `WEBSITE_AUTH_ENABLED` — must be `True` (read-only, set by Azure when Easy Auth is enabled)
- `WEBSITE_AUTH_CLIENT_ID` — must be present (set by Azure during Easy Auth setup)
- `WEBSITE_AUTH_OPENID_ISSUER` — must be present (set by Azure during Easy Auth setup)

If any of these are missing or invalid, the function **refuses to start** with a `RuntimeError` — this fail-closed design prevents the function from running unauthenticated if Easy Auth was not properly configured.

To enable Easy Auth on a Function App, configure Authentication in the Azure portal or via ARM/Bicep with a Microsoft Entra ID provider.

**Enforcement Verification (Self-Call)**

In addition to checking environment variables at startup, the health endpoint (`/api/v1/test/health`) performs a runtime verification when `MCD_AUTH_TYPE=AZURE_FUNCTION_SERVICE_PRINCIPAL`: it makes an unauthenticated HTTP request to itself and confirms that Easy Auth rejects it (401/403). If the request is not rejected, the health endpoint returns 503 — this fails the DC's reachability check and blocks the deployment. Successful verification is cached for the process lifetime; failed results are retried on each health call.

To avoid infinite recursion (the probe request would itself trigger another verification), the probe includes a per-process random token in the `X-MCD-EasyAuth-Probe` header. The health handler recognises the token and short-circuits with a minimal `200` response before running any verification logic. Because the token is generated at import time via `secrets.token_hex()`, only the current process can produce a valid probe — external callers cannot forge the header to bypass verification.
