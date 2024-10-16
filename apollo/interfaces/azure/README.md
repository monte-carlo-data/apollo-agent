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
