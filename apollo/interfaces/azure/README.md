Create resource group
```shell
az group create --name <RESOURCE_GROUP> --location <LOCATION>
```

Create storage (lowercase, no _- support)
```shell
az storage account create --name <STORAGE_NAME> --location <LOCATION> --resource-group <RESOURCE_GROUP> --sku Standard_LRS
```

Create Registry (lowercase, no _- support) and login
```shell
az acr create --resource-group <RESOURCE_GROUP> --name <REGISTRY> --sku Basic
az acr login --name <REGISTRY>
```

Docker build, tag and push
```shell
docker build -t agent -f Dockerfile_az --platform linux/amd64 .
docker tag agent <REGISTRY>.azurecr.io/mcd-agent/agent
docker push <REGISTRY>.azurecr.io/mcd-agent/agent
```

Create plan
```shell
az functionapp plan create --resource-group <RESOURCE_GROUP> --name <PLAN_NAME> --location <LOCATION> --number-of-workers 1 --sku EP1 --is-linux
```

Create function
```shell
az functionapp create --name <FUNCTION_NAME> --storage-account <STORAGE_NAME> --resource-group <RESOURCE_GROUP> --image "<REGISTRY>.azurecr.io/mcd-agent/agent:latest" --registry-password <REGISTRY_PWD> --registry-username <REGISTRY> --functions-version 4 --runtime python --runtime-version 3.11 --plan <PLAN_NAME> --os-type linux
```

Get key to access function
```shell
func azure functionapp list-functions <FUNCTION_NAME> --show-keys
```

Is this actually required? it is according to docs but everything seems to be working fine without it
```shell
az functionapp config appsettings set --name <FUNCTION_NAME> --resource-group <RESOURCE_GROUP> --settings AzureWebJobsFeatureFlags=EnableWorkerIndexing
```

After updating the docker image:
```shell
az functionapp config container set --image "<REGISTRY>.azurecr.io/mcd-agent/agent:latest"  --registry-password <REGISTRY_PWD> --registry-username <REGISTRY>  --resource-group <RESOURCE_GROUP> --name <FUNCTION_NAME>
```
