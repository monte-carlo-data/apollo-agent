# Apollo Agent with MinIO - Helm Chart

This Helm chart deploys Apollo Agent with MinIO as the storage backend on Kubernetes.

## Prerequisites

- Kubernetes cluster (tested with minikube)
- Helm 3.x
- kubectl configured to access your cluster

## Quick Start with Minikube

### 1. Start Minikube

```bash
minikube start
```

### 2. Build and Load Apollo Agent Image

```bash
# Build the image
docker build -t apollo-agent:local --target generic -f Dockerfile ..

# Load into minikube
minikube image load apollo-agent:local
```

Alternatively, if you have a registry:

```bash
# Tag and push to your registry
docker tag apollo-agent:local your-registry/apollo-agent:local
docker push your-registry/apollo-agent:local

# Update values.yaml with your registry image
```

### 3. Install the Chart

```bash
cd examples/helm/apollo-agent-minio
helm install apollo-agent-minio .
```

### 4. Wait for Services to be Ready

```bash
# Check pod status
kubectl get pods -w

# Wait for both MinIO and Apollo Agent to be running
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=apollo-agent-minio --timeout=300s
```

### 5. Access the Services

#### Option A: Port Forward (Recommended for Testing)

```bash
# Apollo Agent
kubectl port-forward svc/apollo-agent-minio-apollo-agent 8080:8080

# MinIO Console (optional)
kubectl port-forward svc/apollo-agent-minio-minio 9001:9001
```

#### Option B: Minikube Service

```bash
# Apollo Agent (uses NodePort 30080)
minikube service apollo-agent-minio-apollo-agent

# MinIO Console
minikube service apollo-agent-minio-minio --url
```

#### Option C: Direct NodePort Access

If using NodePort service type, access via:
- Apollo Agent: `http://$(minikube ip):30080`
- MinIO API: `http://$(minikube ip):<node-port>` (if exposed)
- MinIO Console: `http://$(minikube ip):<node-port>` (if exposed)

## Verification

### 1. Check Health Endpoint

```bash
curl http://localhost:8080/api/v1/test/health
```

Expected response:
```json
{
  "version": "local",
  "platform": "Generic",
  "env": {
    "MCD_STORAGE_BUCKET_NAME": "apollo-bucket",
    ...
  }
}
```

### 2. Test Storage Operations

#### List Objects

```bash
curl -X POST http://localhost:8080/api/v1/agent/execute/storage/list_objects \
  -H "Content-Type: application/json" \
  -d '{"operation": {"trace_id": "test-123", "commands": [{"method": "list_objects"}]}}'
```

#### Write File

```bash
curl -X POST http://localhost:8080/api/v1/agent/execute/storage/write \
  -H "Content-Type: application/json" \
  -d '{"operation": {"trace_id": "test-write", "commands": [{"method": "write", "kwargs": {"key": "test-file.txt", "obj_to_write": "Hello from Kubernetes!"}}]}}'
```

#### Read File

```bash
curl -X POST http://localhost:8080/api/v1/agent/execute/storage/read \
  -H "Content-Type: application/json" \
  -d '{"operation": {"trace_id": "test-read", "commands": [{"method": "read", "kwargs": {"key": "test-file.txt", "encoding": "utf-8"}}]}}'
```

### 3. Verify in MinIO Console

Access MinIO Console via port-forward:

```bash
kubectl port-forward svc/apollo-agent-minio-minio 9001:9001
```

Then open http://localhost:9001 and login with:
- Username: `minioadmin`
- Password: `minioadmin`

You should see the `apollo-bucket` with your test files.

## Configuration

### Customizing Values

Edit `values.yaml` or override values during installation:

```bash
helm install apollo-agent-minio . \
  --set minio.rootPassword=my-secure-password \
  --set apolloAgent.env.storageBucketName=my-bucket
```

### Key Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `minio.rootUser` | MinIO root user | `minioadmin` |
| `minio.rootPassword` | MinIO root password | `minioadmin` |
| `minio.persistence.size` | Storage size for MinIO | `10Gi` |
| `apolloAgent.image.repository` | Apollo Agent image | `apollo-agent` |
| `apolloAgent.image.tag` | Apollo Agent image tag | `local` |
| `apolloAgent.env.storageBucketName` | Bucket name | `apollo-bucket` |
| `apolloAgent.service.type` | Service type | `NodePort` |
| `apolloAgent.service.nodePort` | NodePort for Apollo Agent | `30080` |

## Troubleshooting

### Check Pod Logs

```bash
# Apollo Agent logs
kubectl logs -l app.kubernetes.io/component=apollo-agent

# MinIO logs
kubectl logs -l app.kubernetes.io/component=minio

# Bucket init job logs
kubectl logs -l app.kubernetes.io/component=bucket-init
```

### Check Pod Status

```bash
kubectl get pods
kubectl describe pod <pod-name>
```

### Verify Services

```bash
kubectl get svc
kubectl get endpoints
```

### Test MinIO Connectivity from Apollo Agent Pod

```bash
# Get Apollo Agent pod name
APOLLO_POD=$(kubectl get pod -l app.kubernetes.io/component=apollo-agent -o jsonpath='{.items[0].metadata.name}')

# Test MinIO connectivity
kubectl exec $APOLLO_POD -- curl http://apollo-agent-minio-minio:9000/minio/health/live
```

### Recreate Bucket (if needed)

```bash
# Delete the bucket init job
kubectl delete job apollo-agent-minio-bucket-init

# Manually create bucket using MinIO client
kubectl run -it --rm mc --image=minio/mc --restart=Never -- \
  sh -c "mc alias set minio http://apollo-agent-minio-minio:9000 minioadmin minioadmin && mc mb minio/apollo-bucket"
```

## Uninstallation

```bash
helm uninstall apollo-agent-minio
```

To also remove persistent volumes (this will delete all MinIO data):

```bash
helm uninstall apollo-agent-minio
kubectl delete pvc -l app.kubernetes.io/name=apollo-agent-minio
```

## Production Considerations

For production deployments, consider:

1. **Security**:
   - Use Kubernetes Secrets for credentials
   - Enable TLS for MinIO
   - Use proper RBAC policies

2. **High Availability**:
   - Deploy MinIO in distributed mode
   - Use StatefulSet for MinIO
   - Configure multiple Apollo Agent replicas

3. **Monitoring**:
   - Add Prometheus metrics
   - Configure logging aggregation
   - Set up health check alerts

4. **Storage**:
   - Use appropriate storage classes
   - Configure backup strategies
   - Monitor disk usage

