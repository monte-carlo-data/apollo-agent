# Docker Compose Setup for Apollo Agent with MinIO

This docker-compose file sets up Apollo Agent with MinIO as the storage backend.

## Prerequisites

- Docker
- Docker Compose

## Quick Start

1. Build and start the services:
   ```bash
   docker-compose up -d
   ```

2. Create a bucket in MinIO:
   ```bash
   # Using MinIO client (mc)
   docker run --rm -it --network examples_apollo-network \
     -e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
     minio/mc mb minio/apollo-bucket
   
   # Or use the MinIO Console UI at http://localhost:9001
   # Login with: minioadmin / minioadmin
   # Then create a bucket named "apollo-bucket"
   ```

3. Verify the setup:
   ```bash
   # Check if apollo-agent is running
   curl http://localhost:8080/api/v1/agent/health
   
   # Test storage operations
   curl -X POST http://localhost:8080/api/v1/agent/execute/storage/list_objects \
     -H "Content-Type: application/json" \
     -d '{"operation": {"trace_id": "test-123", "commands": [{"method": "list_objects"}]}}'
   ```

## Services

### MinIO
- **API Port**: 9000
- **Console Port**: 9001
- **Default Credentials**: 
  - Access Key: `minioadmin`
  - Secret Key: `minioadmin`
- **Console URL**: http://localhost:9001

### Apollo Agent
- **Port**: 8080
- **API Endpoint**: http://localhost:8080/api/v1/agent/execute/{connection_type}/{operation_name}
- **Health Check**: http://localhost:8080/api/v1/agent/health

## Environment Variables

### Apollo Agent Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `MCD_STORAGE` | Storage type | `MINIO` |
| `MCD_STORAGE_BUCKET_NAME` | MinIO bucket name | `apollo-bucket` |
| `MCD_STORAGE_PREFIX` | Storage prefix for files | `mcd` |
| `MCD_MINIO_ENDPOINT_URL` | MinIO server endpoint | `http://minio:9000` |
| `MCD_MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MCD_MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `PORT` | Gunicorn port | `8080` |
| `MCD_AGENT_CLOUD_PLATFORM` | Platform type | `Generic` |

## Customization

### Change MinIO Credentials

Update the environment variables in `docker-compose.yml`:

```yaml
minio:
  environment:
    MINIO_ROOT_USER: your-access-key
    MINIO_ROOT_PASSWORD: your-secret-key

apollo-agent:
  environment:
    MCD_MINIO_ACCESS_KEY: your-access-key
    MCD_MINIO_SECRET_KEY: your-secret-key
```

### Change Bucket Name

Update `MCD_STORAGE_BUCKET_NAME` in the apollo-agent service and create the bucket accordingly.

### Use External MinIO

If you want to use an external MinIO instance, update `MCD_MINIO_ENDPOINT_URL`:

```yaml
apollo-agent:
  environment:
    MCD_MINIO_ENDPOINT_URL: https://your-minio-server.com
```

## Stopping Services

```bash
docker-compose down
```

To also remove volumes (this will delete all MinIO data):

```bash
docker-compose down -v
```

## Troubleshooting

### Check Logs

```bash
# Apollo Agent logs
docker-compose logs apollo-agent

# MinIO logs
docker-compose logs minio
```

### Verify MinIO is Accessible

```bash
# From within the apollo-agent container
docker-compose exec apollo-agent curl http://minio:9000/minio/health/live
```

### Create Bucket via API

If you need to create the bucket programmatically:

```bash
# Install MinIO client in a temporary container
docker run --rm -it --network examples_apollo-network \
  -e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
  minio/mc mb minio/apollo-bucket
```

