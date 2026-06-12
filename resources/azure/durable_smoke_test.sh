#!/usr/bin/env bash
#
# Durable Functions smoke test for the Azure agent image.
#
# Proves that the SignalR-pruned image (see prune_signalr_extension.py) still
# boots the Azure Functions host cleanly and runs a Durable Functions
# orchestration end-to-end against an Azurite storage backend. This is the
# safety net for the prune: the host eagerly loads every registered extension
# at startup, so a bad removal surfaces as a host-startup failure or a stuck
# orchestration.
#
# Assertions (all hard):
#   1. The host process starts and stays up.
#   2. The startup logs contain NO load failure referencing the removed
#      SignalR / MessagePack assemblies.
#   3. A durable orchestration POSTed to the async execute endpoint reaches a
#      terminal runtime status — exercising the orchestration + activity
#      triggers and the AzureStorage backend (which is what SignalR removal
#      could have broken).
#
# Usage:
#   resources/azure/durable_smoke_test.sh [IMAGE_TAG]
#     IMAGE_TAG  optional pre-built image to test; if omitted the script builds
#                Dockerfile.azure as apollo-azure:smoke.
#
# Requires: docker (with linux/amd64 support), curl, python3.
set -euo pipefail

IMAGE="${1:-}"
PLATFORM="linux/amd64"
NET="apollo-azure-smoke-net"
AZURITE="apollo-azure-smoke-azurite"
APP="apollo-azure-smoke-app"
HOST_PORT="${SMOKE_HOST_PORT:-8080}"
BASE="http://localhost:${HOST_PORT}"

# Point the well-known development-storage account (devstoreaccount1) at the
# Azurite container via DevelopmentStorageProxyUri — the documented way to use
# dev storage on a non-localhost host. The SDK derives the blob/queue/table
# endpoints (ports 10000/10001/10002) from this proxy URI, which avoids the
# "No valid combination of account information found" parse error you get when
# hand-mixing AccountName with account-in-path endpoints.
AZURITE_CONN="UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://${AZURITE}"

cleanup() {
  docker rm -f "$APP" "$AZURITE" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() {
  echo "SMOKE TEST FAILED: $*" >&2
  echo "----- last 80 lines of host logs -----" >&2
  docker logs "$APP" 2>&1 | tail -80 >&2 || true
  exit 1
}

cd "$(git rev-parse --show-toplevel)"

if [[ -z "$IMAGE" ]]; then
  IMAGE="apollo-azure:smoke"
  echo "==> Building $IMAGE from Dockerfile.azure"
  docker build --platform "$PLATFORM" -f Dockerfile.azure -t "$IMAGE" .
fi

cleanup
docker network create "$NET" >/dev/null

echo "==> Starting Azurite storage emulator"
# --skipApiVersionCheck: the DurableTask.AzureStorage SDK sends an x-ms-version
# newer than this Azurite build advertises; without this flag Azurite rejects
# those requests and the durable client fails with a generic "error
# communicating with Azure Storage".
docker run -d --name "$AZURITE" --network "$NET" --platform "$PLATFORM" \
  mcr.microsoft.com/azure-storage/azurite \
  azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0 --skipApiVersionCheck >/dev/null

echo "==> Starting Azure Functions host ($IMAGE)"
# - MCD_AUTH_TYPE + WEBSITE_AUTH_* sentinels force AuthLevel.ANONYMOUS so the
#   durable endpoints are reachable without a function key (see auth.py).
# - APPLICATIONINSIGHTS_CONNECTION_STRING is a syntactically valid dummy so
#   configure_azure_monitor() initialises without trying to export telemetry.
docker run -d --name "$APP" --network "$NET" --platform "$PLATFORM" \
  -p "${HOST_PORT}:8080" \
  -e AzureWebJobsStorage="$AZURITE_CONN" \
  -e FUNCTIONS_WORKER_RUNTIME=python \
  -e AzureWebJobsSecretStorageType=files \
  -e WEBSITES_ENABLE_APP_SERVICE_STORAGE=false \
  -e WEBSITE_HOSTNAME="localhost:8080" \
  -e MCD_AUTH_TYPE=AZURE_FUNCTION_SERVICE_PRINCIPAL \
  -e WEBSITE_AUTH_ENABLED=true \
  -e WEBSITE_AUTH_CLIENT_ID=smoke \
  -e WEBSITE_AUTH_OPENID_ISSUER=smoke \
  -e APPLICATIONINSIGHTS_CONNECTION_STRING="InstrumentationKey=00000000-0000-0000-0000-000000000000;IngestionEndpoint=https://localhost/" \
  "$IMAGE" >/dev/null

echo "==> Waiting for the host to report ready"
ready=0
for _ in $(seq 1 60); do
  if ! docker ps -q --filter "name=$APP" | grep -q .; then
    fail "host container exited during startup"
  fi
  if docker logs "$APP" 2>&1 | grep -qiE "Worker process started and initialized|Host started|Job host started|Application started"; then
    ready=1
    break
  fi
  sleep 2
done
[[ $ready -eq 1 ]] || fail "host did not report ready within ~120s"
echo "    host is up"

# --- Assertion 2: no load errors for the removed SignalR/MessagePack assemblies ---
echo "==> Checking startup logs for SignalR/MessagePack load errors"
if docker logs "$APP" 2>&1 \
    | grep -iE "signalr|messagepack" \
    | grep -iqE "could not load|unable to load|filenotfound|failed to load|loaderexception|typeloadexception"; then
  fail "host logged a load error referencing the removed SignalR/MessagePack assemblies"
fi
echo "    no SignalR/MessagePack load errors"

# --- Assertion 3: a durable orchestration reaches a terminal state ---
echo "==> Triggering a durable orchestration"
resp="$(curl -fsS -X POST \
  "${BASE}/async/api/v1/agent/execute/storage/smoke_test" \
  -H 'Content-Type: application/json' \
  -d '{"operation": {"trace_id": "smoke", "commands": []}}')" \
  || fail "execute endpoint did not return success"

iid="$(printf '%s' "$resp" | python3 -c 'import sys, json; print(json.load(sys.stdin)["__mcd_request_id__"])')" \
  || fail "execute response missing __mcd_request_id__: $resp"
echo "    instance_id=$iid"

echo "==> Polling status until terminal"
status="unknown"
for _ in $(seq 1 60); do
  s="$(curl -fsS "${BASE}/async/api/v1/status/${iid}")" || { sleep 2; continue; }
  status="$(printf '%s' "$s" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("__mcd_status__", "unknown"))')"
  echo "    status=$status"
  case "$status" in
    Completed | Failed | Terminated) break ;;
  esac
  sleep 2
done

case "$status" in
  Completed | Failed | Terminated)
    echo "    durable orchestration reached terminal status '$status'"
    ;;
  *)
    fail "orchestration never reached a terminal status (last='$status') — durable runtime may be broken"
    ;;
esac

echo
echo "SMOKE TEST PASSED — host boots without SignalR and the Durable Functions runtime is healthy."
