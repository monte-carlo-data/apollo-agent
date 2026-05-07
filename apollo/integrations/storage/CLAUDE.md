# Storage — Backend Abstraction and Factory

Three files; one public entry point.

## File responsibilities

- **`base_storage_client.py`** — Abstract `BaseStorageClient` contract every backend must
  implement. Core methods: `write`, `read`, `delete`, `download_file`, `upload_file`,
  `managed_download`, `read_json`, `read_many_json`, `list_objects`,
  `generate_presigned_url`, `is_bucket_private`. The base class also handles prefix
  application/stripping (`_apply_prefix`, `_remove_prefix*`) so backends don't repeat
  that logic.

- **`factory.py`** — Sole entry point for constructing a `BaseStorageClient`. Resolves the
  backend type and prefix from environment, then lazily imports and instantiates the
  correct class. No other module should construct a storage client directly.

- **`storage_proxy_client.py`** — `BaseProxyClient`-shaped wrapper used when the agent's
  connection type is `"storage"`. Delegates to an inner `BaseStorageClient`. Adapts the
  interface slightly: `list_objects` returns `{"list": ..., "page_token": ...}` instead of
  a tuple; `generate_presigned_url` accepts `expiration` as seconds (int) rather than
  `timedelta`.

## Backend resolution (factory.py)

Priority order inside `get_storage_client(platform=None)`:

1. `MCD_STORAGE` env var — `S3`, `GCS`, `AZURE_BLOB`, or `S3_COMPATIBLE` (highest priority).
2. Platform default (used only when `platform` arg is provided):
   - `aws` / `aws-generic` → S3
   - `gcp` → GCS
   - `azure` → Azure Blob
3. Neither set → raises `AgentConfigurationError`.

Prefix comes from `MCD_STORAGE_PREFIX` (default `"mcd"`). An empty string or `"/"` collapses
to no prefix.

## Public entry point

`get_storage_client(platform=None)` is the only sanctioned constructor. Two callers today:

- `StorageProxyClient.__init__` — the agent's `"storage"` connection-type proxy.
- `HttpProxyClient.download_to_storage` — streams an HTTP download into the configured
  backend without constructing a full `StorageProxyClient`.

## Lazy SDK imports

Each backend's cloud SDK is imported only inside its dispatch branch in `get_storage_client`.
This avoids loading `azure-storage-blob` and `google-cloud-storage` into an agent that only
uses S3.

## Adding a new storage backend

1. Implement `BaseStorageClient` in a new module under `apollo/integrations/<backend>/`.
2. Add a storage-type constant in `apollo/common/agent/constants.py`
   (e.g. `STORAGE_TYPE_FOO = "FOO"`).
3. Add a dispatch branch in `get_storage_client` using a lazy import (follow the existing
   `STORAGE_TYPE_S3_COMPATIBLE` branch as the pattern).
4. Optionally add an entry to `_DEFAULT_PLATFORM_STORAGE` if a platform should resolve to
   the new backend by default.
