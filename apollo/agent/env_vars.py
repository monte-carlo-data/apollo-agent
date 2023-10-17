# Environment variables reported back in the `/test/health` endpoint
IS_REMOTE_UPGRADABLE_ENV_VAR = "MCD_AGENT_IS_REMOTE_UPGRADABLE"
HEALTH_ENV_VARS = [
    "PYTHON_VERSION",
    "SERVER_SOFTWARE",
    "MCD_AGENT_IMAGE_TAG",
    "MCD_AGENT_CLOUD_PLATFORM",
    "MCD_AGENT_WRAPPER_TYPE",
    "MCD_AGENT_WRAPPER_VERSION",
    IS_REMOTE_UPGRADABLE_ENV_VAR,
]

# Environment variable used in the `Generic` platform to select the storage type
STORAGE_TYPE_ENV_VAR = "MCD_STORAGE"

# Environment variable used to control the expiration in seconds for the clients cache
CLIENT_CACHE_EXPIRATION_SECONDS_ENV_VAR = "MCD_CLIENT_CACHE_EXPIRATION_SECONDS"

# Environment variable used to configure the bucket name for both S3 and GCS
STORAGE_BUCKET_NAME_ENV_VAR = "MCD_STORAGE_BUCKET_NAME"

# Environment variable used to initialize Flask application with debug=True and to set log level to debug
# Used only by the generic interface when `interfaces/generic/main.py` is executed.
DEBUG_ENV_VAR = "MCD_DEBUG"

TEMP_PATH_ENV_VAR = "MCD_TEMP_FOLDER"
DEFAULT_TEMP_PATH = "/tmp"
