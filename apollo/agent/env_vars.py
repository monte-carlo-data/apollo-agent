# Environment variables reported back in the `/test/health` endpoint
IS_REMOTE_UPGRADABLE_ENV_VAR = "MCD_AGENT_IS_REMOTE_UPGRADABLE"
AGENT_IMAGE_TAG_ENV_VAR = "MCD_AGENT_IMAGE_TAG"
AGENT_WRAPPER_TYPE_ENV_VAR = "MCD_AGENT_WRAPPER_TYPE"
HEALTH_ENV_VARS = [
    "PYTHON_VERSION",
    "SERVER_SOFTWARE",
    "MCD_AGENT_CLOUD_PLATFORM",
    AGENT_WRAPPER_TYPE_ENV_VAR,
    "MCD_AGENT_WRAPPER_VERSION",
    IS_REMOTE_UPGRADABLE_ENV_VAR,
    AGENT_IMAGE_TAG_ENV_VAR,
]

WRAPPER_TYPE_CLOUDFORMATION = "CLOUDFORMATION"

# Environment variable used in the `Generic` platform to select the storage type
STORAGE_TYPE_ENV_VAR = "MCD_STORAGE"

# Environment variable used to set the prefix for storage files, all files will be stored inside the
# folder specified by this variable. Set to empty or "/" to disable, it defaults to `STORAGE_PREFIX_DEFAULT_VALUE`.
STORAGE_PREFIX_ENV_VAR = "MCD_STORAGE_PREFIX"

# Default value for storage prefix, all files will be stored inside a folder with name "mcd"
STORAGE_PREFIX_DEFAULT_VALUE = "mcd"

# Environment variable used to control the expiration in seconds for the clients cache
CLIENT_CACHE_EXPIRATION_SECONDS_ENV_VAR = "MCD_CLIENT_CACHE_EXPIRATION_SECONDS"

# Environment variable used to configure the bucket name for both S3 and GCS
STORAGE_BUCKET_NAME_ENV_VAR = "MCD_STORAGE_BUCKET_NAME"

# Environment variable used to initialize Flask application with debug=True and to set log level to debug
# Used only by the generic interface when `interfaces/generic/main.py` is executed.
DEBUG_ENV_VAR = "MCD_DEBUG"
DEBUG_LOG_ENV_VAR = "MCD_DEBUG_LOG"

TEMP_PATH_ENV_VAR = "MCD_TEMP_FOLDER"
DEFAULT_TEMP_PATH = "/tmp"

# URL used to retrieve the public IP address of the agent
# It must return just the IP address, other urls are: https://ifconfig.me or https://ident.me
CHECK_OUTBOUND_IP_ADDRESS_URL_ENV_VAR = "MCD_CHECK_OUTBOUND_IP_URL"
CHECK_OUTBOUND_IP_ADDRESS_URL_DEFAULT_VALUE = "https://checkip.amazonaws.com"

# Cloud Formation Stack ID, used for updates
CLOUD_FORMATION_STACK_ID_ENV_VAR = "MCD_STACK_ID"
