# Environment variables reported back in the `/test/health` endpoint
IS_REMOTE_UPGRADABLE_ENV_VAR = "MCD_AGENT_IS_REMOTE_UPGRADABLE"
AGENT_IMAGE_TAG_ENV_VAR = "MCD_AGENT_IMAGE_TAG"
AGENT_WRAPPER_TYPE_ENV_VAR = "MCD_AGENT_WRAPPER_TYPE"

# CloudFormation Stack ID, used for updates, returned in health endpoint
CLOUDFORMATION_STACK_ID_ENV_VAR = "MCD_STACK_ID"

# CloudWatch Log Group ID
CLOUDWATCH_LOG_GROUP_ID_ENV_VAR = "MCD_LOG_GROUP_ID"

# AWS Function Name
AWS_LAMBDA_FUNCTION_NAME_ENV_VAR = "AWS_LAMBDA_FUNCTION_NAME"

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

# Environment variable used to control the expiration in seconds for the pre-signed URL responses
PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_ENV_VAR = (
    "MCD_PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS"
)

# Default value for expiration in seconds of pre-signed URL responses
PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_DEFAULT_VALUE = str(60 * 60 * 1)  # 1 hour

# Environment variable used to configure the bucket name for S3, GCS and Azure
STORAGE_BUCKET_NAME_ENV_VAR = "MCD_STORAGE_BUCKET_NAME"

# Environment variable used to configure the storage account name for Azure
STORAGE_ACCOUNT_NAME_ENV_VAR = "MCD_STORAGE_ACCOUNT_NAME"

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

# Orchestration activity timeout in seconds, should be less than 15 minutes to timeout
# before the function times out
ORCHESTRATION_ACTIVITY_TIMEOUT_ENV_VAR = "MCD_ORCHESTRATION_ACTIVITY_TIMEOUT"
ORCHESTRATION_ACTIVITY_TIMEOUT_DEFAULT_VALUE = 60 * 14 + 45  # 14:45 minutes

# used to mark last time the agent was updated, used to restart without updating the image
LAST_UPDATE_TS_ENV_VAR = "MCD_LAST_UPDATE_TS"

AZURE_MAX_ACTIVITY_FUNCTIONS_ENV_VAR = (
    "AzureFunctionsJobHost__extensions__durableTask__maxConcurrentActivityFunctions"
)
AZURE_MAX_ORCHESTRATOR_FUNCTIONS_ENV_VAR = (
    "AzureFunctionsJobHost__extensions__durableTask__maxConcurrentOrchestratorFunctions"
)

# used to configure the Gunicorn web server when running the agent image as a generic
# service instead of a function
GUNICORN_WORKERS_ENV_VAR = "GUNICORN_WORKERS"
GUNICORN_THREADS_ENV_VAR = "GUNICORN_THREADS"
GUNICORN_TIMEOUT_ENV_VAR = "GUNICORN_TIMEOUT"

MCD_AGENT_CLOUD_PLATFORM_ENV_VAR = "MCD_AGENT_CLOUD_PLATFORM"


HEALTH_ENV_VARS = [
    "PYTHON_VERSION",
    "SERVER_SOFTWARE",
    MCD_AGENT_CLOUD_PLATFORM_ENV_VAR,
    AGENT_WRAPPER_TYPE_ENV_VAR,
    "MCD_AGENT_WRAPPER_VERSION",
    IS_REMOTE_UPGRADABLE_ENV_VAR,
    AGENT_IMAGE_TAG_ENV_VAR,
    CLOUDFORMATION_STACK_ID_ENV_VAR,
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    AWS_LAMBDA_FUNCTION_NAME_ENV_VAR,
    "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
    "WEBSITE_HOSTNAME",
    "REGION_NAME",
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_ACCOUNT_NAME_ENV_VAR,
    "FUNCTIONS_WORKER_PROCESS_COUNT",
    "PYTHON_THREADPOOL_THREAD_COUNT",
    AZURE_MAX_ACTIVITY_FUNCTIONS_ENV_VAR,
    AZURE_MAX_ORCHESTRATOR_FUNCTIONS_ENV_VAR,
    LAST_UPDATE_TS_ENV_VAR,
]
