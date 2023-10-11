# Environment variables reported back in the `/test/health` endpoint
HEALTH_ENV_VARS = [
    "PYTHON_VERSION",
    "SERVER_SOFTWARE",
    "MCD_AGENT_IMAGE_TAG",
    "MCD_AGENT_CLOUD_PLATFORM",
    "MCD_AGENT_WRAPPER_TYPE",
    "MCD_AGENT_WRAPPER_VERSION",
    "MCD_AGENT_IS_REMOTE_UPGRADABLE",
]

# Environment variable used in the `Generic` platform to select the storage type
STORAGE_TYPE_ENV_VAR = "MCD_STORAGE"

# Environment variable used to control the expiration in seconds for the clients cache
CLIENT_CACHE_EXPIRATION_SECONDS_ENV_VAR = "MCD_CLIENT_CACHE_EXPIRATION_SECONDS"

# Environment variable used to initialize Flask application with debug=True and to set log level to debug
# Used only by the generic interface when `interfaces/generic/main.py` is executed.
DEBUG_ENV_VAR = "MCD_DEBUG"
