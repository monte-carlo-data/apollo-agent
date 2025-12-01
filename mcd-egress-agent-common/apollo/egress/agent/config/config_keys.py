# enable/disable connection pool
CONFIG_USE_CONNECTION_POOL = "USE_CONNECTION_POOL"
# connection pool size
CONFIG_CONNECTION_POOL_SIZE = "CONNECTION_POOL_SIZE"
# number of threads used to execute queries
CONFIG_QUERIES_RUNNER_THREAD_COUNT = "QUERIES_RUNNER_THREAD_COUNT"
# number of threads used to execute operations (like storage, health, etc.)
CONFIG_OPS_RUNNER_THREAD_COUNT = "OPS_RUNNER_THREAD_COUNT"
# number of threads used to publish results to MC backend
CONFIG_PUBLISHER_THREAD_COUNT = "PUBLISHER_THREAD_COUNT"
# how to run queries, by default we run them asynchronously, set this to `true` to run them synchronously
# you should update the size of the connection pool and the number of query runners accordingly
CONFIG_USE_SYNC_QUERIES = "USE_SYNC_QUERIES"
# name of the stage to use to store files
CONFIG_STAGE_NAME = "STAGE_NAME"
# expiration seconds for pre-signed urls used for responses
CONFIG_PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS = (
    "PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS"
)
# whether the agent accepts config changes remotely or not
CONFIG_IS_REMOTE_UPGRADABLE = "IS_REMOTE_UPGRADABLE"
# interval to send ACK messages in seconds
CONFIG_ACK_INTERVAL_SECONDS = "ACK_INTERVAL_SECONDS"
# interval to push logs in seconds
CONFIG_PUSH_LOGS_INTERVAL_SECONDS = "PUSH_LOGS_INTERVAL_SECONDS"
# name of the warehouse to use to execute queries
CONFIG_WAREHOUSE_NAME = "WAREHOUSE_NAME"
# JSON string with job types configuration mapping job types to warehouses and configuring pool size
CONFIG_JOB_TYPES = "JOB_TYPES"
