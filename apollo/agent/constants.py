# Name of the attribute used in the operation result to include the error message, not included if no error
ATTRIBUTE_NAME_ERROR = "__mcd_error__"

# Name of the attribute used in the operation result to include the error type, if any
ATTRIBUTE_NAME_ERROR_TYPE = "__mcd_error_type__"

# Name of the attribute used in the operation result to include the error attributes, if any
ATTRIBUTE_NAME_ERROR_ATTRS = "__mcd_error_attrs__"

# Name of the attribute used in the operation result to include the exception text, if any
ATTRIBUTE_NAME_EXCEPTION = "__mcd_exception__"

# Name of the attribute used in the operation result to include the stack trace for the error, if any
ATTRIBUTE_NAME_STACK_TRACE = "__mcd_stack_trace__"

# Name of the attribute used in the operation result to return the trace id from the request
ATTRIBUTE_NAME_TRACE_ID = "__mcd_trace_id__"

# Name of the attribute used in the operation response to wrap the result, not included if there was an error
ATTRIBUTE_NAME_RESULT = "__mcd_result__"

# Name of the attribute used in the operation response to wrap the result location, not included if there was an error
ATTRIBUTE_NAME_RESULT_LOCATION = "__mcd_result_location__"

# Name of the attribute used in the operation response to indicate if the response is compressed or not
ATTRIBUTE_NAME_RESULT_COMPRESSED = "__mcd_result_compressed__"

# Name of the attribute used in call arguments to reference a local variable in the context
ATTRIBUTE_NAME_REFERENCE = "__reference__"

# Name of the attribute used to indicate some special types in arguments or serialized results, like `call`,
# `bytes`, etc.
ATTRIBUTE_NAME_TYPE = "__type__"

# Name of the attribute used to include data for special types like `bytes`
ATTRIBUTE_NAME_DATA = "__data__"

# Value for the attribute __type__ to indicate this is a call to another method, used to pass calls as arguments
# For example when you call `_client.method_a(_client.method_b())`, the call to `method_b` is passed in the list
# of args for `method_a`.
ATTRIBUTE_VALUE_TYPE_CALL = "call"

# Value for the attribute __type__ to indicate this is a bytes array, data is encoded in base64
ATTRIBUTE_VALUE_TYPE_BYTES = "bytes"

# Value for the attribute __type__ to indicate this is a datetime
ATTRIBUTE_VALUE_TYPE_DATETIME = "datetime"

# Value for the attribute __type__ to indicate this is a date
ATTRIBUTE_VALUE_TYPE_DATE = "date"

# Value for the attribute __type__ to indicate this is a decimal
ATTRIBUTE_VALUE_TYPE_DECIMAL = "decimal"

# Value for the attribute __type__ to indicate this is a Looker category enum, client side it will be converted to
# the right type
ATTRIBUTE_VALUE_TYPE_LOOKER_CATEGORY = "looker.category"

# Value for the attribute __type__ to indicate this is an Oracle DbType.
ATTRIBUTE_VALUE_TYPE_ORACLE_DB_TYPE = "oracle.db_type"

# Value to use when redacting sensitive data in log messages
ATTRIBUTE_VALUE_REDACTED = "__redacted__"

# Attribute name for trace id in log messages
LOG_ATTRIBUTE_TRACE_ID = "mcd_trace_id"

# Attribute name for operation name in log messages
LOG_ATTRIBUTE_OPERATION_NAME = "mcd_operation_name"

# Name of the client variable in the evaluation context, this is the default value for `target` in calls
CONTEXT_VAR_CLIENT = "_client"

# Name of the variable holding the `utils` object that can be used for calls like `__utils.build_dict()`
CONTEXT_VAR_UTILS = "__utils"

# Name of the function that will be called as the entry point of an agent script
AGENT_SCRIPT_ENTRYPOINT = "execute_script_handler"

# List of modules available for agent scripts
AGENT_SCRIPT_BUILTIN_MODULES = [
    "typing",
    "json",
    "re",
    "dataclasses",
    "time",
    "datetime",
    "decimal",
    "uuid",
    "ipaddress",
]

# Header used to return the trace if received in the request when the result is binary
TRACE_ID_HEADER = "x-mcd-trace-id"

# Platform names
PLATFORM_GENERIC = "Generic"
PLATFORM_GCP = "GCP"
PLATFORM_AWS = "AWS"
PLATFORM_AZURE = "Azure"

# Storage types
STORAGE_TYPE_S3 = "S3"
STORAGE_TYPE_GCS = "GCS"
STORAGE_TYPE_AZURE = "AZURE_BLOB"

# Response types
RESPONSE_TYPE_JSON = "json"
RESPONSE_TYPE_URL = "url"

# Connection types
CONNECTION_TYPE_BIGQUERY = "bigquery"
CONNECTION_TYPE_DATABRICKS = "databricks"
CONNECTION_TYPE_HTTP = "http"
CONNECTION_TYPE_STORAGE = "storage"
CONNECTION_TYPE_LOOKER = "looker"
CONNECTION_TYPE_GIT = "git"
CONNECTION_TYPE_REDSHIFT = "redshift"
CONNECTION_TYPE_POSTGRES = "postgres"
CONNECTION_TYPE_SQL_SERVER = "sql-server"
CONNECTION_TYPE_SNOWFLAKE = "snowflake"
CONNECTION_TYPE_MYSQL = "mysql"
CONNECTION_TYPE_ORACLE = "oracle"
CONNECTION_TYPE_TERADATA = "teradata"
CONNECTION_TYPE_AZURE_DEDICATED_SQL_POOL = "azure-dedicated-sql-pool"
CONNECTION_TYPE_AZURE_SQL_DATABASE = "azure-sql-database"
CONNECTION_TYPE_TABLEAU = "tableau"
CONNECTION_TYPE_SAP_HANA = "sap-hana"
CONNECTION_TYPE_POWER_BI = "power-bi"
CONNECTION_TYPE_GLUE = "glue"
CONNECTION_TYPE_ATHENA = "athena"
CONNECTION_TYPE_PRESTO = "presto"
CONNECTION_TYPE_HIVE = "hive"
CONNECTION_TYPE_MSK_CONNECT = "msk-connect"
CONNECTION_TYPE_MSK_KAFKA = "msk-kafka"

CONNECTION_TYPES = (
    CONNECTION_TYPE_BIGQUERY,
    CONNECTION_TYPE_DATABRICKS,
    CONNECTION_TYPE_HTTP,
    CONNECTION_TYPE_STORAGE,
    CONNECTION_TYPE_LOOKER,
    CONNECTION_TYPE_GIT,
    CONNECTION_TYPE_REDSHIFT,
    CONNECTION_TYPE_POSTGRES,
    CONNECTION_TYPE_SQL_SERVER,
    CONNECTION_TYPE_SNOWFLAKE,
    CONNECTION_TYPE_MYSQL,
    CONNECTION_TYPE_ORACLE,
    CONNECTION_TYPE_TERADATA,
    CONNECTION_TYPE_AZURE_DEDICATED_SQL_POOL,
    CONNECTION_TYPE_AZURE_SQL_DATABASE,
    CONNECTION_TYPE_TABLEAU,
    CONNECTION_TYPE_SAP_HANA,
    CONNECTION_TYPE_POWER_BI,
    CONNECTION_TYPE_GLUE,
    CONNECTION_TYPE_ATHENA,
    CONNECTION_TYPE_PRESTO,
    CONNECTION_TYPE_HIVE,
    CONNECTION_TYPE_MSK_CONNECT,
    CONNECTION_TYPE_MSK_KAFKA,
)
