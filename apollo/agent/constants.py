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
