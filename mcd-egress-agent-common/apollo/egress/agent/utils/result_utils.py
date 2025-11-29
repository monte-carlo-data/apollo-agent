from typing import Dict, Any

from snowflake.connector import DatabaseError, ProgrammingError

from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_NAME_ERROR_TYPE,
)


class ResultUtils:
    @staticmethod
    def result_for_error_message(error_message: str) -> Dict[str, Any]:
        return {
            ATTRIBUTE_NAME_ERROR: error_message,
        }

    @staticmethod
    def result_for_exception(ex: Exception) -> Dict:
        result: Dict[str, Any] = {
            ATTRIBUTE_NAME_ERROR: str(ex) or f"Unknown error: {type(ex).__name__}",
        }
        if isinstance(ex, DatabaseError):
            result[ATTRIBUTE_NAME_ERROR_ATTRS] = {
                "errno": ex.errno,
                "sqlstate": ex.sqlstate,
            }
            if isinstance(ex, ProgrammingError):
                result[ATTRIBUTE_NAME_ERROR_TYPE] = "ProgrammingError"
            elif isinstance(ex, DatabaseError):
                result[ATTRIBUTE_NAME_ERROR_TYPE] = "DatabaseError"

        return result
