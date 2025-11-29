from dataclasses import dataclass
from typing import Any, Dict, Optional

from dataclasses_json import DataClassJsonMixin


@dataclass
class OperationAttributes(DataClassJsonMixin):
    operation_id: str
    trace_id: str
    compress_response_file: bool
    response_size_limit_bytes: int
    job_type: Optional[str] = None


@dataclass
class AgentOperationResult:
    """
    Data class to represent the result of an operation
    """

    operation_id: str
    result: Optional[Dict[str, Any]] = None
    query_id: Optional[str] = None
    operation_attrs: Optional[OperationAttributes] = None
