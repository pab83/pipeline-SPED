from typing import Any, List, Optional
from enum import Enum

from pydantic import BaseModel


# ---------------------------
# ENUMS
# ---------------------------
class Status(str, Enum):
    SUCCESS = "success"
    ERROR = "error"


# ---------------------------
# ERROR STRUCTURE
# ---------------------------
class ErrorInfo(BaseModel):
    type: str
    message: str
    retryable: bool = True


# ---------------------------
# RESULT MESSAGE
# ---------------------------
class ResultMessage(BaseModel):
    message_id: str
    correlation_id: str
    schema_version: str = "1.0"
    model: str
    status: Status
    processing_time_ms: Optional[int] = None

    # Success fields
    result: Optional[Any] = None
    embedding: Optional[List[float]] = None
    embedding_dim: Optional[int] = None
    content_hash: Optional[str] = None

    # Error field
    error: Optional[ErrorInfo] = None