from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
from enum import Enum

# ---------------------------
# ENUMS
# ---------------------------
class TargetModel(str, Enum):
    OCR = "ocr"
    MOONDREAM = "moondream"
    EMBEDDINGS = "embeddings"

# ---------------------------
# TASK MESSAGE
# ---------------------------
class TaskMessage(BaseModel):
    message_id: str
    correlation_id: str
    schema_version: str = "1.0"
    timestamp: datetime
    source: str
    target_model: TargetModel
    retry_count: int = 0
    max_retries: int = 3
    payload: Dict[str, Any]