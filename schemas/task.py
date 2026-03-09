from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
from enum import Enum

# ---------------------------
# ENUMS
# ---------------------------
class TargetModel(str, Enum):
    """ Enum que define los posibles modelos o fases a los que una tarea puede estar dirigida dentro del pipeline. Cada valor del enum representa un módulo o fase específica del pipeline, como OCR, Moondream o Embeddings. Este enum se utiliza en el TaskMessage para indicar claramente qué modelo o fase debe ejecutar la tarea, lo que ayuda a organizar y dirigir las tareas de manera eficiente dentro del sistema."""
    OCR = "ocr"
    MOONDREAM = "moondream"
    EMBEDDINGS = "embeddings"

# ---------------------------
# TASK MESSAGE
# ---------------------------
class TaskMessage(BaseModel):
    """ Estructura del mensaje que representa una tarea a ejecutar en el pipeline. Este modelo define los campos necesarios para describir una tarea, incluyendo identificadores únicos, información de tiempo, el modelo objetivo para la tarea, y un payload con los datos específicos que la tarea necesita para su ejecución. El campo retry_count se utiliza para llevar un seguimiento de cuántas veces se ha intentado ejecutar la tarea, mientras que max_retries define el número máximo de intentos permitidos antes de considerar la tarea como fallida."""
    message_id: str
    correlation_id: str
    schema_version: str = "1.0"
    timestamp: datetime
    source: str
    target_model: TargetModel
    retry_count: int = 0
    max_retries: int = 3
    payload: Dict[str, Any]