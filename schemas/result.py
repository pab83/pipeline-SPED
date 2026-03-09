from typing import Any, List, Optional
from enum import Enum

from pydantic import BaseModel


# ---------------------------
# ENUMS
# ---------------------------
class Status(str, Enum):
    """ Enum que define los posibles estados de una tarea o resultado dentro del pipeline. El estado SUCCESS indica que la tarea se completó correctamente, mientras que ERROR indica que hubo un problema durante la ejecución de la tarea. Este enum se utiliza en el ResultMessage para indicar claramente el resultado de la ejecución de una tarea, lo que facilita el manejo de resultados y errores dentro del sistema."""
    SUCCESS = "success"
    ERROR = "error"


# ---------------------------
# ERROR STRUCTURE
# ---------------------------
class ErrorInfo(BaseModel):
    """ Estructura que define la información de error en caso de que una tarea falle durante su ejecución. Incluye un campo type para categorizar el tipo de error, un mensaje descriptivo del error, y un campo retryable que indica si el error es transitorio y si la tarea puede ser reintentada automáticamente por el sistema. Esta estructura se utiliza en el ResultMessage para proporcionar detalles claros sobre cualquier error que ocurra durante la ejecución de una tarea, lo que ayuda en la depuración y manejo de errores dentro del pipeline."""
    type: str
    message: str
    retryable: bool = True


# ---------------------------
# RESULT MESSAGE
# ---------------------------
class ResultMessage(BaseModel):
    """ Estructura del mensaje que representa el resultado de la ejecución de una tarea dentro del pipeline. Este modelo define los campos necesarios para describir el resultado, incluyendo identificadores únicos, información de tiempo, el modelo que ejecutó la tarea, el estado del resultado (éxito o error), y campos opcionales para resultados exitosos como el resultado específico, embedding generado, dimensión del embedding, y hash del contenido. En caso de error, incluye un campo error con la información detallada del error utilizando la estructura ErrorInfo."""
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