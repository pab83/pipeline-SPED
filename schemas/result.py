from typing import Optional, List, Any
from enum import Enum
from pydantic import BaseModel

# ---------------------------
# ENUMS
# ---------------------------
class Status(str, Enum):
    """
    Define los posibles estados de una tarea o resultado dentro del pipeline.
    """
    SUCCESS = "success"
    """La tarea se completó correctamente sin interrupciones."""
    
    ERROR = "error"
    """Se produjo un fallo crítico durante la ejecución de la tarea."""


# ---------------------------
# ERROR STRUCTURE
# ---------------------------
class ErrorInfo(BaseModel):
    """
    Estructura detallada de error en caso de fallo en la ejecución.
    """
    type: str
    """Categoría técnica del error (ej. 'ConnectionError', 'Timeout')."""
    
    message: str
    """Descripción legible para humanos sobre lo que salió mal."""
    
    retryable: bool = True
    """Indica si el sistema debe intentar re-encolar la tarea automáticamente."""


# ---------------------------
# RESULT MESSAGE
# ---------------------------
class ResultMessage(BaseModel):
    """
    Estructura del mensaje que representa el resultado final de una tarea.
    """
    message_id: str
    """Identificador único universal (UUID) del mensaje de resultado."""
    
    correlation_id: str
    """ID de seguimiento para vincular el resultado con la tarea original."""
    
    schema_version: str = "1.0"
    """Versión del esquema de datos para asegurar compatibilidad entre workers."""
    
    model: str
    """Nombre o versión del modelo/proceso que generó este resultado."""
    
    status: Status
    """Estado final del procesamiento (success o error)."""
    
    processing_time_ms: Optional[int] = None
    """Tiempo total invertido en el procesamiento medido en milisegundos."""

    # Success fields
    result: Optional[Any] = None
    """Datos de salida generados (texto extraído, metadatos, etc.)."""
    
    embedding: Optional[List[float]] = None
    """Representación vectorial numérica del contenido si aplica."""
    
    embedding_dim: Optional[int] = None
    """Dimensión del vector de embedding generado."""
    
    content_hash: Optional[str] = None
    """Firma digital del contenido procesado para verificar integridad."""

    # Error field
    error: Optional[ErrorInfo] = None
    """Objeto con detalles técnicos en caso de que el status sea 'error'."""