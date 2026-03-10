from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
from enum import Enum

# ---------------------------
# ENUMS
# ---------------------------
class TargetModel(str, Enum):
    """
    Define los modelos o fases de destino para una tarea en la pipeline.
    """
    OCR = "ocr"
    """Fase de reconocimiento óptico de caracteres."""
    
    MOONDREAM = "moondream"
    """Modelo de IA para descripción visual y análisis de imágenes."""
    
    EMBEDDINGS = "embeddings"
    """Generación de vectores numéricos para búsqueda semántica."""

# ---------------------------
# TASK MESSAGE
# ---------------------------
class TaskMessage(BaseModel):
    """
    Estructura del mensaje que define una tarea a ejecutar por los workers.
    """
    message_id: str
    """Identificador único universal (UUID) de este mensaje específico."""
    
    correlation_id: str
    """ID de seguimiento que vincula esta tarea con un proceso de auditoría global."""
    
    schema_version: str = "1.0"
    """Versión del esquema para asegurar compatibilidad entre productor y consumidor."""
    
    timestamp: datetime
    """Fecha y hora exacta en la que se generó la tarea."""
    
    source: str
    """Módulo o fase de origen que emitió la tarea (ej. 'phase_2')."""
    
    target_model: TargetModel
    """El worker o modelo específico encargado de procesar esta tarea."""
    
    retry_count: int = 0
    """Contador actual de reintentos realizados tras fallos transitorios."""
    
    max_retries: int = 3
    """Límite máximo de intentos permitidos antes de descartar la tarea."""
    
    payload: Dict[str, Any]
    """Diccionario con los datos específicos necesarios (ej. path del archivo, metadatos)."""