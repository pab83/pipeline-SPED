import os
from uuid import uuid4
from datetime import datetime
from typing import Union, Tuple

from messaging.redis_client import RedisQueueClient
from schemas.task import TaskMessage, TargetModel

mq_client = RedisQueueClient()
"""Instancia del cliente de Redis utilizada para la comunicación con las colas."""

QUEUE_NAME_OCR = "cola_modelo_ocr"
"""Nombre de la cola de Redis destinada a las tareas de reconocimiento de texto (OCR)."""

QUEUE_NAME_MOONDREAM = "cola_modelo_moondream"
"""Nombre de la cola de Redis para el modelo Moondream (análisis visual)."""

QUEUE_NAME_EMBEDDINGS = "cola_modelo_embeddings"
"""Nombre de la cola de Redis para la generación de vectores (Embeddings)."""

QUEUE_MAP = {
    TargetModel.OCR: QUEUE_NAME_OCR,
    TargetModel.MOONDREAM: QUEUE_NAME_MOONDREAM,
    TargetModel.EMBEDDINGS: QUEUE_NAME_EMBEDDINGS,
}
"""Diccionario de mapeo que vincula cada tipo de `TargetModel` con su cola correspondiente."""


def _normalize_target_model(target_model: Union[str, TargetModel]) -> TargetModel:
    """
    Normaliza el modelo de destino para asegurar compatibilidad.

    Acepta tanto strings ('ocr', 'moondream', 'embeddings') como instancias de TargetModel.
    
    Args:
        target_model: El modelo o string a normalizar.

    Returns:
        La instancia correspondiente de TargetModel.

    Raises:
        ValueError: Si el modelo proporcionado no está soportado por la pipeline.
    """


def send_task(
    file_path: str,
    target_model: Union[str, TargetModel],
    prompt: str = "",
    source: str = "pipeline_v0",
) -> Tuple[str, str]:
    """
    Empaqueta y envía una tarea de procesamiento a la cola de Redis correspondiente.

    Esta función genera identificadores únicos para el mensaje y la correlación,
    normaliza el modelo de destino y publica el `TaskMessage` serializado.

    Args:
        file_path: Ruta absoluta o relativa al archivo que debe ser procesado.
        target_model: Modelo de destino (puedes pasar el Enum o el string: 'ocr', 'moondream', 'embeddings').
        prompt: Instrucción adicional opcional para modelos que aceptan lenguaje natural.
        source: Identificador del componente que origina la petición (por defecto 'pipeline_v0').

    Returns:
        Una tupla `(message_id, correlation_id)`. El `correlation_id` es vital para 
        rastrear la tarea a través de los logs y la base de datos.

    Example:
        ```python
        message_id, correlation_id = send_task("/path/to/img.jpg", "ocr")
        ```
    """
    model_enum = _normalize_target_model(target_model)

    task = TaskMessage(
        message_id=str(uuid4()),
        correlation_id=str(uuid4()),
        timestamp=datetime.utcnow(),
        source=source,
        target_model=model_enum,
        payload={"file_path": file_path, "prompt": prompt},
    )

    queue_name = QUEUE_MAP[model_enum]
    mq_client.publish(queue_name, task.model_dump())
    
    # Nota: Es recomendable usar logging.info en lugar de print para producción
    print(f"Tarea enviada {task.message_id} a {model_enum.value} ({queue_name})")

    return (task.message_id, task.correlation_id)


if __name__ == "__main__":
    folder = os.getenv("BASE_PATH")
    for f in os.listdir(folder):
        file_path = os.path.join(folder, f)
        send_task(file_path, target_model=TargetModel.OCR)
