import os
from uuid import uuid4
from datetime import datetime
from typing import Union, Tuple

from messaging.redis_client import RedisQueueClient
from schemas.task import TaskMessage, TargetModel


mq_client = RedisQueueClient()

QUEUE_NAME_OCR = "cola_modelo_ocr"
QUEUE_NAME_MOONDREAM = "cola_modelo_moondream"
QUEUE_NAME_EMBEDDINGS = "cola_modelo_embeddings"

QUEUE_MAP = {
    TargetModel.OCR: QUEUE_NAME_OCR,
    TargetModel.MOONDREAM: QUEUE_NAME_MOONDREAM,
    TargetModel.EMBEDDINGS: QUEUE_NAME_EMBEDDINGS,
}


def _normalize_target_model(target_model: Union[str, TargetModel]) -> TargetModel:
    """
    Acepta tanto strings ('ocr', 'moondream', 'embeddings') como TargetModel.
    Lanza ValueError si el modelo no es válido.
    """
    if isinstance(target_model, TargetModel):
        return target_model

    try:
        return TargetModel(target_model)
    except ValueError as exc:
        raise ValueError(
            f"target_model inválido: {target_model!r}. "
            f"Valores válidos: {[m.value for m in TargetModel]}"
        ) from exc


def send_task(
    file_path: str,
    target_model: Union[str, TargetModel],
    prompt: str = "",
    source: str = "pipeline_v0",
) -> Tuple[str, str]:
    """
    Envía una tarea al modelo indicado.

    Devuelve una tupla (message_id, correlation_id) para poder rastrear la tarea.
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
    print(f"Tarea enviada {task.message_id} a {model_enum.value} ({queue_name})")

    return (task.message_id, task.correlation_id)


if __name__ == "__main__":
    folder = os.getenv("BASE_PATH", "./resources/documentos")
    for f in os.listdir(folder):
        file_path = os.path.join(folder, f)
        send_task(file_path, target_model=TargetModel.OCR)
