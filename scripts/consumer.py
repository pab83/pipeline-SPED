import os
from messaging.redis_client import RedisQueueClient
from schemas.result import ResultMessage

mq_client = RedisQueueClient()
"""Instancia del cliente Redis utilizada para escuchar y procesar los resultados de los workers."""

RESULT_QUEUE = "cola_resultados"
"""Nombre de la cola de Redis donde los workers publican los resultados (`ResultMessage`)."""

def handle_result(result_dict: dict):
    """
    Procesa un resultado individual recibido desde la cola de mensajería.

    Esta función realiza la validación del esquema mediante Pydantic, registra la recepción
    del mensaje y persiste el resultado en el sistema de archivos local para su posterior
    auditoría o integración con la base de datos.

    Args:
        result_dict: Diccionario crudo recibido de Redis que representa un `ResultMessage`.

    Raises:
        ValidationError: Si el contenido del mensaje no cumple con el esquema `ResultMessage`.
        OSError: Si hay problemas al crear el directorio o escribir el archivo JSON.
        
    Note:
        Los archivos se guardan por defecto en `./resources/results` a menos que se
        especifique lo contrario mediante la variable de entorno `RESULT_PATH`.
    """
    result = ResultMessage.model_validate(result_dict)
    print(f"Resultado recibido: {result.message_id} - Modelo: {result.model}")

    folder = os.getenv("RESULT_PATH", "./resources/results")
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, f"{result.message_id}.json"), "w") as f:
        f.write(result.model_dump_json())

if __name__ == "__main__":
    mq_client.consume(RESULT_QUEUE, handle_result)
