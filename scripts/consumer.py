import os
from messaging.redis_client import RedisQueueClient
from schemas.result import ResultMessage

mq_client = RedisQueueClient()
RESULT_QUEUE = "cola_resultados"

def handle_result(result_dict):
    result = ResultMessage.model_validate(result_dict)
    print(f"Resultado recibido: {result.message_id} - Modelo: {result.model}")

    folder = os.getenv("RESULT_PATH", "./resources/results")
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, f"{result.message_id}.json"), "w") as f:
        f.write(result.model_dump_json())

if __name__ == "__main__":
    mq_client.consume(RESULT_QUEUE, handle_result)
