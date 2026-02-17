import os
import json
import redis
import logging
from typing import Callable
from datetime import datetime, date
from .base import BaseQueueClient

logging.basicConfig(level=logging.INFO)

class RedisQueueClient(BaseQueueClient):
    """
    Cliente Redis para colas.
    Usa listas (LPUSH / BRPOP) como cola.
    """
    
    def __init__(self, host=None, port=None, db=0):
        self.host = host or os.getenv("REDIS_HOST")
        self.port = port or int(os.getenv("REDIS_PORT", 6379))
        self.r = redis.Redis(host=self.host, port=self.port, db=db)


    def publish(self, queue_name: str, message: dict):
        try:
            self.r.lpush(queue_name, json.dumps(message,default=self._json_serializer))
            logging.info(f"Mensaje publicado en {queue_name}")
        except Exception:
            logging.exception(f"Error publicando mensaje en {queue_name}")

    def consume(self, queue_name: str, callback: Callable[[dict], None]):
        logging.info(f"Escuchando cola {queue_name}")
        while True:
            try:
                _, raw = self.r.brpop(queue_name)
                msg = json.loads(raw)
                callback(msg)
            except Exception:
                logging.exception("Error consumiendo mensaje")

    def ack(self, message_id: str):
        # Con Redis no es necesario si usamos idempotencia
        pass

    def send_to_dlq(self, dlq_name: str, message: dict):
        try:
            self.r.lpush(dlq_name, json.dumps(message,default=self._json_serializer))
            logging.warning(f"Mensaje enviado a DLQ {dlq_name}")
        except Exception:
            logging.exception(f"Error enviando mensaje a DLQ {dlq_name}")

    def _json_serializer(self, obj):
        
        """Convierte datetime/date a string ISO para evitar errores de serialización"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")