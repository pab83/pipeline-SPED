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
    Cliente de mensajería basado en Redis para la gestión de colas de la Pipeline.
    
    Implementa un patrón Productor-Consumidor utilizando listas de Redis con
    operaciones atómicas `LPUSH` para publicar y `BRPOP` para consumo bloqueante.
    """
    
    def __init__(self, host: str = None, port: int = None, db: int = 0):
        """
        Inicializa la conexión con el servidor Redis.
        
        Args:
            host: Dirección del servidor (por defecto toma REDIS_HOST del entorno).
            port: Puerto del servidor (por defecto toma REDIS_PORT o 6379).
            db: Índice de la base de datos Redis a utilizar.
        """
        self.host = host or os.getenv("REDIS_HOST")
        self.port = port or int(os.getenv("REDIS_PORT", 6379))
        self.r = redis.Redis(host=self.host, port=self.port, db=db)

    def publish(self, queue_name: str, message: dict):
        """
        Publica un mensaje serializado en una cola específica.
        
        Args:
            queue_name: Nombre de la lista/cola en Redis.
            message: Diccionario con los datos del mensaje (ej. TaskMessage).
        """
        try:
            self.r.lpush(queue_name, json.dumps(message, default=self._json_serializer))
            logging.info(f"Mensaje publicado en {queue_name}")
        except Exception:
            logging.exception(f"Error publicando mensaje en {queue_name}")

    def consume(self, queue_name: str, callback: Callable[[dict], None]):
        """
        Escucha una cola de forma bloqueante y ejecuta un callback por cada mensaje.
        
        Args:
            queue_name: Nombre de la cola a monitorizar.
            callback: Función que procesará el mensaje recibido (convertido a dict).
        """
        logging.info(f"Escuchando cola {queue_name}")
        while True:
            try:
                # BRPOP devuelve una tupla (lista, valor)
                _, raw = self.r.brpop(queue_name)
                msg = json.loads(raw)
                callback(msg)
            except Exception:
                logging.exception("Error consumiendo mensaje")

    def ack(self, message_id: str):
        """
        Confirma el procesamiento exitoso de un mensaje.
        
        Nota:
            En esta implementación basada en Listas simples, el ACK es implícito 
            al extraer el mensaje con BRPOP. Se mantiene por compatibilidad con la clase base.
        """
        pass

    def send_to_dlq(self, dlq_name: str, message: dict):
        """
        Envía mensajes fallidos a una Dead Letter Queue (DLQ) para su posterior análisis.
        
        Args:
            dlq_name: Nombre de la cola de errores.
            message: El mensaje original que causó el fallo.
        """
        try:
            self.r.lpush(dlq_name, json.dumps(message, default=self._json_serializer))
            logging.warning(f"Mensaje enviado a DLQ {dlq_name}")
        except Exception:
            logging.exception(f"Error enviando mensaje a DLQ {dlq_name}")

    def _json_serializer(self, obj):
        """
        Manejador interno para objetos no serializables por defecto en JSON.
        
        Convierte objetos `datetime` y `date` a formato ISO 8601 (string) para 
        permitir el transporte de marcas de tiempo en los TaskMessages.
        
        Raises:
            TypeError: Si el objeto no es una fecha o no es serializable.
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")