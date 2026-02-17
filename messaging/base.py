from abc import ABC, abstractmethod
from typing import Any, Callable

class BaseQueueClient(ABC):
    """
    Interface abstracta para clientes de messaging.
    Permite consumir y producir mensajes de manera uniforme.
    """

    @abstractmethod
    def publish(self, queue_name: str, message: dict):
        """
        Publica un mensaje en la cola.
        """
        pass

    @abstractmethod
    def consume(self, queue_name: str, callback: Callable[[dict], None]):
        """
        Consume mensajes de la cola. Llama a callback por cada mensaje.
        """
        pass

    @abstractmethod
    def ack(self, message_id: str):
        """
        Marca un mensaje como procesado (opcional según implementación).
        """
        pass

    @abstractmethod
    def send_to_dlq(self, dlq_name: str, message: dict):
        """
        Envía mensaje a la Dead Letter Queue.
        """
        pass