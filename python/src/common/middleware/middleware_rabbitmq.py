import pika
from pika.exceptions import AMQPConnectionError, AMQPError

from .middleware import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

EXCHANGE_TYPE = "topic"  # Tipo de exchange que se utilizará para la comunicación


def _create_channel(host):
    """Crea una conexión y un canal de comunicación con RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    return connection, connection.channel()


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.consumer_tag = None

        try:
            self._connection, self._channel = _create_channel(self.host)
            self._channel.queue_declare(queue=self.queue_name, durable=True)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consumer_tag = None

        try:
            self._connection, self._channel = _create_channel(self.host)
            self._channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=EXCHANGE_TYPE,
                durable=True,
            )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError from e
