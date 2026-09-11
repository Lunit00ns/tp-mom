import pika
from pika.exceptions import AMQPConnectionError, AMQPError

from .middleware import (
    MessageMiddlewareCloseError,
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
        """Crea la conexión y declara la cola en RabbitMQ."""
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

    def send(self, message):
        """Envía un mensaje a la cola asociada a esta instancia."""
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(
                "Se perdió la conexión con RabbitMQ al intentar enviar un mensaje."
            )
        except AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Ocurrió un error al enviar el mensaje a RabbitMQ: {e!s}"
            )

    def start_consuming(self, on_message_callback):
        """Inicia el consumo de mensajes de la cola asociada a esta instancia.

        `on_message_callback` debe aceptar tres argumentos: el cuerpo del mensaje, una función
        para confirmar la recepción del mensaje (ack), y una función para rechazar el mensaje
        (nack). Esto permite al consumidor decidir si el mensaje fue procesado correctamente o no.
        """

        def _wrapper(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)

        try:
            self.consumer_tag = self._channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=_wrapper,
                auto_ack=False,
            )
            self._channel.start_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(
                "Se perdió la conexión con RabbitMQ al intentar consumir mensajes."
            )
        except AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Ocurrió un error al consumir mensajes de: {e!s}"
            )

    def stop_consuming(self):
        """Detiene el consumo de mensajes. Si no se está consumiendo, no hace nada."""
        try:
            if self._channel and self._channel.is_open:
                if self.consumer_tag:
                    self._channel.basic_cancel(self.consumer_tag)
                    self.consumer_tag = None
                if self._channel.is_consuming:
                    self._channel.stop_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(
                "Se perdió la conexión con RabbitMQ al intentar detener el consumo de mensajes."
            )
        except AMQPError:
            pass

    def close(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Se perdió la conexión con RabbitMQ al intentar cerrar la conexión: {e!s}"
            )
        except AMQPError as e:
            raise MessageMiddlewareCloseError(
                f"Ocurrió un error al cerrar la conexión con RabbitMQ: {e!s}"
            )


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        """Crea la conexión y declara el exchange en RabbitMQ de tipo 'topic'."""
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
