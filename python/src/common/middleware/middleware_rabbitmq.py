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


def _close_connection(connection, channel):
    """Cierra el canal y la conexión de RabbitMQ si siguen abiertos."""
    if channel.is_open:
        _rabbitmq_call(channel.close, error=MessageMiddlewareCloseError)
    if connection.is_open:
        _rabbitmq_call(connection.close, error=MessageMiddlewareCloseError)


def _rabbitmq_call(function, *args, error=MessageMiddlewareMessageError, **kwargs):
    """Ejecuta una llamada a RabbitMQ y traduce sus errores."""
    try:
        return function(*args, **kwargs)
    except AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError from e
    except AMQPError as e:
        raise error from e


def _create_message_handler(on_message_callback):
    """Adapta el callback de RabbitMQ al callback del middleware."""
    def handle_message(channel, method, _properties, body):
        ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
        on_message_callback(body, ack, nack)

    return handle_message


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        """Crea la conexión y declara la cola en RabbitMQ."""
        self.host = host
        self.queue_name = queue_name
        self.consumer_tag = None

        self._connection, self._channel = _rabbitmq_call(_create_channel, self.host)
        _rabbitmq_call(self._channel.queue_declare, queue=self.queue_name, durable=True)

    def send(self, message):
        """Envía un mensaje a la cola asociada a esta instancia."""
        _rabbitmq_call(
            self._channel.basic_publish,
            exchange="",
            routing_key=self.queue_name,
            body=message,
        )

    def start_consuming(self, on_message_callback):
        """Inicia el consumo de mensajes de la cola asociada a esta instancia.

        `on_message_callback` debe aceptar tres argumentos: el cuerpo del mensaje, una función
        para confirmar la recepción del mensaje (ack), y una función para rechazar el mensaje
        (nack). Esto permite al consumidor decidir si el mensaje fue procesado correctamente o no.
        """

        self.consumer_tag = _rabbitmq_call(
            self._channel.basic_consume,
            queue=self.queue_name,
            on_message_callback=_create_message_handler(on_message_callback),
            auto_ack=False,
        )
        _rabbitmq_call(self._channel.start_consuming)

    def stop_consuming(self):
        """Detiene el consumo de mensajes. Si no se está consumiendo, no hace nada."""
        if self._channel.is_open and self.consumer_tag:
            _rabbitmq_call(self._channel.basic_cancel, self.consumer_tag)
            self.consumer_tag = None
            _rabbitmq_call(self._channel.stop_consuming)

    def close(self):
        _close_connection(self._connection, self._channel)


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        """Crea la conexión y declara el exchange en RabbitMQ de tipo 'topic'."""
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consumer_tag = None

        self._connection, self._channel = _rabbitmq_call(_create_channel, self.host)
        _rabbitmq_call(
            self._channel.exchange_declare,
            exchange=self.exchange_name,
            exchange_type=EXCHANGE_TYPE,
            durable=True,
        )

    def send(self, message):
        """Envía un mensaje al exchange con la routing key configurada."""
        _rabbitmq_call(
            self._channel.basic_publish,
            exchange=self.exchange_name,
            routing_key=self.routing_keys[0],
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )

    def start_consuming(self, on_message_callback):
        """Inicia el consumo de mensajes del exchange. Se crea una cola anónima
        y exclusiva para este consumidor, y se vincula a las routing keys configuradas.
        """
        # Cola anónima y exclusiva para el suscriptor.
        result = _rabbitmq_call(self._channel.queue_declare, queue="", exclusive=True)
        queue_name = result.method.queue

        for routing_key in self.routing_keys:
            _rabbitmq_call(
                self._channel.queue_bind,
                exchange=self.exchange_name,
                queue=queue_name,
                routing_key=routing_key,
            )

        self.consumer_tag = _rabbitmq_call(
            self._channel.basic_consume,
            queue=queue_name,
            on_message_callback=_create_message_handler(on_message_callback),
            auto_ack=False,
        )
        _rabbitmq_call(self._channel.start_consuming)

    def stop_consuming(self):
        """Detiene el consumo de mensajes. Si no se está consumiendo, no hace nada."""
        if self._channel.is_open and self.consumer_tag:
            _rabbitmq_call(self._channel.basic_cancel, self.consumer_tag)
            self.consumer_tag = None
            _rabbitmq_call(self._channel.stop_consuming)

    def close(self):
        """Cierra la conexión y el canal de comunicación con RabbitMQ."""
        _close_connection(self._connection, self._channel)
