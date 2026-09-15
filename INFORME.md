## Decisiones de Diseño

- La conexión se crea en el `__init__`, porque los objetos se usan directamente
    para enviar, consumir o cerrar. Así, la cola o el exchange quedan disponibles
    desde el inicio. Si la declaración de la topología falla, se cierra la conexión
    ya abierta antes de propagar el error para no dejar recursos colgados.
- La lógica común de cola y exchange (conexión, consumo, detención y cierre) se
    concentra en una clase base `_MessageMiddlewareRabbitMQ`. Cada clase concreta
    hereda de esa base y de su interfaz, y solo aporta la declaración de su
    topología y desde qué cola consume, evitando código repetido.
- Al enviar por el exchange, el mensaje se publica en **todas las routing keys**
    configuradas, según la interfaz simplificada pedida para este trabajo. Al
    consumir, la cola se vincula a todas esas routing keys.
- `_create_message_handler` adapta la función que RabbitMQ llama con
    `(ch, method, properties, body)` para entregar a la interfaz solo el mensaje
    y las funciones `ack` y `nack`.
- Para cada consumidor del exchange se crea una cola anónima y exclusiva. Así,
    cada consumidor tiene su propia cola para recibir sus mensajes, sin tener que
    elegir ni administrar un nombre, y la cola se elimina al desconectarse.
- Se creó un wrapper `_rabbitmq_call` que ejecuta cualquier llamada a la librería
    de RabbitMQ y traduce sus excepciones a los errores definidos por el middleware,
    sin filtrar detalles de la librería hacia afuera: los de conexión se mapean a
    `MessageMiddlewareDisconnectedError` y el resto a `MessageMiddlewareMessageError`
    (o `MessageMiddlewareCloseError` al cerrar). Centralizar esta traducción evita
    repetir el mismo `try/except` en cada operación.
