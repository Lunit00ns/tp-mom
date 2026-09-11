## Decisiones de Diseño

- La conexión se crea en el `__init__`, porque los objetos se usan directamente
    para enviar, consumir o cerrar. Así, la cola o el exchange quedan disponibles
    desde el inicio.
- Se separó la creación del canal, el cierre de recursos y el manejo de errores
    en funciones auxiliares para no repetir la misma lógica.
- Al consumir del exchange, la cola se vincula a todas las routing keys
    configuradas. Al publicar, se usa solo la primera porque cada routing key
    representa una ruta distinta; publicar el mismo mensaje en todas lo duplicaría
    innecesariamente.
- `_create_message_handler` adapta la función que RabbitMQ llama con
    `(ch, method, properties, body)` para entregar a la interfaz solo el mensaje
    y las funciones `ack` y `nack`.
- Para cada consumidor del exchange se crea una cola anónima y exclusiva. Así,
    cada consumidor tiene su propia cola para recibir sus mensajes, sin tener que
    elegir ni administrar un nombre, y la cola se elimina al desconectarse.
- Las conexiones y canales se cierran al terminar, y los errores de RabbitMQ
    se convierten en los errores definidos por el middleware.