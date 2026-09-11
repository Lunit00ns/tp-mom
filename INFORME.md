# Decisiones de Diseño

* Se elige crear la conexción dentro del `__init__` ya que los objetos (en este caso) se usan inmediatamente después para enviar, consumir o cerrar: la cola o exchange queda declarada desde el inicio

    > La alternativa sería una conexión explícita, que exigiría manejar estados "objeto creado pero no conectado", y para el alcance de este trabajo no lo veo necesario.