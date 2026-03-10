# 📨 Sistema de Mensajería

Este módulo constituye la columna vertebral de la comunicación asíncrona entre el orquestador y los Workers de procesamiento pesado (OCR, VLM, Embeddings). Utiliza **Redis** como broker para gestionar colas de trabajo distribuidas, permitiendo que el pipeline escale horizontalmente.



---

## 🏛️ Arquitectura Base

El sistema se basa en una interfaz abstracta que define el contrato mínimo para cualquier cliente de mensajería. Esto permite cambiar el motor de colas (ej. de Redis a RabbitMQ) sin alterar la lógica de los Workers.

::: messaging.base.BaseQueueClient
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true

---

## 🏎️ Implementación: Redis Queue Client

`RedisQueueClient` es la implementación oficial. Utiliza el tipo de dato **List** de Redis para simular una cola FIFO (First-In, First-Out) con operaciones atómicas.

::: messaging.redis_client.RedisQueueClient
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      group_by_category: true

### 🛠️ Detalles Técnicos de Operación

* **Publicación Atómica (`LPUSH`)**: Los mensajes se insertan en la cabeza de la lista. El cliente incluye un serializador personalizado para transformar objetos `datetime` de Python a formato **ISO 8601** automáticamente.
* **Consumo Bloqueante (`BRPOP`)**: El método `consume` utiliza una espera bloqueante. El Worker no consume ciclos de CPU ni ancho de banda realizando *polling*; simplemente "despierta" cuando Redis le entrega un nuevo mensaje.
* **Confirmación (ACK)**: En esta arquitectura de listas, el mensaje se extrae de la cola en el momento de la entrega. El éxito o fallo se gestiona mediante la publicación de un nuevo `ResultMessage` o el envío a la **DLQ**.

---

## 🛠️ Ejemplo de Flujo: Ciclo de Vida de una Tarea

Para entender cómo interactúan los componentes en un escenario real (ej. análisis visual con **Moondream**), el proceso sigue estos pasos:

### 1. Despacho (Productor)
El orquestador genera un `TaskMessage` con un `correlation_id` único. Este ID es la "llave" que permitirá al sistema volver a unir el resultado de la IA con el archivo correcto en la base de datos SQL. El mensaje se publica mediante `publish()` en la cola correspondiente.



### 2. Procesamiento (Worker)
El Worker, que se encuentra a la escucha mediante `consume()`, recibe el JSON. El cliente lo convierte en un diccionario y lo pasa al *callback* de procesamiento. Mientras el Worker trabaja, la conexión con Redis permanece abierta pero inactiva, optimizando recursos.

### 3. Finalización y Gestión de Errores
Dependiendo del resultado de la inferencia:

* **Éxito**: El Worker construye un `ResultMessage` de éxito y lo publica en la cola de resultados globales.
* **Fallo**: Si el archivo está corrupto o el modelo lanza una excepción, el Worker invoca `send_to_dlq()`. El mensaje original se mueve a la **Dead Letter Queue** (`dlq_vlm_tasks`) para auditoría manual sin bloquear al resto de la fila.



### 📊 Tabla de Acciones de Mensajería

| Acción | Método del Cliente | Objeto de Datos | Propósito |
| :--- | :--- | :--- | :--- |
| **Enviar Tarea** | `publish()` | `TaskMessage` | Asignar trabajo a un Worker. |
| **Escuchar Cola** | `consume()` | `dict` (JSON) | Recibir tareas de forma bloqueante. |
| **Reportar Éxito** | `publish()` | `ResultMessage` | Devolver el texto/descripción extraída. |
| **Reportar Fallo** | `send_to_dlq()` | `TaskMessage` | Aislar tareas fallidas para revisión. |

---

## ⚙️ Configuración del Entorno

El cliente de Redis se configura dinámicamente mediante variables de entorno, lo que facilita su despliegue en infraestructuras con **Docker Compose** o **Kubernetes**.

| Variable | Descripción | Valor por Defecto |
| :--- | :--- | :--- |
| `REDIS_HOST` | Dirección del servidor Redis. | `localhost` |
| `REDIS_PORT` | Puerto de comunicación. | `6379` |
| `REDIS_DB` | Índice de la base de datos Redis. | `0` |