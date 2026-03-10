# Esquema de Mensajería (Redis)

El pipeline utiliza **Redis** como broker de mensajes para la comunicación asíncrona entre el orquestador y los Workers de IA. El intercambio de datos se rige por esquemas estrictos definidos mediante **Pydantic**, garantizando la integridad de los datos en entornos distribuidos.



---

## 📥 Mensaje de Tarea (`TaskMessage`)

Este esquema define la estructura de las solicitudes enviadas a los Workers. Cada mensaje es una unidad de trabajo autónoma que contiene tanto los datos (`payload`) como los metadatos de control.

::: schemas.task
    options:
      show_root_toc_entry: false
      members:
        - message_id
        - correlation_id
        - target_model
        - retry_count
        - payload
      show_source: true

###  Modelos de Destino (`TargetModel`)
El campo `target_model` determina qué pool de Workers debe procesar la tarea:

* **`ocr`**: Extracción de texto mediante motores ópticos.
* **`moondream`**: Análisis visual y descripción de imágenes (VLM).
* **`embeddings`**: Vectorización de contenido para búsqueda semántica.

---

## 📤 Mensaje de Resultado (`ResultMessage`)

Una vez finalizado el procesamiento, el Worker emite un mensaje de resultado. Este esquema es polimórfico: puede contener los datos de éxito o los detalles técnicos de un fallo.

::: schemas.result
    options:
      show_root_toc_entry: false
      members:
        - status
        - result
        - embedding
        - error
        - processing_time_ms
      show_source: true

###  Gestión de Errores (`ErrorInfo`)
Si el `status` es `error`, el mensaje incluye un objeto `ErrorInfo` con la categoría técnica y la política de reintento:
* **`type`**: Tipo de excepción (ej. `FileNotFoundError`).
* **`retryable`**: Booleano que indica al orquestador si vale la pena re-encolar la tarea.



---

## 🔄 Flujo de Reconciliación (Correlation ID)

El sistema utiliza el `correlation_id` como clave de unión entre el mundo asíncrono (Redis) y el mundo relacional (PostgreSQL):

1. **Emisión**: El Productor genera un `TaskMessage`, guarda el `correlation_id` en la tabla de mapeo de la DB y publica en Redis.
2. **Procesamiento**: El Worker procesa la tarea y mantiene el mismo `correlation_id` en su `ResultMessage`.
3. **Consumo**: El Consumidor recibe el resultado, localiza el registro original en la DB usando el ID de correlación y persiste el `result` o el `error`.

---

## 📊 Versiones y Compatibilidad

* **`schema_version`**: Actualmente `1.0`. Este campo permite realizar actualizaciones en caliente del pipeline. Si un Worker detecta una versión de esquema superior a la que puede procesar, marcará la tarea como error de compatibilidad.
* **Serialización**: Los mensajes se serializan en formato **JSON UTF-8** para asegurar la interoperabilidad.