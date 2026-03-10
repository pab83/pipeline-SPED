# 🧠 Fase 3: Inferencia de Modelos e Inteligencia Artificial

La **Fase 3** representa la capa de enriquecimiento semántico del pipeline. En esta etapa, el sistema deja de tratar los archivos como simples bytes para entender su contenido y contexto mediante el uso de modelos de lenguaje (LLM), visión (VLM) y reconocimiento de caracteres (OCR).

Esta fase no es lineal, utiliza una arquitectura una arquitectura híbrida:

1.  **Asíncrona (Redis)**: Para tareas pesadas de extracción técnica (OCR y VLM).
2.  **Síncrona (API REST)**: Para la toma de decisiones y clasificación semántica (LLM).



---

## 🚀 Orquestador de Fase (Run)
El script `run_phase_3.py` coordina el flujo de los agentes de producción y consumo. A diferencia de fases anteriores, este orquestador puede mantenerse en ejecución para procesar flujos continuos de datos.

::: scripts.phase_3.run_phase_3
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - main
      show_signature_annotations: true

---

## 👁️ Descripción Visual (VLM Pipeline)
Este módulo implementa un pipeline continuo para la generación de descripciones visuales mediante el modelo **Moondream**.

::: scripts.phase_3.describe_img
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - main
        - send_moondream_batch
        - process_moondream_results
        - count_pending_images
      docstring_section_style: table
      show_signature_annotations: true

> **Importante:** Las descripciones generadas aquí son fundamentales para que el LLM posterior pueda clasificar archivos que no contienen texto extraíble (ej. fotos de una obra o de un recibo borroso).

### 🔄 Auto-Batching y Flujo Continuo
A diferencia de otros scripts, el pipeline de Moondream está diseñado para la eficiencia en grandes volúmenes:

1.  **Arranque**: Envía un primer bloque (`BATCH_SIZE`) de tareas a Redis.
2.  **Consumo Reactivo**: Escucha los resultados de los Workers.
3.  **Reposición Automática**: Cada vez que se completa el procesamiento de un lote, el script detecta automáticamente si hay más imágenes pendientes y lanza el siguiente lote sin intervención manual.
4.  **Reconciliación**: Utiliza la tabla `moondream_task_map` para asegurar que cada descripción visual se asigne correctamente al `file_id` original mediante el `correlation_id`.



---

## 📨 Gestión de Tareas OCR (Producer/Consumer)
Este módulo es el responsable de la comunicación con los Workers de OCR. Implementa un patrón de **solicitud-respuesta asíncrona** utilizando una tabla de mapeo para la reconciliación de datos.

::: scripts.phase_3.process_ocr_tasks
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - send_ocr_tasks
        - process_ocr_results
        - extract_text_from_result
      docstring_section_style: table
      show_signature_annotations: true



### 🛰️ Mecanismo de Correlación
Para asegurar la integridad de los datos entre Redis y PostgreSQL:

1. **Producer**: Genera un `correlation_id` único, envía la tarea a Redis y registra el vínculo en la tabla `ocr_task_map`.
2. **Consumer**: Escucha la cola de resultados, recupera el `file_id` usando el ID de correlación y actualiza el registro final con el texto extraído.

---

## 🏷️ Clasificación Semántica (LLM)
A diferencia de los procesos anteriores, la clasificación se realiza mediante llamadas directas al motor de inferencia. Es el "cerebro" final que consolida la información técnica en etiquetas de negocio.

::: scripts.phase_3.process_files
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - procesar_archivos
        - clasificar_documento
        - sanitize_text
      docstring_section_style: table
      show_signature_annotations: true



### 🤖 Lógica de Inferencia Enriquecida
El modelo de lenguaje realiza una **fusión de datos** multimodal:

* **Metadatos**: Analiza el `full_path` para detectar códigos de proyecto (ej. `CSBORA`).
* **Contexto Textual**: Procesa el `text_excerpt` (OCR) para identificar el tipo de documento.
* **Contexto Visual**: Incorpora la `descripcion_imagen` generada por el VLM para entender archivos sin texto.

### 📥 Protocolo de Comunicación REST
La integración con el LLM (ej. Qwen 2.5) sigue un flujo síncrono:

1.  **Prompt Estructurado**: Exige una respuesta exclusivamente en formato **JSON**.
2.  **Sanitización**: Limpia caracteres no imprimibles y trunca el contenido para ajustarse a la ventana de contexto.
3.  **Control de Reintentos**: Ante fallos HTTP 400 o timeouts, implementa una política de hasta 3 reintentos.



---

## 📊 Resumen de Arquitectura de Colas
| Cola | Target Model | Protocolo | Tipo de Tarea |
| :--- | :--- | :--- | :--- |
| `cola_tareas_OCR` | OCR | Asíncrono (Redis) | Extracción de texto. |
| `cola_resultados_moondream` | VLM | Asíncrono (Redis) | Descripción visual de imágenes. |
| `LLM_URL` (Directo) | LLM | Síncrono (REST) | Clasificación y categorización. |