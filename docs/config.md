# Configuración del Sistema

Esta sección describe las constantes, variables de entorno y parámetros técnicos que rigen el comportamiento del pipeline en todas sus etapas de ejecución.

## Configuración General
Define las variables globales del proyecto, incluyendo las rutas base del sistema de archivos, credenciales de acceso a bases de datos y la configuración del broker de mensajería (Redis).

::: scripts.config.general
    options:
      show_root_toc_entry: false
      show_root_heading: false
      show_source: true

---

## Parámetros por Fase de Procesamiento

La configuración está segmentada por fases para permitir un ajuste fino de los umbrales de procesamiento, extensiones de archivo permitidas y lógica específica de cada etapa.

### Fase 0: Discovery
Parámetros relacionados con el escaneo inicial del sistema de archivos y la profundidad del rastreo.
::: scripts.config.phase_0

### Fase 1: Integrity
Configuración de los algoritmos de hashing y validación de integridad de los archivos ingestados.
::: scripts.config.phase_1

### Fase 2: Refinement
Configuración de los motores de deduplicación y criterios de limpieza de metadatos.
::: scripts.config.phase_2

### Fase 3: Enrichment
Configuración crítica de los modelos de Inteligencia Artificial (LLM/VLM) y los motores de OCR, incluyendo URLs de endpoints y parámetros de inferencia.
::: scripts.config.phase_3

---


## Resumen de Variables Críticas

| Variable | Descripción | Ubicación |
| :--- | :--- | :--- |
| `MAX_RETRIES` | Número máximo de reintentos para scripts fallidos. | `config.general` |
| `BATCH_SIZE` | Tamaño de lote para envíos a colas de Redis. | `config.phase_3` |
| `LOG_FILE` | Ruta absoluta al archivo de registro de la ejecución. | `config.general` |
| `RETRY_DELAY` | Tiempo de espera entre reintentos automáticos. | `config.general` |