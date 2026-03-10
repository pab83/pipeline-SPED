# 📂 Fase 0: Escaneo y Preparación de Datos

La **Fase 0** es el cimiento de la pipeline. Su objetivo es identificar nuevos archivos en el sistema de archivos, registrarlos en la base de datos y preparar el estado inicial para que las siguientes fases puedan procesarlos.

---

## 🚀 Orquestador de Fase (Run)
Gestiona la secuencia de ejecución y el logging de la Fase 0.

::: scripts.phase_0.run_phase_0
    options:
      heading_level: 3
      show_root_heading: true
      show_root_toc_entry: false
      show_source: true
      members:
        - main
        - RUN_ID
        - PHASE_NUMBER
        - SCRIPTS
      group_by_category: true
      show_category_heading: true

---

## 🔍 Identificación de Archivos (Scan)
Este módulo se encarga de la lectura masiva del sistema de archivos y la extracción de metadatos.

::: scripts.phase_0.scan_files
    options:
      heading_level: 3
      members:
        - audit
        - process_file
        - generate_files
        - normalize_path
        - list_top_directories
        - BASE_SEP_COUNT
      show_root_heading: true
      show_root_toc_entry: false
      show_source: true
      group_by_category: true
      show_category_heading: true

---

## 🗄️ Persistencia (Database)
Encargado de crear las entradas correspondientes en PostgreSQL.

::: scripts.phase_0.create_db
    options:
      show_root_heading: true
      heading_level: 3
      show_source: true
      show_root_toc_entry: false
      # Solo mostramos la función principal
      members:
        - create_db
      # Esto asegura que el botón de source aparezca arriba
      show_root_full_path: true


---

### 📊 Estructura de la Tabla `files`
La base de datos inicial se compone de las siguientes columnas:

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `id` | SERIAL | Clave primaria autoincremental (Única). |
| `full_path` | TEXT | Ruta absoluta (Única). |
| `file_name` | TEXT | Nombre del archivo con extensión. |
| `size_bytes` | BIGINT | Tamaño en disco. |
| `is_pdf` | BOOLEAN | Flag para identificar documentos PDF. |
| `ocr_needed` | BOOLEAN | Determina si debe enviarse a la fase de OCR. |
| `xxhash64` / `sha256` | TEXT | Hashes para control de duplicados. |
| `first_seen` | TIMESTAMP | Fecha del primer registro. |

---


## 🛠️  Marcado de OCR
Identifica qué documentos PDF requieren procesamiento por modelos de visión (OCR) y cuáles ya poseen capas de texto digital.

::: scripts.phase_0.mark_pdf_ocr
    options:
      show_root_heading: true
      heading_level: 3
      show_source: true
      show_root_toc_entry: false
      members:
        - main
        - pdf_needs_ocr
        - BATCH_SIZE
        - MAX_WORKERS
      group_by_category: true
      show_category_heading: true

---

## 📊 Reporte de Auditoría
Generación de estadísticas y resúmenes ejecutivos del escaneo.

::: scripts.phase_0.generate_phase_0_report
    options:
      show_root_heading: true
      heading_level: 3
      show_source: true
      show_root_toc_entry: false
      members:
        - generate_report
        - REPORT_FILE
      docstring_section_style: table
      show_signature_annotations: true

### 📑 Archivos Generados
La Fase 0 produce los siguientes artefactos de control y auditoría:

| Archivo | Formato | Descripción |
| :--- | :--- | :--- |
| `audit_summary.csv` | CSV | Reporte detallado con métricas globales, distribución por tipos y archivos grandes. |
| `file_audit.csv` | CSV | Listado crudo resultante del escaneo de directorios. (Opcional, por defecto no se crea)  |
| `run_X_phase_0.log` | LOG | Trazabilidad de la ejecución, errores de lectura de archivos y estado de BD. |

---
