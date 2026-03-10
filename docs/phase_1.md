# 🛡️ Fase 1: Hashing e Integridad

Esta fase se encarga de la generación de huellas digitales únicas para cada archivo detectado en la Fase 0. El objetivo es garantizar la integridad de los datos y permitir la detección precisa de duplicados mediante algoritmos de hashing de alto rendimiento.

---

## 🚀 Orquestador de Fase (Run)
Gestiona la ejecución secuencial de los procesos de cálculo de firmas y generación de informes.

::: scripts.phase_1.run_phase_1
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - main
        - RUN_ID
        - PHASE_NUMBER
        - SCRIPTS
      show_category_heading: false
      show_signature_annotations: true

---

## 🔑 Cálculo de Hashes
Implementación de alto rendimiento para la generación de firmas digitales `xxhash64`. Utiliza una arquitectura concurrente (**Multi-process + Multi-thread**) para maximizar el uso de CPU y minimizar los tiempos de espera de lectura en disco.



::: scripts.phase_1.hash_files
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - main
        - compute_xxhash64
        - db_reader
        - db_writer
        - MAX_WORKERS
        - THREADS_PER_WORKER
      docstring_section_style: table
      show_signature_annotations: true

---

## 🖼️ Clasificación de Imágenes
Analiza visualmente los archivos de imagen mediante OpenCV para determinar si contienen texto o estructura documental que requiera procesamiento OCR posterior.



::: scripts.phase_1.mark_img_ocr
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - main
        - process_image
        - IMAGE_EXTENSIONS
        - MAX_IMAGE_SIZE
      docstring_section_style: table
      show_signature_annotations: true

---

## 📊 Reporte de Integridad y Duplicados
Analiza las firmas digitales almacenadas en la base de datos para identificar colisiones de contenido exacto. Genera informes técnicos y ejecutivos sobre la salud y redundancia del repositorio.



::: scripts.phase_1.generate_phase_1_report
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - generate_reports
      docstring_section_style: table
      show_signature_annotations: true

### 📑 Archivos Generados
| Archivo | Formato | Descripción |
| :--- | :--- | :--- |
| `duplicates.csv` | CSV | Listado técnico de hashes con todas sus rutas asociadas. |
| `phase_1_summary.csv` | CSV | Resumen ejecutivo con estadísticas de duplicados y grupos mayores. |
| `run_X_phase_1.log` | LOG | Trazabilidad completa de errores y tiempos de ejecución. |

---