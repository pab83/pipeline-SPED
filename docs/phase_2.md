# ⚙️ Fase 2: Procesamiento y Extracción

La **Fase 2** constituye el núcleo de inteligencia del pipeline. En esta etapa, los metadatos crudos recolectados en la Fase 1 se transforman en información accionable mediante la limpieza de duplicados, la clasificación visual de archivos y la extracción masiva de contenido textual y vectorial.

---

## 🚀 Orquestador de Fase
El script `run_phase_2.py` coordina la ejecución secuencial de todas las tareas de procesamiento, asegurando que las dependencias entre scripts (como tener la base de datos migrada antes de extraer texto) se respeten.

::: scripts.phase_2.run_phase_2
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - main
      show_signature_annotations: true

---

## 🗄️ Preparación de Datos (Migrations)
Antes del procesamiento, se actualiza el esquema de la base de datos para soportar el almacenamiento de texto extraído y la integración de **pgvector** para búsquedas semánticas.

::: scripts.phase_2.migrate_phase_2
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - run_migrations
      docstring_section_style: table



---

## 🧹 Deduplicación y Canonización
Este módulo identifica archivos redundantes utilizando dos metodologías complementarias:

1. **Hash-Level**: Deduplicación exacta mediante hash.
2. **Semantic-Level**: Agrupación por similitud de contenido (Embeddings).

::: scripts.phase_2.dedup
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - choose_canonical
        - hash_level_canonicalization
        - semantic_canonicalization
      docstring_section_style: table

> **Nota:** La función `choose_canonical` utiliza una lógica de puntuación para decidir qué archivo conservar, priorizando PDFs digitales y archivos con fechas de modificación más recientes.

---

## 📝 Extracción de Texto
Motor de lectura multiformato que procesa documentos PDF, Word y texto plano. Gestiona automáticamente las codificaciones de caracteres y limita la carga para optimizar el rendimiento.

::: scripts.phase_2.extract_text
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - main
        - extract_text_from_pdf
        - extract_text_from_docx
        - safe_read_text_file
      show_signature_annotations: true



---

## 👁️ Clasificación Visual (Computer Vision)
Para optimizar el uso de motores OCR, este script utiliza **OpenCV** para determinar si una imagen (JPG, PNG, etc.) tiene la estructura de un documento escaneado.

::: scripts.phase_2.img_looks_like_document
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      show_source: true
      members:
        - looks_like_document
        - process_image
      docstring_section_style: table

### Criterios de Clasificación
* **Detección de Bordes**: Uso de Canny para encontrar estructuras rectangulares.
* **Análisis de Textura**: Filtro de varianza para descartar fotografías naturales y conservar escaneos uniformes.
* **Procesamiento Paralelo**: Implementado mediante `multiprocessing` para gestionar grandes volúmenes de imágenes.