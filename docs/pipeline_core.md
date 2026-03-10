# ⚙️ Core de la Pipeline

Esta sección documenta los componentes centrales que gestionan el flujo de datos.

---

## 🏃 Orquestador Principal (Run)
El punto de entrada que coordina la ejecución secuencial de las fases.

::: scripts.run_pipeline
    options:
      show_root_heading: false
      show_source: true
      show_if_no_docstring: false
      inherited_members: false
      members:
        - main
        - RUN_ID
        - PHASES
      # Solo documentamos funciones locales
      group_by_category: true
      show_category_heading: true
Para más detalles sobre la lógica interna, consulta las [Utilidades de Orquestación](./helpers_orchestrate.md#helper-orchestration-system).

---

## 📤 Productor de Tareas
::: scripts.producer
    options:
      show_root_heading: false
      show_source: true
      members:
        - mq_client
        - QUEUE_MAP
        - send_task
        - _normalize_target_model
      group_by_category: true

---

## 📥 Consumidor de Resultados
::: scripts.consumer
    options:
      show_root_heading: false
      show_source: true
      filters:
        - "!^api"
        - "!^__"
      group_by_category: true