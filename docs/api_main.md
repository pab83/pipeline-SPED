# 🔌 API Endpoints

A continuación se detallan los puntos de entrada de la API. Estas rutas permiten interactuar con la Pipeline, consultar el estado de la base de datos y gestionar las tareas.

::: api.main
    options:
      members:
        - start_pipeline
        - run_phase_api
        - get_run_status
        - stop_pipeline
        - change_focus
      show_root_heading: false
      show_source: true
      # Filtros críticos:
      show_if_no_docstring: false
      docstring_section_style: list
      filters:
        - "!^app$"
        - "!^__"
      members_order: source
      # Solo mostramos las funciones (que en api.main son los endpoints) Esta opción le dice que solo muestre lo que está físicamente en el archivo
      group_by_category: true
      show_category_heading: false
      show_root_toc_entry: true
      
