# 🗄️ Estructura de Datos y Modelos

Esta sección describe la arquitectura de la base de datos PostgreSQL y los modelos de objetos que representan las entidades de la Pipeline.

## 📊 Modelos de Datos (SQLAlchemy)
::: api.models
    options:
      show_root_heading: false
      show_source: true
      # Filtramos para que solo aparezcan las CLASES
      filters:
        - "!^Base$"
        - "!^engine$"
        - "!^SessionLocal$"
        - "!^__"
      # Solo mostramos los miembros que son clases
      group_by_category: true
      show_category_heading: false
      

---

## 🔌 Conexión y Sesiones
::: api.db
    options:
      show_root_heading: false
      # Aquí solo queremos ver las funciones de utilidad, no las variables de config
      filters:
        - "!^URL$"
        - "!^engine$"
        - "!^SessionLocal$"