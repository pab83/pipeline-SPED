# 📊 Helper: Database Status Management

El módulo `db_status.py` es el encargado de gestionar la persistencia del estado de ejecución en la base de datos de auditoría. Implementa una arquitectura de **actualización en cascada**, donde los cambios en el estado de un script se propagan automáticamente hacia la fase y el "Run" global.



---

## 🏗️ Gestión de Sesiones (SQLAlchemy)
Para optimizar el rendimiento y evitar fugas de conexiones en subprocesos, el módulo gestiona una sesión global única por proceso mediante un patrón de **Lazy Initialization**.

::: scripts.helpers.db_status
    options:
      show_root_toc_entry: false
      show_root_heading: false
      members:
        - get_db
        - close_db
      show_source: true

---

## 🔄 Jerarquía de Actualización (Cascada)

El sistema utiliza una lógica de **prioridad de estados**. Cuando un componente hijo (Script) cambia, el componente padre (Phase) recalcula su propio estado basándose en la siguiente jerarquía de severidad:

1.  **Running**: Si al menos un hijo está en progreso.
2.  **Error**: Si algún hijo falló (y no hay otros en ejecución).
3.  **Cancelled**: Si se recibió señal de parada por parte del usuario.
4.  **Finished**: Solo si todos los hijos han terminado con éxito.

### 🚀 Nivel 1: Pipeline Run
Representa la ejecución global del proceso. Controla el tiempo total y la fase actual activa.

::: scripts.helpers.db_status
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      members:
        - mark_run_started
        - update_run_status
        - mark_run_finished
        - mark_run_cancelled
        - check_cancelled

### 📂 Nivel 2: Pipeline Phase
Gestiona el progreso de los bloques lógicos del sistema (Ingesta, OCR, VLM, Clasificación).

::: scripts.helpers.db_status
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      members:
        - get_or_create_phase_id
        - mark_phase_started
        - update_phase_status
        - mark_phase_finished
        - mark_phase_error
        - mark_phase_cancelled

### 📜 Nivel 3: Pipeline Script
Gestiona la ejecución de los archivos `.py` individuales y el almacenamiento de sus logs capturados.

::: scripts.helpers.db_status
    options:
      show_root_toc_entry: false
      show_root_heading: true
      heading_level: 3
      members:
        - mark_script_running
        - update_script_status
        - mark_script_finished
        - mark_script_error
        - mark_script_cancelled

---

## 📝 Persistencia de Logs y Trazabilidad
Las funciones de nivel de script aceptan un buffer de logs que se sincroniza con la base de datos:

* **Streaming**: El orquestador envía fragmentos del buffer mientras el script corre.
* **Agregación**: Se utiliza `"\n".join(logs)` para reconstruir la salida de consola en la columna `logs`.
* **Visibilidad**: Esto permite que el estado del sistema sea observable desde la API REST o el Dashboard sin acceder a los archivos de log locales.

---

## ⚠️ Mecanismo de Cancelación (Interruptor de Seguridad)
La función `check_cancelled(run_id)` es el "Kill Switch" del sistema. 

1. El orquestador la consulta periódicamente durante la ejecución de los subprocesos.
2. Si un usuario marca el Run como `cancelled` desde la interfaz, esta función devuelve `True`.
3. El orquestador captura esta señal y procede a terminar (kill) todos los procesos hijos de manera ordenada pero inmediata.

---

### ¿Qué aporta este módulo al Pipeline?
1.  **Trazabilidad histórica**: Permite auditar ejecuciones pasadas, tiempos de procesamiento y errores específicos.
2.  **Consistencia**: El uso de `db.commit()` asegura que los cambios de estado sean atómicos.
3.  **Aislamiento**: Los scripts de proceso no necesitan conocer la lógica de base de datos; el orquestador se encarga de todo mediante este helper.