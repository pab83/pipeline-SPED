# ⚠️ Exceptions: Control de Flujo Personalizado

El módulo `exceptions.py` define las señales de interrupción específicas del dominio para el pipeline. Estas excepciones permiten que el orquestador distinga entre un fallo técnico (bug, error de red) y una acción deliberada del usuario o del sistema.

---

##  PipelineCancelledException

Esta excepción es la pieza central del mecanismo de parada segura. No representa un error de código, sino una **señal de control**.

::: scripts.exceptions.PipelineCancelledException
    options:
      show_root_toc_entry: false
      show_source: true
      show_bases: true



---

##  Funcionamiento y Captura

Cuando el orquestador detecta una señal de cancelación en la base de datos (vía `db_status.check_cancelled`), lanza esta excepción. El ciclo de vida de la captura es el siguiente:

1.  **Detección**: El `helper.orchestrate` identifica el estado `cancelled`.
2.  **Lanzamiento**: Se eleva `PipelineCancelledException`.
3.  **Limpieza (Cleanup)**: El bloque `except PipelineCancelledException` en el orquestador captura la señal.
4.  **Terminación de Procesos**: Se invocan las rutinas de `psutil` para matar los subprocesos (OCR, VLM, LLM) que aún estén activos.
5.  **Persistencia**: Se marca el estado final en la base de datos como `cancelled` en lugar de `error`.

###  Códigos de Salida Relacionados

| Excepción | Exit Code | Significado |
| :--- | :--- | :--- |
| `PipelineCancelledException` | **64** | El proceso se detuvo correctamente tras una orden de cancelación. |
| `RuntimeError` / `Exception` | **1** | El proceso falló por un error no controlado. |

---

##  Ventajas de este Enfoque

* **Parada Limpia**: Evita que queden procesos "huérfanos" consumiendo CPU o GPU en el servidor.
* **Claridad en Logs**: En el panel de control, el usuario ve un estado de "Cancelado" (color gris/naranja) en lugar de un "Error" (color rojo), lo que evita alarmas innecesarias.
* **Aislamiento**: Permite que las funciones de bajo nivel notifiquen la parada hacia arriba en la pila de llamadas hasta llegar al orquestador principal.