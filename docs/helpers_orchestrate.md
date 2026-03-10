# 🛠️ Helper: Orchestration System

El módulo `orchestrate.py` es el motor de ejecución del pipeline. Proporciona las abstracciones necesarias para ejecutar scripts y fases como subprocesos, garantizando que el sistema sea robusto frente a fallos y capaz de responder a comandos de cancelación.



---

## ⚙️ Funciones Principales

::: scripts.helpers.orchestrate
    options:
      show_root_toc_entry: false
      members:
        - execute_phase_logic
        - run_script
        - run_phase
      show_source: true
      docstring_section_style: table

---

## 🛡️ Características de Robustez

### 🔄 Lógica de Reintentos (Retry Policy)
La función `run_script` implementa un bucle de reintentos automático. Si un script devuelve un código de salida distinto de `0`, el sistema:

1. Registra el fallo en los logs.
2. Espera el tiempo definido en `RETRY_DELAY`.
3. Incrementa el contador hasta alcanzar `MAX_RETRIES`.
4. Si agota los intentos, eleva un error fatal que detiene la fase.

### 🛑 Cancelación Activa (Kill Signal)
Durante la lectura de logs de cualquier subproceso, el orquestador invoca a `check_cancelled()`. Si se detecta una señal en la base de datos:

* Se utiliza la librería `psutil` para identificar el árbol de procesos hijo.
* Se envían señales de terminación (`kill`) para asegurar que no queden procesos "zombie" o tareas pesadas (como OCR) corriendo en segundo plano.

### 📝 Streaming de Logs y Estado
A diferencia de una ejecución simple, este orquestador realiza un **streaming de la salida estándar**:

* Cada línea impresa por el script hijo se captura, se añade a un buffer y se envía a la base de datos inmediatamente.
* Esto permite monitorizar el progreso del pipeline desde una interfaz externa o la base de datos en tiempo real.

---

## 📊 Flujo de Estados
El orquestador transita los scripts por los siguientes estados en la tabla de monitoreo:

| Estado | Condición |
| :--- | :--- |
| `running` | Al iniciar el subproceso. |
| `success` | Si el proceso termina con exit code 0. |
| `error` | Si se agotan los reintentos tras fallos. |
| `cancelled` | Si se detecta la señal de parada del usuario. |