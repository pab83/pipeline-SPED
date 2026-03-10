# Sistema de Trazabilidad y Logging

El sistema de logging está diseñado para proporcionar una trazabilidad segregada y persistente. Cada fase del pipeline opera sobre su propio flujo de eventos, lo que facilita la depuración y el monitoreo de procesos específicos sin saturar un único archivo de registro.

## Arquitectura de Archivos por Fase

La ruta de los archivos de log se define dinámicamente en los módulos de configuración de cada etapa. El sistema utiliza el `RUN_ID` (identificador único de ejecución) para nombrar los archivos, asegurando que los logs de diferentes ejecuciones no se sobrescriban.

### Segmentación de Registros
| Fase | Archivo de Configuración | Definición del Log |
| :--- | :--- | :--- |
| **Fase 0** | `scripts.config.phase_0` | `run_{RUN_ID}_phase_0.log` |
| **Fase 1** | `scripts.config.phase_1` | `run_{RUN_ID}_phase_1.log` |
| **Fase 2** | `scripts.config.phase_2` | `run_{RUN_ID}_phase_2.log` |
| **Fase 3** | `scripts.config.phase_3` | `run_{RUN_ID}_phase_3.log` |



---

## Módulo de Registro: `scripts.helpers.logs`

Este helper centraliza la lógica de escritura y permite el cambio dinámico del destino del log mediante una variable privada de módulo.

::: scripts.helpers.logs
    options:
      show_root_toc_entry: false
      show_root_heading: false
      members:
        - set_log_file
        - log
      show_source: true

### Mecanismo de Funcionamiento
1. **Configuración Dinámica**: Mediante `set_log_file(path)`, el orquestador o el script de fase redirigen todas las trazas al archivo físico correspondiente antes de iniciar la lógica.
2. **Doble Salida (Tee)**: La función `log(msg)` realiza una escritura atómica en el archivo de texto y, simultáneamente, emite el mensaje por `stdout` para el monitoreo en consola.
3. **Integración con Buffer**: Si se proporciona un `logs_buffer`, el mensaje se adjunta a una lista en memoria. Este buffer es utilizado por el orquestador para sincronizar los logs con la base de datos de forma masiva, optimizando las operaciones de I/O de red.
4. **Codificación Robusta**: Se utiliza `utf-8` de forma explícita para prevenir fallos al registrar rutas con caracteres especiales o extracciones de texto complejas.

---

## Estructura de Directorios

El sistema garantiza la existencia de las rutas antes de iniciar cualquier operación de escritura, basándose en la configuración de `RESOURCES_DIR`.

* **Directorio Base**: `resources/logs/` (definido en `config.general`).
* **Auto-generación**: Se ejecuta `os.makedirs(LOG_DIR, exist_ok=True)` al inicio de la configuración de cada fase para evitar errores de "Directorio no encontrado".



---

## Trazabilidad en la Base de Datos

Aunque los archivos físicos residen en el servidor, el sistema de orquestación utiliza el `logs_buffer` para poblar la tabla `PipelineScript`. Esto permite que el estado "vivo" del proceso sea consultable vía API sin necesidad de acceder al sistema de archivos del servidor, proporcionando tres niveles de acceso:

1. **Archivo Físico**: Persistencia a largo plazo y depuración profunda.
2. **Base de Datos**: Visualización rápida desde el dashboard de monitoreo.
3. **Consola (Stdout)**: Monitoreo en tiempo real durante la ejecución manual.