# 🚀 Pipeline de Auditoría y Procesamiento (v0)

Bienvenido a la documentación técnica oficial del proyecto **Pipeline RPR**. Este sistema es una solución robusta para el escaneo masivo, hashing de integridad, deduplicación y enriquecimiento de archivos mediante técnicas de OCR e Inteligencia Artificial.

---

## 🏗️ Arquitectura del Sistema

La arquitectura sigue un patrón de **Productor/Consumidor** desacoplado mediante **Redis**, permitiendo que el escaneo de archivos y su procesamiento posterior escalen de forma independiente.



### Componentes Principales:
* **API (FastAPI):** Puerta de enlace para la monitorización de estados y control de la base de datos.
* **Messaging (Redis):** Bus de eventos que gestiona las colas de tareas entre fases.
* **Database (PostgreSQL):** El "cerebro" donde reside el inventario de archivos, sus hashes (SHA256) y los resultados del procesamiento.
* **Workers (Scripts):** El motor de ejecución dividido en fases lógicas (0-3).

---

## 🔄 Flujo de Trabajo (Ciclo de Vida)

La pipeline está estructurada en **4 fases secuenciales**. Cada fase depende del éxito de la anterior para garantizar que no se procesen datos corruptos o duplicados.



| Fase | Identificador | Responsabilidad Principal | Script de Entrada |
| :--- | :--- | :--- | :--- |
| **Fase 0** | **Discovery** | Escaneo del sistema de archivos e inserción inicial en DB. | `run_phase_0.py` |
| **Fase 1** | **Integrity** | Cálculo de hashes SHA256 para detectar cambios o colisiones. | `run_phase_1.py` |
| **Fase 2** | **Refinement** | Deduplicación de registros y extracción de texto base. | `run_phase_2.py` |
| **Fase 3** | **Enrichment** | Procesamiento avanzado: OCR de imágenes y descripción por IA. | `run_phase_3.py` |

---

## 🛠️ Guía de Operaciones Rápidas

### Despliegue con Docker
Para levantar toda la infraestructura (Base de Datos, Redis y la API):
```bash
docker-compose up -d --build