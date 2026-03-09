# ⚙️ Configuración del Sistema

En esta sección se detallan todas las constantes, variables de entorno y parámetros técnicos que gobiernan el comportamiento de la Pipeline en sus distintas etapas.

## 🌍 Configuración General
Variables globales del proyecto, rutas base y configuración de conexiones (DB/Redis).
::: scripts.config.general

---

## 🏗️ Configuración por Fases

Cada fase tiene su propio conjunto de parámetros ajustables (umbrales de similitud, extensiones permitidas, etc.) para permitir un ajuste fino del procesamiento.

### Fase 0: Discovery
Configuración del escaneo inicial.
::: scripts.config.phase_0

### Fase 1: Integrity
Parámetros para el cálculo de firmas digitales.
::: scripts.config.phase_1

### Fase 2: Refinement
Configuración de deduplicación y limpieza.
::: scripts.config.phase_2

### Fase 3: Enrichment
Configuración de modelos de IA y OCR.
::: scripts.config.phase_3

---

## 🛠️ Utilidades de Configuración (Helpers)
Funciones auxiliares para la carga de entornos y gestión de logs.
::: scripts.helpers.logs