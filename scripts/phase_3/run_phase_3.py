import os
from typing import List

# Configuración y Excepciones
from scripts.config.phase_3 import LOG_FILE
from scripts.helpers.logs import set_log_file
from scripts.helpers.orchestrate import (
    execute_phase_logic,
    get_or_create_phase_id,
    run_script
)

# ----------------------------
# Parámetros de ejecución
# ----------------------------
RUN_ID: int = int(os.getenv("RUN_ID", "0"))
"""ID único de ejecución para el seguimiento de la Fase 3."""

PHASE_NUMBER: int = 3
"""Identificador de Fase: Inferencia de Modelos (OCR, VLM y Clasificación)."""

SCRIPTS: List[str] = [
     "describe_img.py",   # Envía imágenes a la cola para descripción visual
     "process_ocr_tasks.py",    # Consume y persiste resultados de OCR
     "process_files.py",  # Encola archivos para análisis de NLP
]
"""
Lista secuencial de scripts para la Fase 3. 
Nota: En esta fase, los scripts actúan como 'Producers' para Redis o 
'Consumers' para la persistencia de resultados de modelos IA.
"""

def main() -> None:
    """
    Punto de entrada para la orquestación de la Fase 3.
    
    Esta fase gestiona la interacción con modelos de Machine Learning (IA) 
    utilizando una arquitectura de colas basada en Redis.
    La función establece el entorno de logging y ejecuta la lógica de 
    control centralizada para la fase.
    """
    # Establecer el archivo de log antes de ejecutar la lógica
    set_log_file(LOG_FILE)
    
    # Delegar la orquestación a la función genérica
    execute_phase_logic(
        run_id=RUN_ID,
        phase_number=PHASE_NUMBER,
        scripts_list=SCRIPTS
    )

if __name__ == "__main__":
    main()