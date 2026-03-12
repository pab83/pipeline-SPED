import os
from typing import List

# Configuración y Excepciones
from scripts.config.phase_4 import LOG_FILE
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
"""ID único de ejecución para el seguimiento de la Fase 4."""

PHASE_NUMBER: int = 4
"""Identificador de Fase: Publicar datos."""

SCRIPTS: List[str] = [
    "create_olap_db",
    "data_publisher.py"
]
"""
Lista secuencial de scripts para la Fase 4. 
Nota: En esta fase, los scripts actúan como 
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