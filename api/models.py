from sqlalchemy import Column, Integer, BigInteger, String, Text, ForeignKey, TIMESTAMP
from sqlalchemy.orm import relationship
from api.db import Base
from pydantic import BaseModel, Field
from typing import List, Optional

# ----------------------------
# SQLAlchemy ORM
# ----------------------------

class PipelineRun(Base):
    """ Modelo ORM que representa una ejecución completa del pipeline. Contiene información general sobre la ejecución, como su estado global, la fase actual, el número de archivos procesados, y timestamps para seguimiento. Este modelo se relaciona con PipelinePhase para mantener un desglose jerárquico de las fases dentro de cada ejecución del pipeline."""
    __tablename__ = "pipeline_runs"
    __table_args__ = {"schema": "pipeline_status"}  # <--- schema separado

    run_id = Column(BigInteger, primary_key=True,autoincrement=True)
    status = Column(String(20), nullable=False, default="pending")  # pending, running, finished, error, cancelled
    current_phase = Column(Integer, default=0)
    processed_files = Column(BigInteger, default=0)
    started_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    error_message = Column(Text)

    phases = relationship("PipelinePhase", back_populates="run")


class PipelinePhase(Base):
    """ Modelo ORM que representa una fase específica dentro de una ejecución del pipeline. Cada fase tiene un número identificador, un estado, timestamps para seguimiento, y un campo para mensajes de error en caso de que la fase falle. Este modelo se relaciona con PipelineRun para mantener la jerarquía de ejecución y con PipelineScript para mantener un desglose detallado de los scripts ejecutados dentro de cada fase."""
    __tablename__ = "pipeline_phases"
    __table_args__ = {"schema": "pipeline_status"}

    phase_id = Column(BigInteger, primary_key=True)
    run_id = Column(BigInteger, ForeignKey("pipeline_status.pipeline_runs.run_id", ondelete="CASCADE"))
    phase_number = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default="pending")  # pending, running, finished, error
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    error_message = Column(Text)

    run = relationship("PipelineRun", back_populates="phases")
    scripts = relationship("PipelineScript", back_populates="phase")


class PipelineScript(Base):
    """ Modelo ORM que representa la ejecución de un script específico dentro de una fase del pipeline. Contiene información detallada sobre el script, como su nombre, estado, logs capturados durante su ejecución, y mensajes de error si el script falla. Este modelo se relaciona con PipelinePhase para mantener la jerarquía de fases y scripts dentro de cada ejecución del pipeline."""
    __tablename__ = "pipeline_scripts"
    __table_args__ = {"schema": "pipeline_status"}

    script_id = Column(BigInteger, primary_key=True)
    phase_id = Column(BigInteger, ForeignKey("pipeline_status.pipeline_phases.phase_id", ondelete="CASCADE"))
    script_name = Column(Text, nullable=False)
    status = Column(String(20), nullable=False, default="pending")  # pending, running, finished, error
    logs = Column(Text)
    error_message = Column(Text)

    phase = relationship("PipelinePhase", back_populates="scripts")


# ----------------------------
# Pydantic models para API
# ----------------------------

class ScriptStatus(BaseModel):
    """
    Representa el estado de una ejecución de un script.
    Contiene el nombre, estado, logs y detalles de error si lo hubiera.
    """
    script_name: str = Field(
        ..., 
        json_schema_extra={"example":"process_data.py"}, 
        description="Nombre del archivo script que se está ejecutando"
    )
    status: str = Field(
        ..., 
        json_schema_extra={"example":"finished"}, 
        description="Estado del script: pending, running, finished, error o cancelled"
    )
    error_message: Optional[str] = Field(
        None, 
        json_schema_extra={"example":"File not found"}, 
        description="Detalle del error si el script falló"
    )
    logs: Optional[List[str]] = Field(
        [], 
        json_schema_extra={"example":["Iniciando...", "Cargando CSV...", "Proceso completado"]}, 
        description="Lista de líneas de log capturadas durante la ejecución del script"
    )

class PhaseStatus(BaseModel):
    """
    Representa el estado de una ejecución de una fase.
    Contiene numero de fase, estado, desglose jerárquico de scripts y detalles de error si lo hubiera.
    """
    phase_number: int = Field(
        ..., 
        json_schema_extra={"example":1}, 
        description="Número de la fase dentro del ciclo de la pipeline (0-3)"
    )
    status: str = Field(
        ..., 
        json_schema_extra={"example":"running"}, 
        description="Estado actual de la fase completa"
    )
    scripts: List[ScriptStatus] = Field(
        [], 
        description="Lista detallada de todos los scripts ejecutados en esta fase"
    )
    error_message: Optional[str] = Field(
        None, 
        description="Error general a nivel de fase si lo hubiera"
    )

class RunStatus(BaseModel):
    """
    Representa el estado global de una ejecución de la pipeline.
    Contiene numero de run, estado, desglose jerárquico de fases y scripts y detalles de error si lo hubiera.
    """
    run_id: int = Field(..., json_schema_extra={"example":101}, description="ID único de la ejecución")
    status: str = Field(..., json_schema_extra={"example":"running"}, description="Estado global del proceso")
    current_phase: int = Field(..., description="Fase que se está ejecutando actualmente")
    processed_files: Optional[int] = Field(0, description="Contador de archivos procesados hasta el momento")
    phases: List[PhaseStatus] = Field([], description="Detalle de cada una de las fases del run")