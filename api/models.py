from sqlalchemy import Column, Integer, BigInteger, String, Text, ForeignKey, TIMESTAMP
from sqlalchemy.orm import relationship
from api.db import Base
from pydantic import BaseModel
from typing import List, Optional

# ----------------------------
# SQLAlchemy ORM
# ----------------------------

class PipelineRun(Base):
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
    script_name: str
    status: str
    error_message: Optional[str] = None
    logs: Optional[List[str]] = []

class PhaseStatus(BaseModel):
    phase_number: int
    status: str
    scripts: List[ScriptStatus] = []
    error_message: Optional[str] = None

class RunStatus(BaseModel):
    run_id: int
    status: str
    current_phase: int
    processed_files: Optional[int] = 0
    phases: List[PhaseStatus] = []