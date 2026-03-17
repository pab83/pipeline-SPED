"""
Shared fixtures for the pipeline-rpr test suite.

Provides:
- SQLite in-memory database session with schema workaround for PostgreSQL schemas
- FastAPI TestClient with dependency override
- Temporary directory fixture
"""

import os
import pytest
from unittest.mock import patch, MagicMock

# Set environment variables BEFORE importing any project modules
os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")
os.environ.setdefault("PGHOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "testdb")
os.environ.setdefault("RUN_ID", "0")
os.environ.setdefault("BASE_PATH", "/tmp/test_data")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")

from sqlalchemy import create_engine, event, text, BigInteger, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api.db import Base
from api.models import PipelineRun, PipelinePhase, PipelineScript


# Patch BigInteger columns to use INTEGER for SQLite so autoincrement works
# SQLite only auto-generates IDs for "INTEGER PRIMARY KEY", not "BIGINT".
import sqlalchemy
_orig_BigInteger_compile = None


@pytest.fixture()
def db_engine():
    """Create an SQLite in-memory engine with pipeline_status schema attached."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_conn, connection_record):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS pipeline_status")

    # Compile BigInteger as INTEGER for SQLite so autoincrement PKs work
    from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
    original_visit = SQLiteTypeCompiler.visit_big_integer
    SQLiteTypeCompiler.visit_big_integer = lambda self, type_, **kw: "INTEGER"

    Base.metadata.create_all(bind=engine)

    # Restore original after table creation
    SQLiteTypeCompiler.visit_big_integer = original_visit

    yield engine
    engine.dispose()


@pytest.fixture()
def db_session(db_engine):
    """Provide a transactional SQLAlchemy session bound to the in-memory engine."""
    TestingSessionLocal = sessionmaker(bind=db_engine, autocommit=False, autoflush=False)
    session = TestingSessionLocal()
    yield session
    session.close()


@pytest.fixture()
def client(db_session):
    """
    FastAPI TestClient with get_db dependency overridden to use the
    in-memory SQLite session.
    """
    from fastapi.testclient import TestClient
    from api.main import app, get_db

    def _override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db

    # Patch db_status helpers that api.main imports via "from ... import *"
    # so they don't open their own CURRENT_DB_SESSION to a real database.
    with patch("api.main.close_db"), \
         patch("api.main.mark_run_cancelled"), \
         patch("api.main.mark_run_finished"):
        with TestClient(app) as c:
            yield c

    app.dependency_overrides.clear()


@pytest.fixture()
def mock_session_local(db_session):
    """
    Patch SessionLocal in both api.db and scripts.helpers.db_status
    so that all code paths use the in-memory session.
    """
    def _make_session():
        return db_session

    with patch("api.db.SessionLocal", side_effect=_make_session), \
         patch("scripts.helpers.db_status.SessionLocal", side_effect=_make_session):
        yield db_session


@pytest.fixture()
def tmp_dir(tmp_path):
    """Provide a temporary directory for filesystem tests."""
    return tmp_path
