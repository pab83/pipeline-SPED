"""Tests for scripts/helpers/db_status.py."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from api.models import PipelineRun, PipelinePhase, PipelineScript
import scripts.helpers.db_status as db_status_module


@pytest.fixture(autouse=True)
def reset_db_session():
    """Reset CURRENT_DB_SESSION between tests."""
    db_status_module.CURRENT_DB_SESSION = None
    yield
    db_status_module.CURRENT_DB_SESSION = None


@pytest.fixture()
def patched_db(mock_session_local):
    """Provide the patched db session via mock_session_local fixture."""
    return mock_session_local


# ------------ get_db / close_db tests --------------

class TestGetDb:
    def test_get_db_creates_session(self, patched_db):
        session = db_status_module.get_db()
        assert session is not None

    def test_get_db_reuses_session(self, patched_db):
        s1 = db_status_module.get_db()
        s2 = db_status_module.get_db()
        assert s1 is s2


class TestCloseDb:
    def test_close_db(self, patched_db):
        db_status_module.get_db()
        db_status_module.close_db()
        assert db_status_module.CURRENT_DB_SESSION is None

    def test_close_db_when_none(self):
        # Should not raise when session is already None
        db_status_module.CURRENT_DB_SESSION = None
        db_status_module.close_db()
        assert db_status_module.CURRENT_DB_SESSION is None


# ------------ Run status tests --------------

class TestMarkRunStarted:
    def test_mark_run_started(self, patched_db):
        run = PipelineRun(status="pending")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        db_status_module.mark_run_started(run.run_id)

        patched_db.refresh(run)
        assert run.status == "running"
        assert run.started_at is not None


class TestMarkRunFinished:
    def test_mark_run_finished_normal(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        db_status_module.mark_run_finished(run.run_id)

        patched_db.refresh(run)
        assert run.status == "finished"
        assert run.finished_at is not None

    def test_mark_run_finished_preserves_error(self, patched_db):
        run = PipelineRun(status="error")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        db_status_module.mark_run_finished(run.run_id)

        patched_db.refresh(run)
        assert run.status == "error"


class TestMarkRunCancelled:
    def test_mark_run_cancelled(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        db_status_module.mark_run_cancelled(run.run_id)

        patched_db.refresh(run)
        assert run.status == "cancelled"
        assert run.finished_at is not None


# ------------ update_run_status tests --------------

class TestUpdateRunStatus:
    def _create_run_with_phases(self, db, statuses):
        run = PipelineRun(status="running")
        db.add(run)
        db.commit()
        db.refresh(run)
        for i, s in enumerate(statuses):
            phase = PipelinePhase(run_id=run.run_id, phase_number=i, status=s)
            db.add(phase)
        db.commit()
        return run

    def test_update_run_status_running(self, patched_db):
        run = self._create_run_with_phases(patched_db, ["running", "pending"])
        db_status_module.update_run_status(run.run_id)
        patched_db.refresh(run)
        assert run.status == "running"

    def test_update_run_status_error(self, patched_db):
        run = self._create_run_with_phases(patched_db, ["finished", "error"])
        db_status_module.update_run_status(run.run_id)
        patched_db.refresh(run)
        assert run.status == "error"

    def test_update_run_status_cancelled(self, patched_db):
        run = self._create_run_with_phases(patched_db, ["finished", "cancelled"])
        db_status_module.update_run_status(run.run_id)
        patched_db.refresh(run)
        assert run.status == "cancelled"

    def test_update_run_status_all_finished(self, patched_db):
        run = self._create_run_with_phases(patched_db, ["finished", "finished"])
        db_status_module.update_run_status(run.run_id)
        patched_db.refresh(run)
        assert run.status == "finished"

    def test_update_run_status_mixed_failed(self, patched_db):
        run = self._create_run_with_phases(patched_db, ["finished", "unknown_state"])
        db_status_module.update_run_status(run.run_id)
        patched_db.refresh(run)
        assert run.status == "failed"

    def test_update_run_status_with_processed_files(self, patched_db):
        run = self._create_run_with_phases(patched_db, ["running"])
        db_status_module.update_run_status(run.run_id, processed_files=500)
        patched_db.refresh(run)
        assert run.processed_files == 500

    def test_update_run_status_no_phases(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        db_status_module.update_run_status(run.run_id)
        patched_db.refresh(run)
        # No phases → return early, status unchanged
        assert run.status == "running"


# ------------ Phase status tests --------------

class TestMarkPhaseStarted:
    def test_mark_phase_started(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="pending")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        db_status_module.mark_phase_started(phase.phase_id)

        patched_db.refresh(phase)
        patched_db.refresh(run)
        assert phase.status == "running"
        assert phase.started_at is not None
        assert run.current_phase == 0


class TestMarkPhaseFinished:
    def test_mark_phase_finished_normal(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        db_status_module.mark_phase_finished(phase.phase_id)

        patched_db.refresh(phase)
        assert phase.status == "finished"

    def test_mark_phase_finished_preserves_error(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="error")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        db_status_module.mark_phase_finished(phase.phase_id)

        patched_db.refresh(phase)
        assert phase.status == "error"


class TestMarkPhaseCancelled:
    def test_mark_phase_cancelled(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        db_status_module.mark_phase_cancelled(phase.phase_id)

        patched_db.refresh(phase)
        assert phase.status == "cancelled"
        assert phase.finished_at is not None


class TestMarkPhaseError:
    def test_mark_phase_error(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        db_status_module.mark_phase_error(phase.phase_id, "Something broke")

        patched_db.refresh(phase)
        assert phase.status == "error"
        assert phase.error_message == "Something broke"


# ------------ update_phase_status tests --------------

class TestUpdatePhaseStatus:
    def _create_phase_with_scripts(self, db, run_id, statuses):
        phase = PipelinePhase(run_id=run_id, phase_number=0, status="running")
        db.add(phase)
        db.commit()
        db.refresh(phase)
        for s in statuses:
            script = PipelineScript(phase_id=phase.phase_id, script_name="s.py", status=s)
            db.add(script)
        db.commit()
        return phase

    def test_update_phase_status_from_scripts(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = self._create_phase_with_scripts(patched_db, run.run_id, ["finished", "error"])
        db_status_module.update_phase_status(phase.phase_id)
        patched_db.refresh(phase)
        assert phase.status == "error"

    def test_update_phase_status_propagates_to_run(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = self._create_phase_with_scripts(patched_db, run.run_id, ["finished", "finished"])
        db_status_module.update_phase_status(phase.phase_id)
        patched_db.refresh(run)
        assert run.status == "finished"

    def test_update_phase_status_no_scripts(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        # No scripts → return early
        db_status_module.update_phase_status(phase.phase_id)
        patched_db.refresh(phase)
        assert phase.status == "running"


# ------------ Script status tests --------------

class TestMarkScriptRunning:
    def test_mark_script_running(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        db_status_module.mark_script_running(phase.phase_id, "test.py")

        script = patched_db.query(PipelineScript).filter_by(
            phase_id=phase.phase_id, script_name="test.py"
        ).first()
        assert script is not None
        assert script.status == "running"

    def test_mark_script_running_existing(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        script = PipelineScript(phase_id=phase.phase_id, script_name="test.py", status="pending")
        patched_db.add(script)
        patched_db.commit()

        db_status_module.mark_script_running(phase.phase_id, "test.py")

        patched_db.refresh(script)
        assert script.status == "running"


class TestMarkScriptFinished:
    def test_mark_script_finished(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        script = PipelineScript(phase_id=phase.phase_id, script_name="test.py", status="running")
        patched_db.add(script)
        patched_db.commit()

        db_status_module.mark_script_finished(phase.phase_id, "test.py", logs=["done"])

        patched_db.refresh(script)
        assert script.status == "finished"
        assert script.logs == "done"


class TestMarkScriptError:
    def test_mark_script_error(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        script = PipelineScript(phase_id=phase.phase_id, script_name="test.py", status="running")
        patched_db.add(script)
        patched_db.commit()

        db_status_module.mark_script_error(
            phase.phase_id, "test.py", "crash", logs=["error log"]
        )

        patched_db.refresh(script)
        assert script.status == "error"
        assert script.error_message == "crash"
        assert script.logs == "error log"


class TestMarkScriptCancelled:
    def test_mark_script_cancelled(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        script = PipelineScript(phase_id=phase.phase_id, script_name="test.py", status="running")
        patched_db.add(script)
        patched_db.commit()

        db_status_module.mark_script_cancelled(phase.phase_id, "test.py")

        patched_db.refresh(script)
        assert script.status == "cancelled"


# ------------ update_script_status tests --------------

class TestUpdateScriptStatus:
    def test_update_script_status_creates_if_missing(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        db_status_module.update_script_status(phase.phase_id, "new_script.py")

        script = patched_db.query(PipelineScript).filter_by(
            phase_id=phase.phase_id, script_name="new_script.py"
        ).first()
        assert script is not None
        assert script.status == "pending"

    def test_update_script_status_syncs_with_phase_cancelled(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="cancelled")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        script = PipelineScript(phase_id=phase.phase_id, script_name="s.py", status="running")
        patched_db.add(script)
        patched_db.commit()

        db_status_module.update_script_status(phase.phase_id, "s.py")

        patched_db.refresh(script)
        assert script.status == "cancelled"

    def test_update_script_status_syncs_with_phase_error(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="error")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        script = PipelineScript(phase_id=phase.phase_id, script_name="s.py", status="running")
        patched_db.add(script)
        patched_db.commit()

        db_status_module.update_script_status(phase.phase_id, "s.py")

        patched_db.refresh(script)
        assert script.status == "error"

    def test_update_script_status_syncs_with_phase_finished(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="finished")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        script = PipelineScript(phase_id=phase.phase_id, script_name="s.py", status="running")
        patched_db.add(script)
        patched_db.commit()

        db_status_module.update_script_status(phase.phase_id, "s.py")

        patched_db.refresh(script)
        assert script.status == "finished"

    def test_update_script_status_updates_logs(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="running")
        patched_db.add(phase)
        patched_db.commit()
        patched_db.refresh(phase)

        script = PipelineScript(phase_id=phase.phase_id, script_name="s.py", status="running")
        patched_db.add(script)
        patched_db.commit()

        db_status_module.update_script_status(
            phase.phase_id, "s.py", logs=["line1", "line2"]
        )

        patched_db.refresh(script)
        assert script.logs == "line1\nline2"


# ------------ Auxiliary functions tests --------------

class TestGetOrCreatePhaseId:
    def test_get_or_create_phase_id_creates(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        phase_id = db_status_module.get_or_create_phase_id(run.run_id, phase_number=2)
        assert phase_id is not None

        phase = patched_db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
        assert phase is not None
        assert phase.phase_number == 2

    def test_get_or_create_phase_id_returns_existing(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        id1 = db_status_module.get_or_create_phase_id(run.run_id, phase_number=1)
        id2 = db_status_module.get_or_create_phase_id(run.run_id, phase_number=1)
        assert id1 == id2


class TestCheckCancelled:
    def test_check_cancelled_true(self, patched_db):
        run = PipelineRun(status="cancelled")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        assert db_status_module.check_cancelled(run.run_id) is True

    def test_check_cancelled_false(self, patched_db):
        run = PipelineRun(status="running")
        patched_db.add(run)
        patched_db.commit()
        patched_db.refresh(run)

        assert db_status_module.check_cancelled(run.run_id) is False

    def test_check_cancelled_nonexistent(self, patched_db):
        assert db_status_module.check_cancelled(99999) is False
