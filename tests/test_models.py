"""Tests for api/models.py (Pydantic response models)."""

import pytest
from api.models import ScriptStatus, PhaseStatus, RunStatus


class TestScriptStatus:
    def test_script_status_valid(self):
        s = ScriptStatus(script_name="test.py", status="running")
        assert s.script_name == "test.py"
        assert s.status == "running"

    def test_script_status_optional_fields(self):
        s = ScriptStatus(script_name="test.py", status="pending")
        assert s.error_message is None
        assert s.logs == []


class TestPhaseStatus:
    def test_phase_status_valid(self):
        script = ScriptStatus(script_name="s.py", status="finished")
        p = PhaseStatus(phase_number=1, status="running", scripts=[script])
        assert p.phase_number == 1
        assert p.status == "running"
        assert len(p.scripts) == 1
        assert p.scripts[0].script_name == "s.py"

    def test_phase_status_defaults(self):
        p = PhaseStatus(phase_number=0, status="pending")
        assert p.scripts == []
        assert p.error_message is None


class TestRunStatus:
    def test_run_status_valid(self):
        phase = PhaseStatus(phase_number=0, status="finished")
        r = RunStatus(
            run_id=1,
            status="running",
            current_phase=0,
            processed_files=100,
            phases=[phase],
        )
        assert r.run_id == 1
        assert r.status == "running"
        assert r.current_phase == 0
        assert r.processed_files == 100
        assert len(r.phases) == 1

    def test_run_status_defaults(self):
        r = RunStatus(run_id=1, status="pending", current_phase=0)
        assert r.processed_files == 0
        assert r.phases == []
