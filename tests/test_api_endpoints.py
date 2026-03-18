"""Tests for api/main.py (FastAPI endpoints)."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from api.models import PipelineRun, PipelinePhase, PipelineScript


class TestStartPipeline:
    @patch("api.main.launch_script")
    @patch("api.main.os.path.exists", return_value=True)
    def test_start_pipeline_success(self, mock_exists, mock_launch, client, db_session):
        resp = client.post("/start")
        assert resp.status_code == 200
        data = resp.json()
        assert "run_id" in data
        assert data["message"] == "Pipeline started"
        mock_launch.assert_called_once()

    def test_start_pipeline_zombie_detection(self, client, db_session):
        # Create a "running" run to trigger zombie detection
        run = PipelineRun(status="running")
        db_session.add(run)
        db_session.commit()

        resp = client.post("/start")
        assert resp.status_code == 400
        assert "already running" in resp.json()["detail"].lower() or "running" in resp.json()["detail"]

    @patch("api.main.os.path.exists", return_value=False)
    def test_start_pipeline_script_not_found(self, mock_exists, client, db_session):
        resp = client.post("/start")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    @patch("api.main.launch_script", side_effect=Exception("launch failed"))
    @patch("api.main.os.path.exists", return_value=True)
    def test_start_pipeline_launch_error(self, mock_exists, mock_launch, client, db_session):
        resp = client.post("/start")
        assert resp.status_code == 500
        assert "launch failed" in resp.json()["detail"]


class TestRunPhase:
    @patch("api.main.launch_script")
    @patch("api.main.os.path.exists", return_value=True)
    def test_run_phase_valid_phases(self, mock_exists, mock_launch, client, db_session):
        for phase in [0, 1, 2, 3, 4]:
            mock_launch.reset_mock()
            resp = client.post(f"/run_phase/{phase}")
            assert resp.status_code == 200, f"Phase {phase} failed"
            assert resp.json()["message"] == f"Phase {phase} started"

    def test_run_phase_invalid_number(self, client, db_session):
        resp = client.post("/run_phase/5")
        assert resp.status_code == 400
        assert "out of range" in resp.json()["detail"].lower()

    @patch("api.main.os.path.exists", return_value=False)
    def test_run_phase_script_not_found(self, mock_exists, client, db_session):
        resp = client.post("/run_phase/0")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()


class TestGetStatus:
    def test_get_status_existing_run(self, client, db_session):
        run = PipelineRun(status="running", current_phase=1, processed_files=50)
        db_session.add(run)
        db_session.commit()
        db_session.refresh(run)

        phase = PipelinePhase(run_id=run.run_id, phase_number=0, status="finished")
        db_session.add(phase)
        db_session.commit()
        db_session.refresh(phase)

        script = PipelineScript(
            phase_id=phase.phase_id,
            script_name="scan_files.py",
            status="finished",
            logs="line1\nline2",
        )
        db_session.add(script)
        db_session.commit()

        resp = client.get(f"/status/{run.run_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["run_id"] == run.run_id
        assert data["status"] == "running"
        assert data["current_phase"] == 1
        assert data["processed_files"] == 50
        assert len(data["phases"]) == 1
        assert data["phases"][0]["phase_number"] == 0
        assert len(data["phases"][0]["scripts"]) == 1
        assert data["phases"][0]["scripts"][0]["script_name"] == "scan_files.py"

    def test_get_status_not_found(self, client, db_session):
        resp = client.get("/status/99999")
        assert resp.status_code == 404


class TestStopPipeline:
    def test_stop_specific_run(self, client, db_session):
        run = PipelineRun(status="running")
        db_session.add(run)
        db_session.commit()
        db_session.refresh(run)

        resp = client.post(f"/stop?run_id={run.run_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["stopped_count"] == 1
        assert run.run_id in data["run_ids"]

    def test_stop_all_runs(self, client, db_session):
        for _ in range(3):
            db_session.add(PipelineRun(status="running"))
        db_session.commit()

        resp = client.post("/stop")
        assert resp.status_code == 200
        assert resp.json()["stopped_count"] == 3

    def test_stop_no_active_runs(self, client, db_session):
        resp = client.post("/stop")
        assert resp.status_code == 400
        assert "no active" in resp.json()["detail"].lower()

    def test_stop_specific_not_found(self, client, db_session):
        resp = client.post("/stop?run_id=99999")
        assert resp.status_code == 404


class TestChangeFocus:
    def test_change_focus_with_path(self, client):
        resp = client.post("/change_focus/2012/docs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["full_path"] == "/data/2012/docs"

    def test_change_focus_empty(self, client):
        resp = client.post("/change_focus/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["full_path"] == "/data"
