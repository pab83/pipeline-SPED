"""Tests for scripts/run_pipeline.py."""

import pytest
from unittest.mock import patch, MagicMock

from scripts.exceptions import PipelineCancelledException


class TestPhasesConfig:
    def test_phases_list(self):
        from scripts.run_pipeline import PHASES

        assert "scripts.phase_0.run_phase_0" in PHASES
        assert "scripts.phase_1.run_phase_1" in PHASES
        assert "scripts.phase_2.run_phase_2" in PHASES
        assert "scripts.phase_4.run_phase_4" in PHASES
        # phase_3 is commented out
        assert "scripts.phase_3.run_phase_3" not in PHASES


class TestRunIdFromEnv:
    def test_run_id_from_env(self):
        from scripts.run_pipeline import RUN_ID

        # RUN_ID is read from os.environ; we set it to "0" in conftest
        assert isinstance(RUN_ID, int)


class TestMain:
    @patch("scripts.run_pipeline.run_phase")
    @patch("scripts.run_pipeline.get_or_create_phase_id", return_value=10)
    @patch("scripts.run_pipeline.mark_run_finished")
    @patch("scripts.run_pipeline.mark_run_started")
    def test_main_success(self, mock_started, mock_finished, mock_get_phase, mock_run):
        from scripts.run_pipeline import main, PHASES

        main()

        mock_started.assert_called_once()
        mock_finished.assert_called_once()
        assert mock_run.call_count == len(PHASES)

    @patch("scripts.run_pipeline.run_phase", side_effect=PipelineCancelledException)
    @patch("scripts.run_pipeline.get_or_create_phase_id", return_value=10)
    @patch("scripts.run_pipeline.mark_run_cancelled")
    @patch("scripts.run_pipeline.mark_run_started")
    def test_main_cancellation(self, mock_started, mock_cancelled, mock_get_phase, mock_run):
        from scripts.run_pipeline import main

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0
        mock_cancelled.assert_called_once()

    @patch("scripts.run_pipeline.run_phase", side_effect=RuntimeError("boom"))
    @patch("scripts.run_pipeline.get_or_create_phase_id", return_value=10)
    @patch("scripts.run_pipeline.mark_run_finished")
    @patch("scripts.run_pipeline.mark_run_started")
    def test_main_error(self, mock_started, mock_finished, mock_get_phase, mock_run):
        from scripts.run_pipeline import main

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1
        mock_finished.assert_called_once()
