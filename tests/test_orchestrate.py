"""Tests for scripts/helpers/orchestrate.py."""

import pytest
import sys
from unittest.mock import patch, MagicMock, PropertyMock

from scripts.exceptions import PipelineCancelledException


class TestRunScript:
    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.update_script_status")
    @patch("scripts.helpers.orchestrate.mark_script_finished")
    @patch("scripts.helpers.orchestrate.mark_script_running")
    @patch("scripts.helpers.orchestrate.check_cancelled", return_value=False)
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    def test_run_script_success(
        self, mock_popen, mock_get_db, mock_cancelled,
        mock_running, mock_finished, mock_update, mock_close
    ):
        from scripts.helpers.orchestrate import run_script

        proc = MagicMock()
        proc.stdout = iter(["line1\n", "line2\n"])
        proc.wait.return_value = 0
        proc.returncode = 0
        mock_popen.return_value = proc

        run_script(phase_id=1, script_name="test.py", phase_module="scripts.phase_0")

        mock_running.assert_called_once()
        mock_finished.assert_called_once()

    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.time.sleep")
    @patch("scripts.helpers.orchestrate.update_script_status")
    @patch("scripts.helpers.orchestrate.mark_script_finished")
    @patch("scripts.helpers.orchestrate.mark_script_running")
    @patch("scripts.helpers.orchestrate.check_cancelled", return_value=False)
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    def test_run_script_retry_on_failure(
        self, mock_popen, mock_get_db, mock_cancelled,
        mock_running, mock_finished, mock_update, mock_sleep, mock_close
    ):
        from scripts.helpers.orchestrate import run_script

        # First call fails, second succeeds
        fail_proc = MagicMock()
        fail_proc.stdout = iter(["fail\n"])
        fail_proc.wait.return_value = 1
        fail_proc.returncode = 1

        ok_proc = MagicMock()
        ok_proc.stdout = iter(["ok\n"])
        ok_proc.wait.return_value = 0
        ok_proc.returncode = 0

        mock_popen.side_effect = [fail_proc, ok_proc]

        run_script(phase_id=1, script_name="test.py", phase_module="scripts.phase_0")

        assert mock_popen.call_count == 2
        mock_finished.assert_called_once()

    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.time.sleep")
    @patch("scripts.helpers.orchestrate.mark_script_error")
    @patch("scripts.helpers.orchestrate.update_script_status")
    @patch("scripts.helpers.orchestrate.mark_script_running")
    @patch("scripts.helpers.orchestrate.check_cancelled", return_value=False)
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    @patch("scripts.helpers.orchestrate.MAX_RETRIES", 2)
    def test_run_script_max_retries_exceeded(
        self, mock_popen, mock_get_db, mock_cancelled,
        mock_running, mock_update, mock_error, mock_sleep, mock_close
    ):
        from scripts.helpers.orchestrate import run_script

        # Each retry creates a new Popen, so each needs its own stdout iterator
        def make_fail_proc(*args, **kwargs):
            proc = MagicMock()
            proc.stdout = iter(["fail\n"])
            proc.wait.return_value = 1
            proc.returncode = 1
            return proc

        mock_popen.side_effect = make_fail_proc

        with pytest.raises(RuntimeError, match="failed after"):
            run_script(phase_id=1, script_name="test.py", phase_module="scripts.phase_0")

        # mark_script_error is called once at line 127 (max retries), then again
        # at line 143 when the except Exception block catches the RuntimeError.
        assert mock_error.call_count == 2

    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.mark_script_cancelled")
    @patch("scripts.helpers.orchestrate.mark_script_error")
    @patch("scripts.helpers.orchestrate.update_script_status")
    @patch("scripts.helpers.orchestrate.mark_script_running")
    @patch("scripts.helpers.orchestrate.check_cancelled")
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    @patch("scripts.helpers.orchestrate.psutil.Process")
    @patch("scripts.helpers.orchestrate.RUN_ID", 1)
    def test_run_script_cancel_detection(
        self, mock_psutil, mock_popen, mock_get_db, mock_cancelled,
        mock_running, mock_update, mock_error, mock_script_cancelled, mock_close
    ):
        from scripts.helpers.orchestrate import run_script

        proc = MagicMock()
        proc.stdout = iter(["line\n"])
        proc.pid = 123
        mock_popen.return_value = proc

        mock_cancelled.return_value = True
        mock_psutil_proc = MagicMock()
        mock_psutil_proc.children.return_value = []
        mock_psutil.return_value = mock_psutil_proc

        with pytest.raises(PipelineCancelledException):
            run_script(phase_id=1, script_name="test.py", phase_module="scripts.phase_0")

        mock_script_cancelled.assert_called_once()

    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.mark_script_cancelled")
    @patch("scripts.helpers.orchestrate.mark_script_running")
    @patch("scripts.helpers.orchestrate.check_cancelled", return_value=False)
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    @patch("scripts.helpers.orchestrate.psutil.Process")
    def test_run_script_keyboard_interrupt(
        self, mock_psutil, mock_popen, mock_get_db, mock_cancelled,
        mock_running, mock_script_cancelled, mock_close
    ):
        from scripts.helpers.orchestrate import run_script

        proc = MagicMock()
        proc.stdout = MagicMock()
        proc.stdout.__iter__ = MagicMock(side_effect=KeyboardInterrupt)
        proc.pid = 123
        mock_popen.return_value = proc

        mock_psutil_proc = MagicMock()
        mock_psutil_proc.children.return_value = []
        mock_psutil.return_value = mock_psutil_proc

        with pytest.raises(PipelineCancelledException):
            run_script(phase_id=1, script_name="test.py", phase_module="scripts.phase_0")

    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.update_script_status")
    @patch("scripts.helpers.orchestrate.mark_script_finished")
    @patch("scripts.helpers.orchestrate.mark_script_running")
    @patch("scripts.helpers.orchestrate.check_cancelled", return_value=False)
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    def test_run_script_logs_streaming(
        self, mock_popen, mock_get_db, mock_cancelled,
        mock_running, mock_finished, mock_update, mock_close
    ):
        from scripts.helpers.orchestrate import run_script

        lines = ["log line 1\n", "log line 2\n", "log line 3\n"]
        proc = MagicMock()
        proc.stdout = iter(lines)
        proc.wait.return_value = 0
        proc.returncode = 0
        mock_popen.return_value = proc

        run_script(phase_id=1, script_name="test.py", phase_module="scripts.phase_0")

        # update_script_status is called for each line
        assert mock_update.call_count == len(lines)


class TestExecutePhaseLogic:
    @patch("scripts.helpers.orchestrate.update_phase_status")
    @patch("scripts.helpers.orchestrate.mark_phase_finished")
    @patch("scripts.helpers.orchestrate.mark_phase_started")
    @patch("scripts.helpers.orchestrate.get_or_create_phase_id", return_value=10)
    @patch("scripts.helpers.orchestrate.run_script")
    def test_execute_phase_logic_success(
        self, mock_run_script, mock_get_phase, mock_started,
        mock_finished, mock_update
    ):
        from scripts.helpers.orchestrate import execute_phase_logic

        execute_phase_logic(run_id=1, phase_number=0, scripts_list=["a.py", "b.py"])

        assert mock_run_script.call_count == 2
        mock_finished.assert_called_once_with(10)

    @patch("scripts.helpers.orchestrate.mark_phase_cancelled")
    @patch("scripts.helpers.orchestrate.mark_phase_started")
    @patch("scripts.helpers.orchestrate.get_or_create_phase_id", return_value=10)
    @patch("scripts.helpers.orchestrate.run_script", side_effect=PipelineCancelledException)
    def test_execute_phase_logic_cancellation(
        self, mock_run_script, mock_get_phase, mock_started, mock_cancelled
    ):
        from scripts.helpers.orchestrate import execute_phase_logic

        with pytest.raises(SystemExit) as exc_info:
            execute_phase_logic(run_id=1, phase_number=0, scripts_list=["a.py"])

        assert exc_info.value.code == 64
        mock_cancelled.assert_called_once_with(10)

    @patch("scripts.helpers.orchestrate.mark_phase_finished")
    @patch("scripts.helpers.orchestrate.mark_phase_started")
    @patch("scripts.helpers.orchestrate.get_or_create_phase_id", return_value=10)
    @patch("scripts.helpers.orchestrate.run_script", side_effect=RuntimeError("boom"))
    def test_execute_phase_logic_fatal_error(
        self, mock_run_script, mock_get_phase, mock_started, mock_finished
    ):
        from scripts.helpers.orchestrate import execute_phase_logic

        with pytest.raises(SystemExit) as exc_info:
            execute_phase_logic(run_id=1, phase_number=0, scripts_list=["a.py"])

        assert exc_info.value.code == 1


class TestRunPhase:
    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.mark_phase_finished")
    @patch("scripts.helpers.orchestrate.update_phase_status")
    @patch("scripts.helpers.orchestrate.check_cancelled", return_value=False)
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    def test_run_phase_success(
        self, mock_popen, mock_get_db, mock_cancelled,
        mock_update, mock_finished, mock_close
    ):
        from scripts.helpers.orchestrate import run_phase

        proc = MagicMock()
        proc.stdout = iter(["phase output\n"])
        proc.wait.return_value = 0
        proc.returncode = 0
        mock_popen.return_value = proc

        run_phase("scripts.phase_0.run_phase_0", phase_id=1)

        mock_finished.assert_called_once_with(1)

    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.mark_phase_error")
    @patch("scripts.helpers.orchestrate.mark_phase_cancelled")
    @patch("scripts.helpers.orchestrate.update_phase_status")
    @patch("scripts.helpers.orchestrate.check_cancelled", return_value=False)
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    def test_run_phase_cancelled_exit_code_64(
        self, mock_popen, mock_get_db, mock_cancelled,
        mock_update, mock_phase_cancelled, mock_phase_error, mock_close
    ):
        from scripts.helpers.orchestrate import run_phase

        proc = MagicMock()
        proc.stdout = iter([])
        proc.wait.return_value = 64
        proc.returncode = 64
        mock_popen.return_value = proc

        with pytest.raises(PipelineCancelledException):
            run_phase("scripts.phase_0.run_phase_0", phase_id=1)

        mock_phase_cancelled.assert_called_once_with(1)

    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.mark_phase_error")
    @patch("scripts.helpers.orchestrate.update_phase_status")
    @patch("scripts.helpers.orchestrate.check_cancelled", return_value=False)
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    def test_run_phase_error(
        self, mock_popen, mock_get_db, mock_cancelled,
        mock_update, mock_error, mock_close
    ):
        from scripts.helpers.orchestrate import run_phase

        proc = MagicMock()
        proc.stdout = iter([])
        proc.wait.return_value = 1
        proc.returncode = 1
        mock_popen.return_value = proc

        with pytest.raises(RuntimeError, match="Phase failed"):
            run_phase("scripts.phase_0.run_phase_0", phase_id=1)

    @patch("scripts.helpers.orchestrate.close_db")
    @patch("scripts.helpers.orchestrate.mark_phase_error")
    @patch("scripts.helpers.orchestrate.mark_phase_cancelled")
    @patch("scripts.helpers.orchestrate.update_phase_status")
    @patch("scripts.helpers.orchestrate.check_cancelled")
    @patch("scripts.helpers.orchestrate.get_db")
    @patch("scripts.helpers.orchestrate.subprocess.Popen")
    @patch("scripts.helpers.orchestrate.psutil.Process")
    @patch("scripts.helpers.orchestrate.RUN_ID", 1)
    def test_run_phase_cancel_signal(
        self, mock_psutil, mock_popen, mock_get_db, mock_cancelled,
        mock_update, mock_phase_cancelled, mock_phase_error, mock_close
    ):
        from scripts.helpers.orchestrate import run_phase

        proc = MagicMock()
        proc.stdout = iter(["line\n"])
        proc.pid = 999
        mock_popen.return_value = proc

        mock_cancelled.return_value = True
        mock_psutil_proc = MagicMock()
        mock_psutil_proc.children.return_value = []
        mock_psutil.return_value = mock_psutil_proc

        with pytest.raises(PipelineCancelledException):
            run_phase("scripts.phase_0.run_phase_0", phase_id=1)

        mock_phase_cancelled.assert_called()
