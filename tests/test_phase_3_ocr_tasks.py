"""Tests for scripts/phase_3/process_ocr_tasks.py."""

import pytest
from unittest.mock import patch, MagicMock

from schemas.result import ResultMessage, Status, ErrorInfo
from scripts.phase_3.process_ocr_tasks import extract_text_from_result, send_ocr_tasks


class TestExtractTextFromResult:
    def test_extract_text_success_string(self):
        msg = ResultMessage(
            message_id="m1",
            correlation_id="c1",
            model="OCRWorker",
            status=Status.SUCCESS,
            result="extracted text here",
        )
        assert extract_text_from_result(msg) == "extracted text here"

    def test_extract_text_success_dict_text_key(self):
        msg = ResultMessage(
            message_id="m2",
            correlation_id="c2",
            model="OCRWorker",
            status=Status.SUCCESS,
            result={"text": "abc"},
        )
        assert extract_text_from_result(msg) == "abc"

    def test_extract_text_success_dict_content_key(self):
        msg = ResultMessage(
            message_id="m3",
            correlation_id="c3",
            model="OCRWorker",
            status=Status.SUCCESS,
            result={"content": "abc"},
        )
        assert extract_text_from_result(msg) == "abc"

    def test_extract_text_success_dict_unknown_keys(self):
        msg = ResultMessage(
            message_id="m4",
            correlation_id="c4",
            model="OCRWorker",
            status=Status.SUCCESS,
            result={"foo": "bar"},
        )
        result = extract_text_from_result(msg)
        assert "foo" in result
        assert "bar" in result

    def test_extract_text_error_status(self):
        err = ErrorInfo(type="Timeout", message="took too long")
        msg = ResultMessage(
            message_id="m5",
            correlation_id="c5",
            model="OCRWorker",
            status=Status.ERROR,
            error=err,
        )
        assert extract_text_from_result(msg) is None

    def test_extract_text_none_result(self):
        msg = ResultMessage(
            message_id="m6",
            correlation_id="c6",
            model="OCRWorker",
            status=Status.SUCCESS,
            result=None,
        )
        assert extract_text_from_result(msg) is None

    def test_extract_text_other_type(self):
        msg = ResultMessage(
            message_id="m7",
            correlation_id="c7",
            model="OCRWorker",
            status=Status.SUCCESS,
            result=123,
        )
        assert extract_text_from_result(msg) == "123"


class TestSendOcrTasks:
    @patch("scripts.phase_3.process_ocr_tasks.send_task")
    def test_send_ocr_tasks(self, mock_send_task):
        mock_send_task.return_value = ("msg-id-1", "corr-id-1")

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [(1, "/data/file1.pdf"), (2, "/data/file2.pdf")]

        correlation_map = {}
        count = send_ocr_tasks(mock_conn, correlation_map)

        assert count == 2
        assert mock_send_task.call_count == 2
        # Verify INSERT was called for ocr_task_map
        assert mock_cur.execute.call_count >= 3  # SELECT + 2 INSERTs

    @patch("scripts.phase_3.process_ocr_tasks.send_task")
    def test_send_ocr_tasks_no_pending(self, mock_send_task):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = []

        count = send_ocr_tasks(mock_conn, {})
        assert count == 0
        mock_send_task.assert_not_called()

    @patch("scripts.phase_3.process_ocr_tasks.send_task")
    def test_send_ocr_tasks_error_handling(self, mock_send_task):
        # First call raises, second succeeds
        mock_send_task.side_effect = [Exception("redis down"), ("m2", "c2")]

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [(1, "/data/f1.pdf"), (2, "/data/f2.pdf")]

        count = send_ocr_tasks(mock_conn, {})
        # First failed, second succeeded
        assert count == 1
