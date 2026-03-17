"""Tests for schemas/task.py and schemas/result.py."""

import pytest
from datetime import datetime
from pydantic import ValidationError

from schemas.task import TaskMessage, TargetModel
from schemas.result import ResultMessage, Status, ErrorInfo


class TestTargetModel:
    def test_target_model_values(self):
        assert TargetModel.OCR.value == "ocr"
        assert TargetModel.MOONDREAM.value == "moondream"
        assert TargetModel.EMBEDDINGS.value == "embeddings"


class TestTaskMessage:
    def test_task_message_valid(self):
        msg = TaskMessage(
            message_id="abc-123",
            correlation_id="corr-456",
            timestamp=datetime(2024, 1, 1),
            source="phase_2",
            target_model=TargetModel.OCR,
            payload={"file_path": "/data/file.pdf"},
        )
        assert msg.message_id == "abc-123"
        assert msg.correlation_id == "corr-456"
        assert msg.target_model == TargetModel.OCR
        assert msg.payload == {"file_path": "/data/file.pdf"}

    def test_task_message_defaults(self):
        msg = TaskMessage(
            message_id="abc",
            correlation_id="corr",
            timestamp=datetime.now(),
            source="test",
            target_model=TargetModel.OCR,
            payload={},
        )
        assert msg.schema_version == "1.0"
        assert msg.retry_count == 0
        assert msg.max_retries == 3

    def test_task_message_missing_required(self):
        with pytest.raises(ValidationError):
            TaskMessage(
                correlation_id="corr",
                timestamp=datetime.now(),
                source="test",
                target_model=TargetModel.OCR,
                payload={},
            )


class TestStatus:
    def test_status_enum_values(self):
        assert Status.SUCCESS.value == "success"
        assert Status.ERROR.value == "error"


class TestErrorInfo:
    def test_error_info_defaults(self):
        err = ErrorInfo(type="ConnectionError", message="timeout")
        assert err.retryable is True


class TestResultMessage:
    def test_result_message_success(self):
        msg = ResultMessage(
            message_id="r-1",
            correlation_id="c-1",
            model="OCRWorker",
            status=Status.SUCCESS,
            result="extracted text",
        )
        assert msg.status == Status.SUCCESS
        assert msg.result == "extracted text"

    def test_result_message_error(self):
        err = ErrorInfo(type="Timeout", message="took too long")
        msg = ResultMessage(
            message_id="r-2",
            correlation_id="c-2",
            model="OCRWorker",
            status=Status.ERROR,
            error=err,
        )
        assert msg.status == Status.ERROR
        assert msg.error.type == "Timeout"

    def test_result_message_missing_required(self):
        with pytest.raises(ValidationError):
            ResultMessage(
                correlation_id="c-1",
                model="OCRWorker",
                status=Status.SUCCESS,
            )
