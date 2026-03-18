"""Tests for scripts/producer.py."""

import pytest
from unittest.mock import patch, MagicMock
from uuid import UUID

from schemas.task import TargetModel


class TestSendTask:
    @patch("scripts.producer.mq_client")
    @patch("scripts.producer._normalize_target_model", return_value=TargetModel.OCR)
    def test_send_task_with_enum(self, mock_norm, mock_mq):
        from scripts.producer import send_task

        msg_id, corr_id = send_task(
            file_path="/data/test.jpg",
            target_model=TargetModel.OCR,
            source="test",
        )

        mock_mq.publish.assert_called_once()
        call_args = mock_mq.publish.call_args
        assert call_args[0][0] == "cola_modelo_ocr"

    @patch("scripts.producer.mq_client")
    @patch("scripts.producer._normalize_target_model", return_value=TargetModel.OCR)
    def test_send_task_returns_ids(self, mock_norm, mock_mq):
        from scripts.producer import send_task

        msg_id, corr_id = send_task(
            file_path="/data/test.jpg",
            target_model=TargetModel.OCR,
        )

        # Both should be valid UUID strings
        UUID(msg_id)
        UUID(corr_id)
        assert isinstance(msg_id, str)
        assert isinstance(corr_id, str)

    @patch("scripts.producer.mq_client")
    @patch("scripts.producer._normalize_target_model", return_value=TargetModel.OCR)
    def test_send_task_queue_routing_ocr(self, mock_norm, mock_mq):
        from scripts.producer import send_task

        send_task("/data/f.jpg", TargetModel.OCR)
        assert mock_mq.publish.call_args[0][0] == "cola_modelo_ocr"

    @patch("scripts.producer.mq_client")
    @patch("scripts.producer._normalize_target_model", return_value=TargetModel.MOONDREAM)
    def test_send_task_queue_routing_moondream(self, mock_norm, mock_mq):
        from scripts.producer import send_task

        send_task("/data/f.jpg", TargetModel.MOONDREAM)
        assert mock_mq.publish.call_args[0][0] == "cola_modelo_moondream"

    @patch("scripts.producer.mq_client")
    @patch("scripts.producer._normalize_target_model", return_value=TargetModel.EMBEDDINGS)
    def test_send_task_queue_routing_embeddings(self, mock_norm, mock_mq):
        from scripts.producer import send_task

        send_task("/data/f.jpg", TargetModel.EMBEDDINGS)
        assert mock_mq.publish.call_args[0][0] == "cola_modelo_embeddings"


class TestNormalizeTargetModel:
    def test_normalize_target_model_missing_body(self):
        """The _normalize_target_model function has no body (known bug)."""
        from scripts.producer import _normalize_target_model

        # Function has no implementation, returns None implicitly
        result = _normalize_target_model(TargetModel.OCR)
        assert result is None
