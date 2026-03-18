"""Tests for scripts/phase_3/process_files.py."""

import json
import pytest
from unittest.mock import patch, MagicMock

from scripts.phase_3.process_files import sanitize_text, clean_llm_json, clasificar_documento


class TestSanitizeText:
    def test_sanitize_text_normal(self):
        assert sanitize_text("Hello world") == "Hello world"

    def test_sanitize_text_empty(self):
        assert sanitize_text("") == ""

    def test_sanitize_text_none(self):
        assert sanitize_text(None) == ""

    def test_sanitize_text_control_chars(self):
        result = sanitize_text("\x00\x01\x0b text")
        assert "\x00" not in result
        assert "\x01" not in result
        assert "\x0b" not in result
        assert "text" in result

    def test_sanitize_text_truncation(self):
        long_text = "A" * 5000
        result = sanitize_text(long_text, max_chars=3000)
        assert len(result) <= 3100  # includes truncation marker
        assert "…[truncated]…" in result

    def test_sanitize_text_custom_max(self):
        text = "A" * 100
        result = sanitize_text(text, max_chars=10)
        assert len(result) <= 30  # truncation marker adds some chars
        assert "…[truncated]…" in result


class TestCleanLlmJson:
    def test_clean_llm_json_with_markdown(self):
        raw = '```json\n{"a":1}\n```'
        assert clean_llm_json(raw) == '{"a":1}'

    def test_clean_llm_json_without_markdown(self):
        raw = '{"a":1}'
        assert clean_llm_json(raw) == '{"a":1}'

    def test_clean_llm_json_only_backticks(self):
        raw = '```\n{"a":1}\n```'
        assert clean_llm_json(raw) == '{"a":1}'


class TestClasificarDocumento:
    @patch("scripts.phase_3.process_files.requests.post")
    def test_clasificar_documento_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": '{"categoria": "Factura", "anio": "2023", "proyecto": "CSBORA"}'
                    }
                }
            ]
        }
        mock_post.return_value = mock_resp

        file_data = {
            "full_path": "/data/test.pdf",
            "file_type": ".pdf",
            "creation_year": 2023,
            "text_excerpt": "Factura número 12345",
        }
        result = clasificar_documento(file_data)

        assert result["categoria"] == "Factura"
        assert result["anio"] == "2023"
        assert result["proyecto"] == "CSBORA"

    @patch("scripts.phase_3.process_files.requests.post")
    def test_clasificar_documento_retry_on_error(self, mock_post):
        fail_resp = MagicMock()
        fail_resp.raise_for_status.side_effect = Exception("500 error")

        ok_resp = MagicMock()
        ok_resp.raise_for_status = MagicMock()
        ok_resp.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": '{"categoria": "Informe", "anio": "2022", "proyecto": "PROJ1"}'
                    }
                }
            ]
        }

        mock_post.side_effect = [fail_resp, fail_resp, ok_resp]

        file_data = {
            "full_path": "/data/test.pdf",
            "file_type": ".pdf",
            "text_excerpt": "Report",
        }
        result = clasificar_documento(file_data)
        assert result["categoria"] == "Informe"

    @patch("scripts.phase_3.process_files.requests.post")
    def test_clasificar_documento_all_retries_fail(self, mock_post):
        mock_post.side_effect = Exception("always fails")

        file_data = {
            "full_path": "/data/test.pdf",
            "file_type": ".pdf",
            "text_excerpt": "text",
        }
        result = clasificar_documento(file_data)

        assert result["categoria"] == "Desconocido"
        assert result["proyecto"] == "Desconocido"
        assert result["anio"] == "Desconocido"

    @patch("scripts.phase_3.process_files.requests.post")
    def test_clasificar_documento_invalid_json(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "this is not valid json"}}]
        }
        mock_post.return_value = mock_resp

        file_data = {
            "full_path": "/data/test.pdf",
            "file_type": ".pdf",
            "text_excerpt": "text",
        }
        result = clasificar_documento(file_data)
        assert result["categoria"] == "Desconocido"
