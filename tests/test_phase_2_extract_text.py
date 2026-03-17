"""Tests for scripts/phase_2/extract_text.py."""

import pytest
from unittest.mock import patch, MagicMock

from scripts.phase_2.extract_text import (
    safe_read_text_file,
    extract_text_from_pdf,
    extract_text_from_docx,
)


class TestSafeReadTextFile:
    def test_safe_read_text_file_utf8(self, tmp_path):
        f = tmp_path / "utf8.txt"
        f.write_text("Hola mundo UTF-8 ñ", encoding="utf-8")
        result = safe_read_text_file(str(f), max_chars=1000)
        assert "Hola mundo" in result
        assert "ñ" in result

    def test_safe_read_text_file_latin1(self, tmp_path):
        f = tmp_path / "latin1.txt"
        f.write_bytes("café résumé".encode("latin-1"))
        result = safe_read_text_file(str(f), max_chars=1000)
        assert len(result) > 0

    def test_safe_read_text_file_truncation(self, tmp_path):
        f = tmp_path / "long.txt"
        f.write_text("A" * 5000, encoding="utf-8")
        result = safe_read_text_file(str(f), max_chars=100)
        assert len(result) == 100

    def test_safe_read_text_file_nonexistent(self):
        result = safe_read_text_file("/nonexistent/path.txt", max_chars=100)
        assert result == ""


class TestExtractTextFromPdf:
    @patch("scripts.phase_2.extract_text.PdfReader")
    def test_extract_text_from_pdf_digital(self, mock_reader_cls):
        page = MagicMock()
        page.extract_text.return_value = "Page 1 text content"
        mock_reader = MagicMock()
        mock_reader.pages = [page]
        mock_reader_cls.return_value = mock_reader

        result = extract_text_from_pdf("/data/digital.pdf", max_chars=1000, ocr_needed=False)
        assert "Page 1 text" in result

    def test_extract_text_from_pdf_ocr_needed(self):
        result = extract_text_from_pdf("/data/scan.pdf", max_chars=1000, ocr_needed=True)
        assert result == ""

    @patch("scripts.phase_2.extract_text.PdfReader")
    def test_extract_text_from_pdf_truncation(self, mock_reader_cls):
        page = MagicMock()
        page.extract_text.return_value = "A" * 5000
        mock_reader = MagicMock()
        mock_reader.pages = [page]
        mock_reader_cls.return_value = mock_reader

        result = extract_text_from_pdf("/data/long.pdf", max_chars=100, ocr_needed=False)
        assert len(result) <= 100

    @patch("scripts.phase_2.extract_text.PdfReader", side_effect=Exception("corrupt"))
    def test_extract_text_from_pdf_corrupt(self, mock_reader_cls):
        result = extract_text_from_pdf("/data/corrupt.pdf", max_chars=1000, ocr_needed=False)
        assert result == ""


class TestExtractTextFromDocx:
    @patch("scripts.phase_2.extract_text.Document")
    def test_extract_text_from_docx_paragraphs(self, mock_doc_cls):
        para1 = MagicMock()
        para1.text = "First paragraph"
        para2 = MagicMock()
        para2.text = "Second paragraph"

        mock_doc = MagicMock()
        mock_doc.paragraphs = [para1, para2]
        mock_doc.tables = []
        mock_doc_cls.return_value = mock_doc

        result = extract_text_from_docx("/data/doc.docx", max_chars=1000)
        assert "First paragraph" in result
        assert "Second paragraph" in result

    @patch("scripts.phase_2.extract_text.Document")
    def test_extract_text_from_docx_tables(self, mock_doc_cls):
        mock_doc = MagicMock()
        mock_doc.paragraphs = []

        cell = MagicMock()
        cell.text = "cell value"
        row = MagicMock()
        row.cells = [cell]
        table = MagicMock()
        table.rows = [row]
        mock_doc.tables = [table]
        mock_doc_cls.return_value = mock_doc

        result = extract_text_from_docx("/data/tables.docx", max_chars=1000)
        assert "cell value" in result

    @patch("scripts.phase_2.extract_text.Document")
    def test_extract_text_from_docx_truncation(self, mock_doc_cls):
        para = MagicMock()
        para.text = "B" * 5000

        mock_doc = MagicMock()
        mock_doc.paragraphs = [para]
        mock_doc.tables = []
        mock_doc_cls.return_value = mock_doc

        result = extract_text_from_docx("/data/long.docx", max_chars=100)
        assert len(result) <= 100

    @patch("scripts.phase_2.extract_text.Document", side_effect=Exception("corrupt"))
    def test_extract_text_from_docx_corrupt(self, mock_doc_cls):
        result = extract_text_from_docx("/data/corrupt.docx", max_chars=1000)
        assert result == ""
