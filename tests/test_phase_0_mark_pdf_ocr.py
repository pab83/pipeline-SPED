"""Tests for scripts/phase_0/mark_pdf_ocr.py."""

import pytest
from unittest.mock import patch, MagicMock

from scripts.phase_0.mark_pdf_ocr import pdf_needs_ocr


class TestPdfNeedsOcr:
    @patch("scripts.phase_0.mark_pdf_ocr.PdfReader")
    @patch("scripts.phase_0.mark_pdf_ocr.os.path.exists", return_value=True)
    def test_pdf_needs_ocr_digital_pdf(self, mock_exists, mock_reader_cls):
        page = MagicMock()
        page.get.return_value = {"/Font": {"/F1": {}}}
        mock_reader = MagicMock()
        mock_reader.pages = [page]
        mock_reader_cls.return_value = mock_reader

        result = pdf_needs_ocr("/data/digital.pdf")
        assert result is False

    @patch("scripts.phase_0.mark_pdf_ocr.PdfReader")
    @patch("scripts.phase_0.mark_pdf_ocr.os.path.exists", return_value=True)
    def test_pdf_needs_ocr_scanned_pdf(self, mock_exists, mock_reader_cls):
        page = MagicMock()
        page.get.return_value = {}  # No /Font key
        mock_reader = MagicMock()
        mock_reader.pages = [page]
        mock_reader_cls.return_value = mock_reader

        result = pdf_needs_ocr("/data/scanned.pdf")
        assert result is True

    @patch("scripts.phase_0.mark_pdf_ocr.os.path.exists", return_value=False)
    def test_pdf_needs_ocr_file_not_found(self, mock_exists):
        result = pdf_needs_ocr("/data/missing.pdf")
        assert result is None

    @patch("scripts.phase_0.mark_pdf_ocr.PdfReader", side_effect=Exception("corrupt"))
    @patch("scripts.phase_0.mark_pdf_ocr.os.path.exists", return_value=True)
    def test_pdf_needs_ocr_corrupt_pdf(self, mock_exists, mock_reader_cls):
        result = pdf_needs_ocr("/data/corrupt.pdf")
        assert result is None

    @patch("scripts.phase_0.mark_pdf_ocr.PdfReader")
    @patch("scripts.phase_0.mark_pdf_ocr.os.path.exists", return_value=True)
    def test_pdf_needs_ocr_indirect_object(self, mock_exists, mock_reader_cls):
        from pypdf.generic import IndirectObject

        indirect = MagicMock(spec=IndirectObject)
        indirect.get_object.return_value = {"/Font": {"/F1": {}}}

        page = MagicMock()
        page.get.return_value = indirect

        mock_reader = MagicMock()
        mock_reader.pages = [page]
        mock_reader_cls.return_value = mock_reader

        result = pdf_needs_ocr("/data/indirect.pdf")
        assert result is False
