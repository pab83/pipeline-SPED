"""Tests for scripts/phase_0/scan_files.py."""

import os
import pytest
from unittest.mock import patch, MagicMock


class TestNormalizePath:
    def test_normalize_path_empty(self):
        from scripts.phase_0.scan_files import normalize_path
        assert normalize_path("") == ""

    def test_normalize_path_strips_spaces(self):
        from scripts.phase_0.scan_files import normalize_path
        result = normalize_path("  /data/file.txt  ")
        assert result == "/data/file.txt"

    def test_normalize_path_expands_vars(self, monkeypatch):
        from scripts.phase_0.scan_files import normalize_path
        monkeypatch.setenv("MY_DIR", "/custom")
        result = normalize_path("$MY_DIR/file.txt")
        assert "/custom/file.txt" in result

    def test_normalize_path_normalizes_separators(self):
        from scripts.phase_0.scan_files import normalize_path
        # os.path.normpath on POSIX preserves a leading "//" (POSIX spec),
        # so we test with an interior double-slash instead.
        result = normalize_path("/data//subdir//file")
        assert result == "/data/subdir/file"

    def test_normalize_path_relative_with_base(self):
        from scripts.phase_0.scan_files import normalize_path
        result = normalize_path("subdir/file.txt", base_path="/data")
        assert result == os.path.normpath("/data/subdir/file.txt")

    def test_normalize_path_absolute_ignores_base(self):
        from scripts.phase_0.scan_files import normalize_path
        result = normalize_path("/absolute/path.txt", base_path="/data")
        assert result == "/absolute/path.txt"


class TestProcessFile:
    @patch("scripts.phase_0.scan_files.BASE_SEP_COUNT", 1)
    def test_process_file_valid(self):
        from scripts.phase_0.scan_files import process_file

        entry = MagicMock()
        entry.name = "document.txt"
        entry.path = "/data/docs/document.txt"
        stat = MagicMock()
        stat.st_size = 1024
        stat.st_ctime = 1672531200.0  # 2023-01-01
        stat.st_mtime = 1704067200.0  # 2024-01-01
        entry.stat.return_value = stat

        result = process_file(entry)
        assert result is not None
        path, name, ext, size, ctime_year, mtime_year, depth, is_pdf = result
        assert name == "document.txt"
        assert ext == ".txt"
        assert size == 1024
        assert is_pdf is False

    @patch("scripts.phase_0.scan_files.BASE_SEP_COUNT", 1)
    def test_process_file_pdf_flag(self):
        from scripts.phase_0.scan_files import process_file

        entry = MagicMock()
        entry.name = "report.pdf"
        entry.path = "/data/report.pdf"
        stat = MagicMock()
        stat.st_size = 2048
        stat.st_ctime = 1672531200.0
        stat.st_mtime = 1672531200.0
        entry.stat.return_value = stat

        result = process_file(entry)
        assert result is not None
        assert result[7] is True  # is_pdf

    @patch("scripts.phase_0.scan_files.BASE_SEP_COUNT", 1)
    def test_process_file_non_pdf(self):
        from scripts.phase_0.scan_files import process_file

        entry = MagicMock()
        entry.name = "image.jpg"
        entry.path = "/data/image.jpg"
        stat = MagicMock()
        stat.st_size = 500
        stat.st_ctime = 1672531200.0
        stat.st_mtime = 1672531200.0
        entry.stat.return_value = stat

        result = process_file(entry)
        assert result is not None
        assert result[7] is False  # is_pdf

    def test_process_file_exception(self):
        from scripts.phase_0.scan_files import process_file

        entry = MagicMock()
        entry.stat.side_effect = PermissionError("denied")

        result = process_file(entry)
        assert result is None


class TestListTopDirectories:
    def test_list_top_directories(self, tmp_dir):
        from scripts.phase_0.scan_files import list_top_directories

        (tmp_dir / "dir_a").mkdir()
        (tmp_dir / "dir_b").mkdir()
        (tmp_dir / "file.txt").write_text("data")

        result = list_top_directories(str(tmp_dir))
        dir_names = [os.path.basename(p) for p in result]
        assert "dir_a" in dir_names
        assert "dir_b" in dir_names
        assert "file.txt" not in dir_names


class TestGenerateFiles:
    def test_generate_files(self, tmp_dir):
        from scripts.phase_0.scan_files import generate_files

        (tmp_dir / "sub").mkdir()
        (tmp_dir / "file1.txt").write_text("a")
        (tmp_dir / "sub" / "file2.txt").write_text("b")

        entries = list(generate_files(str(tmp_dir)))
        names = [e.name for e in entries]
        assert "file1.txt" in names
        assert "file2.txt" in names


class TestChunks:
    def test_chunks(self):
        from scripts.phase_0.scan_files import chunks

        result = list(chunks([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_chunks_empty(self):
        from scripts.phase_0.scan_files import chunks

        result = list(chunks([], 2))
        assert result == []

    def test_chunks_exact(self):
        from scripts.phase_0.scan_files import chunks

        result = list(chunks([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]
