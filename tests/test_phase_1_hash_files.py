"""Tests for scripts/phase_1/hash_files.py."""

import os
import pytest
from unittest.mock import patch, MagicMock

from scripts.phase_1.hash_files import compute_hashes, update_with_retries


class TestComputeHashes:
    def test_compute_hashes_valid_file(self, tmp_path):
        f = tmp_path / "test.bin"
        f.write_bytes(b"hello world content for hashing")

        result = compute_hashes(str(f))
        assert result is not None
        xx_signed, sha_hex = result
        assert isinstance(xx_signed, int)
        assert isinstance(sha_hex, str)
        assert len(sha_hex) == 64  # SHA-256 hex length

    def test_compute_hashes_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")

        result = compute_hashes(str(f))
        assert result is not None
        xx_signed, sha_hex = result
        assert isinstance(xx_signed, int)
        assert isinstance(sha_hex, str)

    def test_compute_hashes_nonexistent_file(self):
        result = compute_hashes("/nonexistent/path/file.bin")
        assert result is None

    def test_compute_hashes_uint64_to_int64_conversion(self, tmp_path):
        """Verify that large xxhash values are correctly converted to signed int64."""
        f = tmp_path / "data.bin"
        # Write enough data to potentially produce a large hash
        f.write_bytes(os.urandom(4096))

        result = compute_hashes(str(f))
        assert result is not None
        xx_signed, _ = result
        # Must fit in int64 range
        assert -(1 << 63) <= xx_signed < (1 << 63)

    def test_compute_hashes_small_vs_large_file(self, tmp_path):
        """Verify consistent behavior between small and large files."""
        small = tmp_path / "small.bin"
        small.write_bytes(b"small")

        large = tmp_path / "large.bin"
        large.write_bytes(b"x" * (8 * 1024 * 1024))  # 8MB

        r_small = compute_hashes(str(small))
        r_large = compute_hashes(str(large))

        assert r_small is not None
        assert r_large is not None
        # Different content should produce different hashes
        assert r_small[1] != r_large[1]


class TestUpdateWithRetries:
    @patch("scripts.phase_1.hash_files.execute_values")
    def test_update_with_retries_success(self, mock_exec_values):
        mock_conn = MagicMock()

        results = [(123456, "abc123def", 1)]
        result = update_with_retries(mock_conn, results)
        assert result is True
        mock_conn.commit.assert_called_once()
        mock_exec_values.assert_called_once()

    @patch("scripts.phase_1.hash_files.time.sleep")
    @patch("scripts.phase_1.hash_files.execute_values")
    def test_update_with_retries_failure_then_success(self, mock_exec_values, mock_sleep):
        mock_conn = MagicMock()

        # Fail twice then succeed
        mock_exec_values.side_effect = [Exception("fail1"), Exception("fail2"), None]

        results = [(123, "abc", 1)]
        result = update_with_retries(mock_conn, results, max_retries=5)
        assert result is True
        assert mock_exec_values.call_count == 3
        assert mock_conn.rollback.call_count == 2

    @patch("scripts.phase_1.hash_files.time.sleep")
    @patch("scripts.phase_1.hash_files.execute_values")
    def test_update_with_retries_all_failures(self, mock_exec_values, mock_sleep):
        mock_conn = MagicMock()
        mock_exec_values.side_effect = Exception("always fail")

        results = [(123, "abc", 1)]
        result = update_with_retries(mock_conn, results, max_retries=3)
        assert result is False
        assert mock_exec_values.call_count == 3

    @patch("scripts.phase_1.hash_files.time.sleep")
    @patch("scripts.phase_1.hash_files.execute_values")
    def test_update_with_retries_rollback_on_error(self, mock_exec_values, mock_sleep):
        mock_conn = MagicMock()
        mock_exec_values.side_effect = Exception("db error")

        results = [(123, "abc", 1)]
        update_with_retries(mock_conn, results, max_retries=2)

        assert mock_conn.rollback.call_count == 2
