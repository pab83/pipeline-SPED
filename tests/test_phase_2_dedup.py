"""Tests for scripts/phase_2/dedup.py."""

import pytest
from unittest.mock import patch, MagicMock

from scripts.phase_2.dedup import choose_canonical, l2_distance_to_cosine_similarity, hash_level_canonicalization


class TestChooseCanonical:
    def test_choose_canonical_single(self):
        candidates = [{"id": 1, "is_pdf": False, "ocr_needed": False, "size_bytes": 100,
                        "modification_year": 2020, "depth": 3}]
        result = choose_canonical(candidates)
        assert result["id"] == 1

    def test_choose_canonical_pdf_digital_preferred(self):
        candidates = [
            {"id": 1, "is_pdf": False, "ocr_needed": False, "size_bytes": 100,
             "modification_year": 2020, "depth": 3},
            {"id": 2, "is_pdf": True, "ocr_needed": False, "size_bytes": 100,
             "modification_year": 2020, "depth": 3},
        ]
        result = choose_canonical(candidates)
        assert result["id"] == 2  # PDF digital wins

    def test_choose_canonical_larger_size_preferred(self):
        candidates = [
            {"id": 1, "is_pdf": True, "ocr_needed": False, "size_bytes": 500,
             "modification_year": 2020, "depth": 3},
            {"id": 2, "is_pdf": True, "ocr_needed": False, "size_bytes": 1000,
             "modification_year": 2020, "depth": 3},
        ]
        result = choose_canonical(candidates)
        assert result["id"] == 2  # Larger file wins

    def test_choose_canonical_newer_preferred(self):
        candidates = [
            {"id": 1, "is_pdf": True, "ocr_needed": False, "size_bytes": 100,
             "modification_year": 2018, "depth": 3},
            {"id": 2, "is_pdf": True, "ocr_needed": False, "size_bytes": 100,
             "modification_year": 2023, "depth": 3},
        ]
        result = choose_canonical(candidates)
        assert result["id"] == 2  # More recent wins

    def test_choose_canonical_shallower_preferred(self):
        candidates = [
            {"id": 1, "is_pdf": True, "ocr_needed": False, "size_bytes": 100,
             "modification_year": 2020, "depth": 5},
            {"id": 2, "is_pdf": True, "ocr_needed": False, "size_bytes": 100,
             "modification_year": 2020, "depth": 2},
        ]
        result = choose_canonical(candidates)
        assert result["id"] == 2  # Shallower depth wins

    def test_choose_canonical_lower_id_tiebreak(self):
        candidates = [
            {"id": 5, "is_pdf": True, "ocr_needed": False, "size_bytes": 100,
             "modification_year": 2020, "depth": 3},
            {"id": 2, "is_pdf": True, "ocr_needed": False, "size_bytes": 100,
             "modification_year": 2020, "depth": 3},
        ]
        result = choose_canonical(candidates)
        assert result["id"] == 2  # Lower ID wins


class TestL2ToCosine:
    def test_l2_to_cosine_zero_distance(self):
        assert l2_distance_to_cosine_similarity(0.0) == 1.0

    def test_l2_to_cosine_max_distance(self):
        assert l2_distance_to_cosine_similarity(2.0) == -1.0

    def test_l2_to_cosine_clamp(self):
        result = l2_distance_to_cosine_similarity(10.0)
        assert result == -1.0

    def test_l2_to_cosine_typical(self):
        result = l2_distance_to_cosine_similarity(0.5)
        expected = 1.0 - (0.5 * 0.5) / 2.0  # 0.875
        assert abs(result - expected) < 1e-9


class TestHashLevelCanonicalization:
    def test_hash_level_canonicalization_unique_files(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            ("sha_a", [1]),
            ("sha_b", [2]),
        ]

        hash_level_canonicalization(mock_cur)

        # executemany called with updates
        mock_cur.executemany.assert_called_once()
        updates = mock_cur.executemany.call_args[0][1]
        # All unique → all canonical
        for is_canonical, canonical_id, fid in updates:
            assert is_canonical is True
            assert canonical_id is None

    def test_hash_level_canonicalization_duplicates(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            ("sha_dup", [10, 20, 30]),
        ]

        hash_level_canonicalization(mock_cur)

        updates = mock_cur.executemany.call_args[0][1]
        assert len(updates) == 3
        # First is canonical
        assert updates[0] == (True, None, 10)
        # Others point to canonical
        assert updates[1] == (False, 10, 20)
        assert updates[2] == (False, 10, 30)

    def test_hash_level_canonicalization_no_records(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []

        hash_level_canonicalization(mock_cur)

        mock_cur.executemany.assert_not_called()
