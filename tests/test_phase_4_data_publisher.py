"""Tests for scripts/phase_4/data_publisher.py."""

import pytest
from datetime import datetime

from scripts.phase_4.data_publisher import safe_str, safe_int, safe_bool, safe_timestamp, build_directory_levels


class TestSafeStr:
    def test_safe_str_none(self):
        assert safe_str(None) == ""

    def test_safe_str_with_value(self):
        assert safe_str("  hello  ") == "hello"

    def test_safe_str_default(self):
        assert safe_str(None, default="N/A") == "N/A"


class TestSafeInt:
    def test_safe_int_valid(self):
        assert safe_int("42") == 42

    def test_safe_int_none(self):
        assert safe_int(None) is None

    def test_safe_int_invalid(self):
        assert safe_int("abc") is None

    def test_safe_int_custom_default(self):
        assert safe_int(None, default=0) == 0


class TestSafeBool:
    def test_safe_bool_none(self):
        assert safe_bool(None) is False

    def test_safe_bool_true_bool(self):
        assert safe_bool(True) is True

    def test_safe_bool_false_bool(self):
        assert safe_bool(False) is False

    def test_safe_bool_int_nonzero(self):
        assert safe_bool(1) is True

    def test_safe_bool_int_zero(self):
        assert safe_bool(0) is False

    def test_safe_bool_string_true(self):
        assert safe_bool("true") is True
        assert safe_bool("t") is True
        assert safe_bool("1") is True

    def test_safe_bool_string_false(self):
        assert safe_bool("false") is False
        assert safe_bool("0") is False


class TestSafeTimestamp:
    def test_safe_timestamp_none(self):
        assert safe_timestamp(None) is None

    def test_safe_timestamp_value(self):
        dt = datetime(2024, 1, 1)
        assert safe_timestamp(dt) is dt


class TestBuildDirectoryLevels:
    def test_build_directory_levels_normal(self):
        result = build_directory_levels("/data/2012/4-revisiones/subcarpeta/otro", skip_levels=3)
        assert result == ["4-revisiones", "subcarpeta", "otro", None, None]

    def test_build_directory_levels_short_path(self):
        result = build_directory_levels("/data/2012", skip_levels=3)
        assert result == [None, None, None, None, None]

    def test_build_directory_levels_deep_path(self):
        result = build_directory_levels(
            "/data/2012/a/b/c/d/e/f/g", skip_levels=3
        )
        # Only first 5 relevant levels
        assert len(result) == 5
        assert result == ["a", "b", "c", "d", "e"]

    def test_build_directory_levels_custom_skip(self):
        result = build_directory_levels("/a/b/c/d/e", skip_levels=0)
        # With skip_levels=0, all parts are relevant
        assert len(result) == 5
        # Path("/a/b/c/d/e").parts = ('/', 'a', 'b', 'c', 'd', 'e')
        assert result[0] == "/"
        assert result[1] == "a"
