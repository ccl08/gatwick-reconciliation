"""Tests for ID cleaning logic."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from cleaner import clean_order_id, clean_order_ids, format_for_bq, CleanStatus


# ---------------------------------------------------------------------------
# Rule 1: Remove trailing _N suffix
# ---------------------------------------------------------------------------

class TestTrailingSuffix:
    def test_ghp_trailing_1(self):
        r = clean_order_id("GHP-DW-1628713_1")
        assert r.cleaned == "GHP-DW-1628713"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_dw_trailing_1(self):
        r = clean_order_id("DW09926523ZAN_1")
        assert r.cleaned == "DW09926523ZAN"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_trailing_2(self):
        r = clean_order_id("GHP-DW-1234567_2")
        assert r.cleaned == "GHP-DW-1234567"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_trailing_10(self):
        r = clean_order_id("DW12345678ABC_10")
        assert r.cleaned == "DW12345678ABC"
        assert r.status == CleanStatus.AUTO_FIXED


# ---------------------------------------------------------------------------
# Rule 2a: LGW prefix WITH trailing 3 letters → main site format
# ---------------------------------------------------------------------------

class TestLGWWithTrailingLetters:
    def test_lgw_dw_with_trailing_letters(self):
        r = clean_order_id("LGW-DW-11176152ANN")
        assert r.cleaned == "DW11176152ANN"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_lgw_dw_different_suffix(self):
        r = clean_order_id("LGW-DW-09876543TIM")
        assert r.cleaned == "DW09876543TIM"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_lgw_lowercase(self):
        r = clean_order_id("lgw-DW-11176152ANN")
        assert r.cleaned == "DW11176152ANN"
        assert r.status == CleanStatus.AUTO_FIXED


# ---------------------------------------------------------------------------
# Rule 2b: LGW or LG prefix WITHOUT trailing letters → GHP format
# ---------------------------------------------------------------------------

class TestLGWWithoutTrailingLetters:
    def test_lgw_dw_no_trailing(self):
        r = clean_order_id("LGW-DW-08614442")
        assert r.cleaned == "GHP-DW-08614442"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_lg_dw_no_trailing(self):
        r = clean_order_id("LG-DW-10905890")
        assert r.cleaned == "GHP-DW-10905890"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_lgw_lowercase_no_trailing(self):
        r = clean_order_id("lgw-dw-08614442")
        assert r.cleaned == "GHP-dw-08614442"
        assert r.status == CleanStatus.AUTO_FIXED


# ---------------------------------------------------------------------------
# Rule 3: Already correct formats — pass through
# ---------------------------------------------------------------------------

class TestPassThrough:
    def test_ghp_format(self):
        r = clean_order_id("GHP-DW-1662491")
        assert r.cleaned == "GHP-DW-1662491"
        assert r.status == CleanStatus.CLEAN

    def test_dw_format_with_letters(self):
        r = clean_order_id("DW10316811TIM")
        assert r.cleaned == "DW10316811TIM"
        assert r.status == CleanStatus.CLEAN

    def test_dw_format_numeric_only(self):
        r = clean_order_id("DW10316811")
        assert r.cleaned == "DW10316811"
        assert r.status == CleanStatus.CLEAN


# ---------------------------------------------------------------------------
# Rule 2 (variant): LGW prefix with space separators
# ---------------------------------------------------------------------------

class TestLGWSpaceSeparator:
    def test_lgw_space_with_trailing_letters(self):
        r = clean_order_id("LGW DW 11119910REB")
        assert r.cleaned == "DW11119910REB"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_lgw_space_with_trailing_letters_plus_suffix(self):
        r = clean_order_id("LGW DW 11119910REB_1")
        assert r.cleaned == "DW11119910REB"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_lgw_space_no_trailing_letters(self):
        r = clean_order_id("LGW DW 08614442")
        assert r.cleaned == "GHP-DW-08614442"
        assert r.status == CleanStatus.AUTO_FIXED


# ---------------------------------------------------------------------------
# Rule 4: Bare numeric ID with trailing letters → add DW prefix
# ---------------------------------------------------------------------------

class TestBareNumericWithLetters:
    def test_bare_numeric_with_letters(self):
        r = clean_order_id("10915680LIZ")
        assert r.cleaned == "DW10915680LIZ"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_bare_numeric_with_letters_and_suffix(self):
        r = clean_order_id("10915680LIZ_1")
        assert r.cleaned == "DW10915680LIZ"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_bare_numeric_different_letters(self):
        r = clean_order_id("11234567ANN")
        assert r.cleaned == "DW11234567ANN"
        assert r.status == CleanStatus.AUTO_FIXED


# ---------------------------------------------------------------------------
# Rule 5: Unknown prefixes → flag for manual review
# ---------------------------------------------------------------------------

class TestUnknownFormats:
    def test_dfp_prefix(self):
        r = clean_order_id("DFP183373420")
        assert r.status == CleanStatus.FLAGGED
        assert "Unknown" in r.note

    def test_random_string(self):
        r = clean_order_id("ABCDEF123456")
        assert r.status == CleanStatus.FLAGGED

    def test_empty_string(self):
        r = clean_order_id("")
        assert r.status == CleanStatus.FLAGGED
        assert "Empty" in r.note

    def test_whitespace_only(self):
        r = clean_order_id("   ")
        assert r.status == CleanStatus.FLAGGED


# ---------------------------------------------------------------------------
# Combining rules: trailing suffix + prefix
# ---------------------------------------------------------------------------

class TestCombinedRules:
    def test_lgw_trailing_letters_plus_suffix(self):
        # LGW prefix with trailing letters AND _1 suffix
        r = clean_order_id("LGW-DW-11176152ANN_1")
        assert r.cleaned == "DW11176152ANN"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_lgw_no_trailing_letters_plus_suffix(self):
        r = clean_order_id("LGW-DW-08614442_2")
        assert r.cleaned == "GHP-DW-08614442"
        assert r.status == CleanStatus.AUTO_FIXED

    def test_ghp_with_suffix(self):
        r = clean_order_id("GHP-DW-1662491_1")
        assert r.cleaned == "GHP-DW-1662491"
        assert r.status == CleanStatus.AUTO_FIXED


# ---------------------------------------------------------------------------
# Batch cleaning
# ---------------------------------------------------------------------------

class TestBatchCleaning:
    def test_mixed_batch(self):
        ids = [
            "GHP-DW-1628713_1",
            "LGW-DW-11176152ANN",
            "LGW-DW-08614442",
            "GHP-DW-1662491",
            "DW10316811TIM",
            "DFP183373420",
        ]
        results = clean_order_ids(ids)
        assert len(results) == 6
        cleaned = [r.cleaned for r in results]
        assert "GHP-DW-1628713" in cleaned
        assert "DW11176152ANN" in cleaned
        assert "GHP-DW-08614442" in cleaned
        assert "GHP-DW-1662491" in cleaned
        assert "DW10316811TIM" in cleaned


# ---------------------------------------------------------------------------
# BQ formatting
# ---------------------------------------------------------------------------

class TestFormatForBQ:
    def test_format_single(self):
        result = format_for_bq(["GHP-DW-1662491"])
        assert result == '"GHP-DW-1662491"'

    def test_format_multiple(self):
        result = format_for_bq(["GHP-DW-1662491", "DW10316811TIM"])
        assert result == '"GHP-DW-1662491","DW10316811TIM"'

    def test_format_empty_list(self):
        result = format_for_bq([])
        assert result == ""

    def test_skips_empty_strings(self):
        result = format_for_bq(["GHP-DW-1662491", "", "DW10316811TIM"])
        assert result == '"GHP-DW-1662491","DW10316811TIM"'
