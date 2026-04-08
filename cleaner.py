"""
ID cleaning logic for Rakuten affiliate reconciliation.

Cleaning rules:
1. Remove trailing _1, _2 etc (suffix after underscore)
2. LGW- or LG- prefix (dash or space separator):
   - With trailing 3 letters (3 alpha chars at end) → main site format: strip prefix
     LGW-DW-11176152ANN → DW11176152ANN
     LGW DW 11119910REB → DW11119910REB
   - Without trailing letters → GHP format: replace prefix with GHP-
     LGW-DW-08614442 → GHP-DW-08614442
     LG-DW-10905890  → GHP-DW-10905890
3. GHP-DW- or DW prefix → pass through as-is
4. Bare numeric ID with trailing 3 letters (no prefix) → add DW prefix
   10915680LIZ → DW10915680LIZ
5. Anything else → flag for manual review
"""

import re
from dataclasses import dataclass
from enum import Enum


class CleanStatus(str, Enum):
    CLEAN = "clean"          # Already correct, no changes
    AUTO_FIXED = "auto_fixed"  # Cleaned automatically
    FLAGGED = "flagged"      # Unknown format, needs manual review


@dataclass
class CleanResult:
    original: str
    cleaned: str
    status: CleanStatus
    note: str = ""


# Matches trailing _1, _2, _N suffix
_TRAILING_SUFFIX_RE = re.compile(r'_\d+$')

# Matches LGW- or LG- prefix (case-insensitive), dash or space separator
_LGW_PREFIX_RE = re.compile(r'^(LGW?)[- ]', re.IGNORECASE)

# Matches bare numeric ID with trailing 3 alpha chars and no prefix, e.g. 10915680LIZ
_BARE_NUMERIC_WITH_LETTERS_RE = re.compile(r'^\d+[A-Za-z]{3}$')

# Matches 3 trailing alphabetic characters at end of numeric ID segment
_TRAILING_LETTERS_RE = re.compile(r'[A-Za-z]{3}$')

# Valid pass-through patterns
_GHP_RE = re.compile(r'^GHP-DW-', re.IGNORECASE)
_DW_RE = re.compile(r'^DW\d', re.IGNORECASE)


def _strip_trailing_suffix(order_id: str) -> tuple[str, bool]:
    """Remove _N suffix. Returns (cleaned, was_changed)."""
    cleaned = _TRAILING_SUFFIX_RE.sub('', order_id)
    return cleaned, cleaned != order_id


def clean_order_id(raw: str) -> CleanResult:
    """
    Clean a single Order ID according to the defined rules.
    Returns a CleanResult with the cleaned ID and status.
    """
    if not raw or not raw.strip():
        return CleanResult(
            original=raw,
            cleaned=raw,
            status=CleanStatus.FLAGGED,
            note="Empty ID"
        )

    order_id = raw.strip()
    original = order_id
    changed = False

    # Step 1: Remove trailing _N suffix
    order_id, suffix_removed = _strip_trailing_suffix(order_id)
    if suffix_removed:
        changed = True

    # Step 2: Handle LGW- or LG- prefix
    lgw_match = _LGW_PREFIX_RE.match(order_id)
    if lgw_match:
        # Strip the LGW- or LG- prefix to get the core
        core = order_id[lgw_match.end():]  # e.g. "DW-11176152ANN" or "DW 11119910REB"
        core = core.replace(' ', '-')  # normalise space separators to dashes

        # Check if the ID ends with 3 trailing alpha chars → main site format
        if _TRAILING_LETTERS_RE.search(core):
            # e.g. DW-11176152ANN → strip the dash: DW11176152ANN
            cleaned_core = core.replace('-', '', 1)  # remove first dash only
            order_id = cleaned_core
        else:
            # No trailing letters → GHP format
            order_id = 'GHP-' + core  # e.g. GHP-DW-08614442

        changed = True
        return CleanResult(
            original=original,
            cleaned=order_id,
            status=CleanStatus.AUTO_FIXED,
            note=f"LGW/LG prefix converted"
        )

    # Step 3: Valid pass-through formats
    if _GHP_RE.match(order_id) or _DW_RE.match(order_id):
        status = CleanStatus.AUTO_FIXED if changed else CleanStatus.CLEAN
        return CleanResult(
            original=original,
            cleaned=order_id,
            status=status,
            note="Suffix removed" if changed else ""
        )

    # Step 4: Bare numeric ID with trailing 3 letters → add DW prefix
    if _BARE_NUMERIC_WITH_LETTERS_RE.match(order_id):
        return CleanResult(
            original=original,
            cleaned='DW' + order_id,
            status=CleanStatus.AUTO_FIXED,
            note="DW prefix added"
        )

    # Step 5: Unknown format → flag for manual review
    return CleanResult(
        original=original,
        cleaned=order_id,  # return best-effort cleaned (suffix stripped)
        status=CleanStatus.FLAGGED,
        note=f"Unknown ID format"
    )


def clean_order_ids(raw_ids: list[str]) -> list[CleanResult]:
    """Clean a list of Order IDs and return results."""
    return [clean_order_id(raw) for raw in raw_ids]


def format_for_bq(cleaned_ids: list[str]) -> str:
    """Format cleaned IDs as a BQ IN clause string."""
    quoted = [f'"{id_}"' for id_ in cleaned_ids if id_]
    return ','.join(quoted)


def summarise(results: list[CleanResult]) -> dict:
    """Return a summary dict of cleaning results."""
    clean = [r for r in results if r.status == CleanStatus.CLEAN]
    fixed = [r for r in results if r.status == CleanStatus.AUTO_FIXED]
    flagged = [r for r in results if r.status == CleanStatus.FLAGGED]
    return {
        "total": len(results),
        "clean": len(clean),
        "auto_fixed": len(fixed),
        "flagged": len(flagged),
        "clean_items": clean,
        "auto_fixed_items": fixed,
        "flagged_items": flagged,
    }
