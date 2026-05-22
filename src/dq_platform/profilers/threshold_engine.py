"""Promise → DQOps rule_parameters projection.

Direct port of MLG-Spec-Reader/server/dq-promise-thresholds.ts. Shared
fixture suite in tests/fixtures/threshold_cases.json keeps both impls
in lockstep until the TS path is removed.

DQOps ships a `rule_type` per check; this module maps a promise (e.g.
"95%", "24h") onto the right rule-type-specific rule_parameters shape.

Rule types (src/dq_platform/checks/rules.py):
    min_value / max_value / min_max_value
    min_count / max_count / min_max_count
    min_percent / max_percent / min_max_percent
    max_change_percent
    equal_to / not_equal_to
    is_true / is_false
    anomaly_percentile
"""

from __future__ import annotations

import re
from typing import Literal

RuleParams = dict[str, dict[str, float]]


# Anchored on the full string so "-5%" doesn't parse as 5.
_PERCENT_RE = re.compile(r"^(-?\d+(?:\.\d+)?)\s*%?$")
_TIME_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(s|sec|second|m|min|minute|h|hr|hour|d|day)s?",
    re.IGNORECASE,
)


def parse_promise_percent(promise: str | None) -> float | None:
    """Parse a percent-shaped promise like '95' or '95%' to a float.

    Returns None for invalid input or values outside [0, 100].
    """
    if promise is None:
        return None
    match = _PERCENT_RE.match(str(promise).strip())
    if not match:
        return None
    try:
        num = float(match.group(1))
    except ValueError:
        return None
    if num < 0 or num > 100:
        return None
    return num


def parse_promise_hours(promise: str | None) -> float | None:
    """Parse a time-shaped promise like '24h', '5m', '30s' to hours.

    Bare numbers are interpreted as hours. Returns None on parse failure.
    """
    if promise is None:
        return None
    s = str(promise).lower().strip()
    match = _TIME_RE.search(s)
    if not match:
        try:
            bare = float(s)
            return bare if bare == bare else None  # NaN guard
        except ValueError:
            return None
    n = float(match.group(1))
    unit = match.group(2).lower()
    if unit.startswith("s"):
        return n / 3600.0
    if unit.startswith("m"):
        return n / 60.0
    if unit.startswith("h"):
        return n
    if unit.startswith("d"):
        return n * 24.0
    return n


def thresholds_from_promise(
    rule_type: str | None,
    promise: str | None,
    *,
    time_field: Literal["max_value", "max_count"] = "max_value",
) -> RuleParams | None:
    """Project a promise onto DQOps rule_parameters for a given rule_type.

    For percent-shaped rule types the promise is interpreted as a pass rate
    and the appropriate direction (min/max) is chosen by rule name. For
    time-shaped rules (data_freshness/staleness etc., which use max_value
    in seconds), the hour parser is used.

    Returns None when the promise can't be meaningfully projected onto
    this rule type — callers should fall back to sensor-baseline thresholds.
    """
    if not rule_type or promise is None:
        return None

    pct = parse_promise_percent(promise)
    hours = parse_promise_hours(promise)

    # ─ Higher-is-better percentage (e.g. not_nulls_percent, date_in_range_percent) ─
    if rule_type == "min_percent":
        if pct is None:
            return None
        return {
            "warning": {"min_percent": min(100.0, pct + 2)},
            "error": {"min_percent": pct},
        }

    # ─ Lower-is-better percentage (e.g. nulls_percent, duplicate_percent) ─
    if rule_type == "max_percent":
        if pct is None:
            return None
        tolerance = max(0.0, 100.0 - pct)
        return {
            "warning": {"max_percent": max(0.0, tolerance - 2)},
            "error": {"max_percent": tolerance},
        }

    # ─ Bounded percentage (e.g. distinct_percent) — symmetric band ─
    if rule_type == "min_max_percent":
        if pct is None:
            return None
        tolerance = max(0.0, 100.0 - pct)
        return {
            "warning": {"min_percent": pct, "max_percent": 100.0},
            "error": {"min_percent": max(0.0, pct - tolerance), "max_percent": 100.0},
        }

    # ─ Strict count cap (e.g. duplicate_count, nulls_count, *_found) ─
    if rule_type == "max_count":
        if pct is None:
            return None
        # Near-perfect promises → zero tolerance. Others → skip (no sensible map).
        if pct >= 99:
            return {"error": {"max_count": 0}}
        return None

    # ─ Strict count floor (e.g. not_nulls_count) ─
    if rule_type == "min_count":
        # Promise on a count isn't meaningful without a scale; let sensor baseline drive.
        return None

    # ─ Bounded count (e.g. distinct_count, row_count) — use sensor baseline ─
    if rule_type == "min_max_count":
        return None

    # ─ Bounded change tolerance (e.g. row_count_change_1_day) ─
    if rule_type == "max_change_percent":
        if pct is None:
            return None
        tolerance = max(0.0, 100.0 - pct)
        return {
            "warning": {"max_change_percent": max(0.0, tolerance - 2)},
            "error": {"max_change_percent": tolerance},
        }

    # ─ Time-based max_value (e.g. data_freshness/staleness, in seconds) ─
    if rule_type == "max_value":
        if hours is None:
            return None
        secs = hours * 3600.0
        return {
            "warning": {time_field: secs},
            "error": {time_field: max(secs * 1.5, secs + 3600.0)},
        }

    # ─ Bounded numeric (e.g. min_in_range, max_in_range) — sensor baseline ─
    if rule_type in ("min_value", "min_max_value"):
        return None

    # ─ Equality-based schema drift checks (e.g. column_list_changed) ─
    if rule_type in ("equal_to", "not_equal_to"):
        return None

    # ─ Boolean checks (e.g. column_exists) ─
    if rule_type in ("is_true", "is_false"):
        return None

    # ─ Anomaly detection — let DQOps defaults run ─
    if rule_type == "anomaly_percentile":
        return None

    return None


_ODPS_DIMS: tuple[str, ...] = (
    "accuracy",
    "completeness",
    "conformity",
    "consistency",
    "coverage",
    "timeliness",
    "validity",
    "uniqueness",
)


def promised_dimensions_from_profile(
    profile: dict[str, object] | None,
) -> list[str]:
    """Return the ODPS dimensions a DQ profile makes a promise on.

    A dim is "promised" when its value is non-null and not a blank string.
    """
    if not profile:
        return []
    out: list[str] = []
    for dim in _ODPS_DIMS:
        v = profile.get(dim)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out.append(dim)
    return out


def promise_for_dimension(
    profile: dict[str, object] | None,
    dimension: str,
) -> str | None:
    """Return the raw promise string for a dim, or None if not promised."""
    if not profile:
        return None
    v = profile.get(dimension)
    if v is None:
        return None
    s = str(v)
    return s if s.strip() else None
