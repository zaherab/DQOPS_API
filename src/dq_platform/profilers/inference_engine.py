"""Pure inference functions over a sample + column profile.

No DB access. No side effects. Same input → same output.

Each `infer_*` returns either a concrete declaration candidate
(`RegexCandidate`, `CodelistRef`, `EnumCandidate`, `FormatRef`,
`LengthRange`, `NumericRange`) or None when the data doesn't support
the inference at the required confidence.

Confidence thresholds and bundled codelist data are loaded once at
import time and never sent in customer-side queries.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ─── Bundled codelists ───────────────────────────────────────────────────────
# Loaded lazily on first use. Codelist contents never appear in customer
# SQL — matching happens locally on the dq-platform server.

_DATA_DIR = Path(__file__).resolve().parent / "data"


def _load_codelist(filename: str) -> dict[str, Any]:
    with (_DATA_DIR / filename).open() as f:
        data: dict[str, Any] = json.load(f)
    data["set"] = frozenset(data["codes"])
    return data


_iso3166_alpha2 = _load_codelist("iso3166_alpha2.json")
_iso4217 = _load_codelist("iso4217.json")


# ─── Result types ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RegexCandidate:
    pattern: str
    coverage: float  # fraction of sample matching


@dataclass(frozen=True)
class CodelistRef:
    standard: str  # e.g. "ISO_3166_alpha2"
    version: str
    coverage: float


@dataclass(frozen=True)
class EnumCandidate:
    values: tuple[Any, ...]
    coverage: float


@dataclass(frozen=True)
class FormatRef:
    format: str  # email | uuid | iso_date | iso_datetime | e164 | url
    coverage: float


@dataclass(frozen=True)
class LengthRange:
    min: int
    max: int


@dataclass(frozen=True)
class NumericRange:
    min: float
    max: float


@dataclass(frozen=True)
class DateRange:
    min: str  # ISO date 'YYYY-MM-DD'
    max: str  # ISO date 'YYYY-MM-DD'


@dataclass(frozen=True)
class ColumnProfileLite:
    """Subset of profile aggregates inference reads.

    The full TableProfile lives in profile_runner; this lite form keeps
    inference_engine importable without pulling the connector layer.
    """

    count: int = 0
    nulls: int = 0
    distinct: int = 0
    min: Any = None
    max: Any = None
    min_len: int | None = None
    max_len: int | None = None


# ─── Format regexes (anchored, conservative) ─────────────────────────────────
# Coverage threshold is 95% by default. Below that the candidate is rejected
# to avoid over-fitting on noisy samples.

_FORMAT_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
    "uuid": re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"),
    "iso_date": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    "iso_datetime": re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"),
    "e164": re.compile(r"^\+[1-9]\d{6,14}$"),
    "url": re.compile(r"^https?://[A-Za-z0-9.-]+(?::\d+)?(?:/.*)?$"),
}


DEFAULT_COVERAGE = 0.95

# Logical types treated as date-shaped — date_range inference applies.
_DATE_TYPES = {"date", "datetime", "timestamp", "timestamptz"}


# ─── Inference functions ─────────────────────────────────────────────────────


def _str_sample(sample: list[Any]) -> list[str]:
    """Coerce non-null values to str; preserves order."""
    return [str(v) for v in sample if v is not None]


def infer_format(
    sample: list[Any],
    logical_type: str | None = None,
    coverage: float = DEFAULT_COVERAGE,
) -> FormatRef | None:
    """Match a well-known format (email/uuid/iso_date/...) against a sample.

    Returns the format with the highest coverage that clears the threshold,
    or None. Strongly-typed numeric/boolean cols are skipped — formats only
    apply to text-shaped data.
    """
    if logical_type in ("number", "integer", "float", "boolean"):
        return None
    values = _str_sample(sample)
    if not values:
        return None
    best: tuple[str, float] | None = None
    for name, pat in _FORMAT_PATTERNS.items():
        hits = sum(1 for v in values if pat.match(v))
        cov = hits / len(values)
        if cov >= coverage and (best is None or cov > best[1]):
            best = (name, cov)
    if best is None:
        return None
    return FormatRef(format=best[0], coverage=best[1])


def infer_codelist(
    sample: list[Any],
    logical_type: str | None = None,
    coverage: float = DEFAULT_COVERAGE,
) -> CodelistRef | None:
    """Match a sample against bundled ISO codelists (3166 alpha-2, 4217).

    Match is case-insensitive on uppercase candidates only. Numeric or
    bool logical types skip codelist matching.
    """
    if logical_type in ("number", "integer", "float", "boolean"):
        return None
    values = [v.upper() for v in _str_sample(sample) if isinstance(v, str)]
    if not values:
        return None

    candidates = [
        ("ISO_3166_alpha2", _iso3166_alpha2),
        ("ISO_4217", _iso4217),
    ]
    best: tuple[str, str, float] | None = None
    for standard, data in candidates:
        codes: frozenset[str] = data["set"]
        # Require the values themselves to look like codes (right length).
        # Without this, every short uppercase string would match accidentally.
        expected_len = next(iter(codes)).__len__() if codes else 0
        eligible = [v for v in values if len(v) == expected_len]
        if not eligible:
            continue
        hits = sum(1 for v in eligible if v in codes)
        cov = hits / len(eligible)
        if cov >= coverage and (best is None or cov > best[2]):
            best = (standard, data["version"], cov)
    if best is None:
        return None
    return CodelistRef(standard=best[0], version=best[1], coverage=best[2])


def infer_enum(
    sample: list[Any],
    distinct_threshold: int = 20,
    min_repeat_ratio: float = 1.5,
) -> EnumCandidate | None:
    """Detect a closed-enum column from a sample.

    Two gates:
      1. distinct count ≤ distinct_threshold (small finite domain).
      2. values REPEAT — a real enum has fewer distinct values than rows.
         `len(sample) >= len(distinct) * min_repeat_ratio` means each
         value shows up ~1.5×+ on average. Without this, an ID column
         sampled at any size (every value unique) would be mislabeled an
         enum on small samples.

    Gate 2 is what stops `metric_id` (MTR-001, MTR-002, ...) from being
    treated as a 5-value enum.
    """
    non_null = [v for v in sample if v is not None]
    if not non_null:
        return None
    distinct = list(dict.fromkeys(non_null))  # preserves first-seen order
    if len(distinct) > distinct_threshold:
        return None
    # Repetition gate: an enum's values recur. An all-distinct column is
    # an identifier, not an enum.
    if len(non_null) < len(distinct) * min_repeat_ratio:
        return None
    return EnumCandidate(values=tuple(distinct), coverage=1.0)


def infer_regex(
    sample: list[Any],
    coverage: float = DEFAULT_COVERAGE,
    max_pattern_len: int = 128,
) -> RegexCandidate | None:
    """Synthesize a conservative regex from observed char classes.

    Walks each position across the sample. If every value has the same
    length and the char class at each position is stable, emits a regex.
    Otherwise returns None (over-fitting risk).

    Char classes used:
        \\d   when every position-char is a digit
        [A-Z] when every position-char is uppercase letter
        [a-z] when every position-char is lowercase letter
        [A-Za-z] when mixed-case letters only
        \\w   when alphanumeric (any case + digits)
        literal when same exact char everywhere
        no candidate when char class varies (e.g. digit + letter)
    """
    values = _str_sample(sample)
    if not values:
        return None
    lengths = {len(v) for v in values}
    if len(lengths) != 1:
        # Variable-length data — try length-tolerant patterns instead.
        return _infer_regex_variable_length(values, coverage, max_pattern_len)
    n = lengths.pop()
    if n == 0 or n > max_pattern_len:
        return None

    parts: list[str] = []
    for i in range(n):
        chars = {v[i] for v in values}
        cls = _char_class(chars)
        if cls is None:
            return None
        parts.append(cls)
    pattern = "^" + _collapse_runs(parts) + "$"
    return RegexCandidate(pattern=pattern, coverage=1.0)


def _char_class(chars: set[str]) -> str | None:
    if len(chars) == 1:
        c = next(iter(chars))
        # Escape regex metacharacters when emitting as literal.
        if c in r".^$*+?()[]{}|\\":
            return "\\" + c
        return re.escape(c) if not c.isalnum() else c
    if all(c.isdigit() for c in chars):
        return r"\d"
    if all(c.isupper() and c.isalpha() for c in chars):
        return "[A-Z]"
    if all(c.islower() and c.isalpha() for c in chars):
        return "[a-z]"
    if all(c.isalpha() for c in chars):
        return "[A-Za-z]"
    if all(c.isalnum() for c in chars):
        return r"\w"
    return None


def _collapse_runs(parts: list[str]) -> str:
    """Collapse adjacent equal char-class atoms with `{n}` quantifiers."""
    if not parts:
        return ""
    out: list[str] = []
    i = 0
    while i < len(parts):
        j = i
        while j < len(parts) and parts[j] == parts[i]:
            j += 1
        run = j - i
        atom = parts[i]
        if run == 1:
            out.append(atom)
        else:
            out.append(f"{atom}{{{run}}}")
        i = j
    return "".join(out)


def _infer_regex_variable_length(
    values: list[str],
    coverage: float,
    max_pattern_len: int,
) -> RegexCandidate | None:
    """Fallback for variable-length text: detect common prefix + variable tail.

    Looks for a stable prefix shared by ≥ coverage of the sample, optionally
    followed by a class-stable tail. Useful for identifiers like `TXN-1234`
    where the prefix is stable but trailing digits vary in length.
    """
    if not values:
        return None
    # Common prefix
    prefix = values[0]
    for v in values[1:]:
        while not v.startswith(prefix):
            prefix = prefix[:-1]
            if not prefix:
                return None
    if len(prefix) < 1:
        return None
    # If the greedy common prefix leaves an empty tail on some value
    # (e.g. prefix "ORD-1" for ["ORD-1","ORD-12"]), shrink it until every
    # value has a non-empty tail. "ORD-1" → "ORD-" → tails 1/12/123.
    while prefix and any(v[len(prefix) :] == "" for v in values):
        prefix = prefix[:-1]
    if not prefix:
        return None
    # Tail char class — must be uniform across all values
    tails = [v[len(prefix) :] for v in values]
    tail_chars = {c for t in tails for c in t}
    cls = _char_class(tail_chars)
    if cls is None:
        return None
    if len(prefix) + 6 > max_pattern_len:
        return None
    tail_lens = {len(t) for t in tails}
    min_len = min(tail_lens)
    max_len = max(tail_lens)
    if min_len == max_len:
        quantifier = f"{{{min_len}}}"
    else:
        quantifier = f"{{{min_len},{max_len}}}"
    pattern = f"^{re.escape(prefix)}{cls}{quantifier}$"
    return RegexCandidate(pattern=pattern, coverage=1.0)


def infer_length_range(
    profile: ColumnProfileLite,
    padding: float = 0.05,
) -> LengthRange | None:
    """Derive a text-length range from min_len / max_len aggregates.

    Padding widens the range by ±5% by default to absorb minor drift.
    """
    if profile.min_len is None or profile.max_len is None:
        return None
    lo = max(0, int(profile.min_len * (1 - padding)))
    hi = int(profile.max_len * (1 + padding) + 0.5)
    if hi < profile.max_len:
        hi = profile.max_len
    return LengthRange(min=lo, max=hi)


def infer_numeric_range(
    profile: ColumnProfileLite,
    min_distinct: int = 5,
) -> NumericRange | None:
    """Derive a numeric range from min / max aggregates.

    Rejects ranges that are too narrow (fewer than `min_distinct` distinct
    values observed) — those are more likely enums than ranges, and the
    caller should pick that path instead.
    """
    if profile.min is None or profile.max is None:
        return None
    try:
        lo = float(profile.min)
        hi = float(profile.max)
    except (TypeError, ValueError):
        return None
    if profile.distinct and profile.distinct < min_distinct:
        return None
    if hi < lo:
        return None
    return NumericRange(min=lo, max=hi)


def _coerce_date_str(v: Any) -> str | None:
    """Coerce a profiled min/max value to an ISO 'YYYY-MM-DD' string.

    Handles `date` / `datetime` objects (which expose `isoformat()`) and
    strings that begin with a date. Returns None for anything that does not
    look like a date — a numeric min/max from a mistyped column is rejected.
    """
    if v is None:
        return None
    iso = getattr(v, "isoformat", None)
    if callable(iso):
        return str(iso())[:10]
    s = str(v).strip()
    match = re.match(r"\d{4}-\d{2}-\d{2}", s)
    return match.group(0) if match else None


def infer_date_range(
    profile: ColumnProfileLite,
    logical_type: str | None = None,
) -> DateRange | None:
    """Derive a date range from min / max aggregates for a date-shaped column.

    The observed min/max are used verbatim (no padding) — mirroring
    infer_numeric_range. Returns None for non-date logical types or when the
    aggregates do not parse as dates.
    """
    if logical_type is not None and logical_type.lower() not in _DATE_TYPES:
        return None
    lo = _coerce_date_str(profile.min)
    hi = _coerce_date_str(profile.max)
    if lo is None or hi is None:
        return None
    if hi < lo:
        return None
    return DateRange(min=lo, max=hi)


# ─── Public registry of inference functions ──────────────────────────────────


@dataclass
class InferenceResult:
    """Bundled output of all inferers for one column."""

    format: FormatRef | None = None
    codelist: CodelistRef | None = None
    enum: EnumCandidate | None = None
    regex: RegexCandidate | None = None
    length_range: LengthRange | None = None
    numeric_range: NumericRange | None = None
    date_range: DateRange | None = None


def infer_all(
    sample: list[Any],
    profile: ColumnProfileLite,
    logical_type: str | None = None,
) -> InferenceResult:
    """Run every inferer over one column's sample + profile.

    Order matters only for the rules-table consumer (check_emitter), which
    picks the strongest declaration in priority order. This function
    returns ALL inferences and leaves prioritization to the caller.
    """
    return InferenceResult(
        format=infer_format(sample, logical_type),
        codelist=infer_codelist(sample, logical_type),
        enum=infer_enum(sample),
        regex=infer_regex(sample) if logical_type not in ("number", "integer", "float", "boolean") else None,
        length_range=infer_length_range(profile),
        numeric_range=infer_numeric_range(profile) if logical_type in ("number", "integer", "float") else None,
        date_range=infer_date_range(profile, logical_type),
    )
