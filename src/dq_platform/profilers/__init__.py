"""Deterministic DQ check engine — profile-driven check emission.

This package replaces the AI-driven check recommendation path. Given a
content schema, a DQ profile (promised dim levels), and a table profile
of aggregate statistics, the emitter produces a deterministic set of
DQOps check specs.

Modules:
    threshold_engine  Promise % → DQOps rule_parameters projection.
    inference_engine  Detect regex/format/codelist/enum/range from sample.
    check_emitter     Combine declarations + inferences + profile into
                      a deterministic check set.

profile_runner / sample_fetcher / sample_cache live alongside but talk
to customer DBs and are introduced in a separate phase.
"""

from dq_platform.profilers.threshold_engine import (
    parse_promise_hours,
    parse_promise_percent,
    promise_for_dimension,
    promised_dimensions_from_profile,
    thresholds_from_promise,
)

__all__ = [
    "parse_promise_hours",
    "parse_promise_percent",
    "promise_for_dimension",
    "promised_dimensions_from_profile",
    "thresholds_from_promise",
]
