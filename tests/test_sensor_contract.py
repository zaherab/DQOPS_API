"""Meta-test: every Sensor's Jinja variables are guarded.

This is the long-lived regression guard for the class of bug that produced
phantom failing checks in MLG. Rather than listing sensors individually
(see test_sensor_required_params.py for that), this test AST-walks the
sensor catalog and asserts the invariant for every Sensor(...) it finds.

If a future contributor adds a new sensor whose template references a
Jinja variable that isn't auto-supplied, defaulted, or required, this
test fails loudly at CI time instead of the check silently emitting
broken SQL at execution time.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

SENSORS_DIR = Path(__file__).resolve().parent.parent / "src" / "dq_platform" / "checks" / "sensors"

# Identifiers the framework auto-quotes in Sensor.render (see _base.py).
# Two classes:
#   - identifier slots: populated by the caller's check target info
#   - partition_filter: validated separately by render()
AUTO_IDENTIFIERS = frozenset(
    {
        "schema_name",
        "table_name",
        "column_name",
        "reference_schema",
        "reference_table",
        "reference_column",
        "partition_filter",
        # raw_* — un-quoted name copies render() auto-derives for catalog
        # sensors that need the bare name as a string literal.
        "raw_schema_name",
        "raw_table_name",
        "raw_column_name",
    }
)

# Jinja control-flow keywords / filter names that show up in templates but
# are NOT parameters. Anything matched here gets ignored.
JINJA_CONTROL = frozenset(
    {"default", "if", "else", "elif", "endif", "for", "endfor", "in", "not", "and", "or", "none", "true", "false"}
)

# Match a Jinja var reference like `{{ foo }}` or `{{ foo|bar }}` — we
# capture the first identifier in the expression. Also `{% if foo %}`.
_VAR_RE = re.compile(r"{{-?\s*([a-zA-Z_][a-zA-Z0-9_]*)")
_IF_RE = re.compile(r"{%-?\s*if\s+([a-zA-Z_][a-zA-Z0-9_]*)")
# `{% for X in Y %}` — X is a loop-local variable (not a sensor param);
# Y must still be guarded. Capture the loop target so it can be excluded.
_FOR_RE = re.compile(r"{%-?\s*for\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+in\s")


def _literal(node: ast.AST):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Dict):
        out = {}
        for k, v in zip(node.keys, node.values):
            kv = _literal(k)
            if kv is not None:
                out[kv] = _literal(v)
        return out
    if isinstance(node, ast.List):
        return [_literal(e) for e in node.elts]
    return None


def _iter_sensors():
    """Yield (source_file, sensor_var_name, kwargs_dict) for every Sensor(...)
    definition in the sensors package (excluding _base.py)."""
    for py in sorted(SENSORS_DIR.glob("_*.py")):
        if py.name == "_base.py":
            continue
        tree = ast.parse(py.read_text())
        for stmt in tree.body:
            if not isinstance(stmt, ast.Assign):
                continue
            call = stmt.value
            if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "Sensor"):
                continue
            var_name = stmt.targets[0].id if isinstance(stmt.targets[0], ast.Name) else "?"
            kw = {k.arg: _literal(k.value) for k in call.keywords}
            yield py.name, var_name, kw


def _collect_params() -> list[tuple[str, str, list[str]]]:
    """Return [(file, sensor_var, unsatisfied_vars)] for every sensor whose
    template references a Jinja var with no guard. Empty list means all clean.
    """
    problems: list[tuple[str, str, list[str]]] = []
    for fname, var, kw in _iter_sensors():
        template = kw.get("template") or ""
        is_column_level = kw.get("is_column_level")
        required = set(kw.get("required_params") or [])
        defaults_raw = kw.get("default_params")
        defaults = set(defaults_raw.keys()) if isinstance(defaults_raw, dict) else set()

        supplied = set(AUTO_IDENTIFIERS)
        # Table-level sensors don't have column_name unless they say so
        # explicitly via default_params or required_params.
        if is_column_level is False:
            supplied.discard("column_name")

        refs = set(_VAR_RE.findall(template)) | set(_IF_RE.findall(template))
        refs -= JINJA_CONTROL
        # `{% for X in ... %}` loop targets are template-local, not params.
        loop_vars = set(_FOR_RE.findall(template))

        unsatisfied = sorted(refs - supplied - defaults - required - loop_vars)
        if unsatisfied:
            problems.append((fname, var, unsatisfied))
    return problems


def test_every_sensor_template_is_guarded():
    """Contract: every Jinja variable in a sensor template must be either
    auto-supplied by the framework, listed in default_params, or listed in
    required_params. Otherwise rendering emits invalid SQL for any caller
    who doesn't happen to pass that parameter."""
    problems = _collect_params()
    if problems:
        msg = ["Sensor(s) reference un-guarded Jinja variables:"]
        for fname, var, unsat in problems:
            msg.append(f"  [{fname}] {var}: unguarded → {unsat}")
        msg.append(
            "\nAdd the variable to default_params (with a sensible default) "
            "or to required_params (so render() rejects callers that omit it)."
        )
        pytest.fail("\n".join(msg))


def test_sensor_catalogue_is_nonempty():
    """Defensive guard — if the AST walker silently finds zero sensors,
    the previous test would trivially pass even when the catalog is broken.
    """
    count = sum(1 for _ in _iter_sensors())
    assert count > 100, f"Expected >100 sensors, got {count} — is the catalog intact?"
