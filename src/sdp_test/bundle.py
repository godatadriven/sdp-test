from __future__ import annotations
import re
from pathlib import Path
from typing import Any

import yaml

PLACEHOLDER_RE = re.compile(r"\$\{([^}]+)\}")


def _deep_merge(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _lookup_path(context: dict[str, Any], path_expr: str) -> Any:
    current: Any = context
    for part in path_expr.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(f"Unknown placeholder path: {path_expr}")
        current = current[part]
    return current


def _resolve_str(text: str, context: dict[str, Any], *, lenient: bool = False) -> Any:
    # If the entire string is a single placeholder, return the raw value
    # (even complex types like dict/list).
    single = PLACEHOLDER_RE.fullmatch(text)
    if single:
        try:
            return _lookup_path(context, single.group(1).strip())
        except KeyError:
            if lenient:
                return text
            raise

    def _replace(match: re.Match[str]) -> str:
        try:
            value = _lookup_path(context, match.group(1).strip())
        except KeyError:
            if lenient:
                return match.group(0)  # keep placeholder as-is
            raise
        if isinstance(value, (dict, list)):
            raise ValueError(f"Cannot interpolate non-scalar value for {match.group(0)}")
        return "" if value is None else str(value)

    return PLACEHOLDER_RE.sub(_replace, text)


def resolve_template(value: Any, context: dict[str, Any], *, lenient: bool = False) -> Any:
    """Resolve ``${...}`` placeholders in *value* using *context*.

    When *lenient* is ``True``, unresolvable placeholders are kept as-is
    instead of raising ``KeyError``.  This is useful when loading resource
    files without a full bundle context.
    """
    if isinstance(value, str):
        return _resolve_str(value, context, lenient=lenient)
    if isinstance(value, list):
        return [resolve_template(item, context, lenient=lenient) for item in value]
    if isinstance(value, dict):
        return {key: resolve_template(item, context, lenient=lenient) for key, item in value.items()}
    return value


def load_bundle_context(
    bundle_file: str,
    target: str | None = None,
    variable_overrides: dict[str, Any] | None = None,
    variable_resolution_depth: int = 5,
) -> dict[str, Any]:
    bundle_path = Path(bundle_file)
    root = bundle_path.parent
    bundle_data = yaml.safe_load(bundle_path.read_text()) or {}

    resources: dict[str, Any] = {}
    include_entries = bundle_data.get("include") or []
    for pattern in include_entries:
        for included_file in sorted(root.glob(pattern)):
            included_data = yaml.safe_load(included_file.read_text()) or {}
            if included_data.get("resources"):
                # Annotate each pipeline with the directory of its source file
                # so that relative library paths can be resolved correctly.
                for pipeline_def in (included_data["resources"].get("pipelines") or {}).values():
                    pipeline_def["__pipeline_spec_dir"] = str(included_file.parent)
                _deep_merge(resources, included_data["resources"])

    variables: dict[str, Any] = {}
    for name, value in (bundle_data.get("variables") or {}).items():
        if isinstance(value, dict):
            variables[name] = value.get("default")
        else:
            variables[name] = value

    if variable_overrides:
        variables.update(variable_overrides)

    selected_target = target or "local"

    raw_context: dict[str, Any] = {
        "bundle": {
            "name": bundle_data.get("bundle", {}).get("name", "bundle"),
            "uuid": bundle_data.get("bundle", {}).get("uuid"),
            "target": selected_target,
        },
        "var": variables,
        "resources": resources,
        "workspace": {
            "file_path": str(root),
        },
    }

    # Resolve nested ${...} references in the context itself.
    # Use lenient mode because some placeholders (e.g. ${resources.pipelines.*.id})
    # are only available at deploy time and cannot be resolved locally.
    # Iterate to resolve transitive references (e.g. ${var.x} -> ${resources.y.z}).
    resolved_context = raw_context
    for _ in range(variable_resolution_depth):
        next_context = resolve_template(resolved_context, resolved_context, lenient=True)
        if next_context == resolved_context:
            break
        resolved_context = next_context
    return resolved_context


def load_pipeline_test_spec(spec_file: str) -> dict[str, Any]:
    return yaml.safe_load(Path(spec_file).read_text()) or {}
