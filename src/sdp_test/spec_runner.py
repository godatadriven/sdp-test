from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import importlib.util
import re
from pathlib import Path
import sys
import types
from typing import Any
import uuid

import yaml
from pyspark.sql.types import DateType, DecimalType, TimestampType

from .bundle import load_bundle_context, load_pipeline_test_spec, resolve_template
from .model_sql import register_df_as_view, render_model_query
from .spec_models import PipelineEntrySpec, UnitSpec

UNRESOLVED_WITH_ALIAS_RE = re.compile(
    r"with name [`'](?P<alias>[A-Za-z_]\w*)[`']\.[`'](?P<column>[A-Za-z_]\w*)[`']",
    flags=re.IGNORECASE,
)
UNRESOLVED_NO_ALIAS_RE = re.compile(
    r"with name [`'](?P<column>[A-Za-z_]\w*)[`'] cannot be resolved",
    flags=re.IGNORECASE,
)
LOG_LEVEL_ORDER = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "ERROR": 40,
    "NONE": 50,
}


@dataclass
class CaseResult:
    left_minus_right: int
    right_minus_left: int
    actual_rows: list[dict[str, Any]]
    expected_rows: list[dict[str, Any]]


def find_spec_files(search_dir: Path | None = None) -> list[Path]:
    if search_dir is None:
        search_dir = Path.cwd()
    files = set()
    files.update(search_dir.rglob("*_pipeline_tests.yml"))
    files.update(search_dir.rglob("*_pipeline_tests.yaml"))
    return sorted(files)


def cases_from_spec(
    spec_path: Path,
    default_bundle_file: Path | None = None,
) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    """Process a single *_pipeline_tests.yml spec and return all test cases."""
    pipeline_spec = PipelineEntrySpec.model_validate(load_pipeline_test_spec(str(spec_path)))
    pipeline_spec_data = pipeline_spec.model_dump(exclude_none=True)
    bundle_cfg = pipeline_spec_data.get("bundle") or {}
    bundle_file = bundle_cfg.get("file") or (str(default_bundle_file) if default_bundle_file else "databricks.yml")
    bundle_path = (
        (spec_path.parent / bundle_file).resolve()
        if not Path(bundle_file).is_absolute()
        else Path(bundle_file)
    )
    bundle_target = bundle_cfg.get("target")
    bundle_vars = bundle_cfg.get("variables") or {}
    if bundle_path.exists():
        context = load_bundle_context(str(bundle_path), target=bundle_target, variable_overrides=bundle_vars)
    else:
        context: dict[str, Any] = {
            "bundle": {"name": "default", "uuid": None, "target": "local"},
            "var": bundle_vars,
            "resources": {},
            "workspace": {"file_path": str(spec_path.parent.parent)},
        }
    pipeline_defaults, pipeline_def = _load_pipeline_defaults(spec_path, pipeline_spec_data, context)
    defaults = {**pipeline_defaults, **resolve_template(pipeline_spec_data.get("defaults") or {}, context)}

    cases: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    pipeline_log_level = _normalize_log_level(pipeline_spec_data.get("log_level"))
    for test in pipeline_spec_data.get("tests") or []:
        merged = {**defaults, **test}
        merged_context = {**context, **defaults}
        case = resolve_template(merged, merged_context)
        case["__log_level"] = _normalize_log_level(case.get("log_level"), pipeline_log_level)
        case["__spec_dir"] = str(spec_path.parent)
        cases.append((spec_path, case, context))

    for unit_spec_file in _discover_unit_spec_files(pipeline_def, context):
        unit_spec = UnitSpec.model_validate(load_pipeline_test_spec(str(unit_spec_file)))
        unit_spec_data = unit_spec.model_dump(exclude_none=True)
        unit_log_level = _normalize_log_level(unit_spec_data.get("log_level"), pipeline_log_level)
        for test in unit_spec_data.get("tests") or []:
            merged = {**defaults, **test}
            merged_context = {**context, **defaults}
            case = resolve_template(merged, merged_context)
            case["__log_level"] = _normalize_log_level(case.get("log_level"), unit_log_level)
            case["__spec_dir"] = str(unit_spec_file.parent)
            cases.append((unit_spec_file, case, context))
    return cases


def cases_from_pipeline_def(
    pipeline_def: dict[str, Any],
    context: dict[str, Any],
    source_path: Path,
) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    """Discover and return test cases directly from a pipeline definition.

    Works for both Databricks bundle pipeline defs and open source spark-pipeline.yml.
    No ``*_pipeline_tests.yml`` spec file needed.
    """
    defaults = _extract_pipeline_defaults(pipeline_def)
    cases: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []

    for unit_spec_file in _discover_unit_spec_files(pipeline_def, context):
        unit_spec = UnitSpec.model_validate(load_pipeline_test_spec(str(unit_spec_file)))
        unit_spec_data = unit_spec.model_dump(exclude_none=True)
        unit_log_level = _normalize_log_level(unit_spec_data.get("log_level"), "ERROR")
        for test in unit_spec_data.get("tests") or []:
            merged = {**defaults, **test}
            merged_context = {**context, **defaults}
            case = resolve_template(merged, merged_context)
            case["__log_level"] = _normalize_log_level(case.get("log_level"), unit_log_level)
            case["__spec_dir"] = str(unit_spec_file.parent)
            cases.append((unit_spec_file, case, context))
    return cases


def cases_from_bundle(bundle_path: Path) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    """Load a ``databricks.yml`` and return test cases for all pipelines."""
    context = load_bundle_context(str(bundle_path))
    pipelines = (context.get("resources") or {}).get("pipelines") or {}
    cases: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    for pipeline_def in pipelines.values():
        cases.extend(cases_from_pipeline_def(pipeline_def, context, bundle_path))
    return cases


def cases_from_pipeline_file(pipeline_path: Path) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    """Load a ``spark-pipeline.yml`` and return test cases."""
    pipeline_data = yaml.safe_load(pipeline_path.read_text()) or {}
    pipeline_data["__pipeline_spec_dir"] = str(pipeline_path.parent)
    context: dict[str, Any] = {
        "bundle": {"name": "default", "uuid": None, "target": "local"},
        "var": {},
        "resources": {},
        "workspace": {"file_path": str(pipeline_path.parent)},
    }
    pipeline_data = resolve_template(pipeline_data, context)
    return cases_from_pipeline_def(pipeline_data, context, pipeline_path)


def all_cases(
    search_dir: Path | None = None,
    default_bundle_file: Path | None = None,
) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    if search_dir is None:
        search_dir = Path.cwd()
    cases: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    for spec_file in find_spec_files(search_dir):
        cases.extend(cases_from_spec(spec_file, default_bundle_file))
    return cases


def case_id(spec_file: Path, case: dict[str, Any]) -> str:
    suite = case.get("__suite") or case.get("pipeline_name") or spec_file.stem
    return f"{suite}::{case.get('name', 'unnamed')}"


def run_case(spark, case: dict[str, Any]) -> CaseResult:
    schema_names = set()
    for key in ("bronze_schema", "silver_schema", "gold_schema"):
        if key in case:
            schema_names.add(case[key])
    _clear_schemas(spark, schema_names)

    test_name = case.get("name", "unnamed")
    log_level = _normalize_log_level(case.get("__log_level"), "ERROR")
    given = case.get("given") or []
    expect_rows = (case.get("expect") or {}).get("rows") or []
    registered_tables: set[str] = set()

    for input_spec in given:
        table = input_spec["table"]
        rows = input_spec.get("rows") or []
        if "." not in table:
            raise ValueError(f"Given input table must be schema-qualified: {table}")
        schema_name, table_name = table.split(".", 1)
        df = spark.createDataFrame(rows)
        register_df_as_view(spark, df, schema_name, table_name)
        registered_tables.add(f"{schema_name}.{table_name}")

    model_path = _model_path(case)
    if model_path.suffix.lower() == ".py":
        _set_model_runtime_conf(spark, case)
        result_df = _run_python_model(model_path, case)
    else:
        query = render_model_query(str(model_path), _schema_map_from_case(case))
        result_df = _run_query_with_auto_missing_columns(spark, query, registered_tables)

    if not expect_rows:
        actual_count = result_df.count()
        if _should_log(log_level, "INFO"):
            print(f"[INFO] {test_name}: actual_rows={actual_count}, expected_rows=0")
        return CaseResult(
            left_minus_right=actual_count,
            right_minus_left=0,
            actual_rows=_rows_for_log(result_df, result_df.columns),
            expected_rows=[],
        )

    expected_columns = list(expect_rows[0].keys())
    actual_subset = result_df.select(*expected_columns)
    expected_df = spark.createDataFrame(
        _coerce_expected_rows(expect_rows, actual_subset.schema),
        schema=actual_subset.schema,
    )

    left_minus_right = actual_subset.exceptAll(expected_df).count()
    right_minus_left = expected_df.exceptAll(actual_subset).count()
    actual_rows = _rows_for_log(actual_subset, expected_columns)
    expected_rows_log = _rows_for_log(expected_df, expected_columns)
    if _should_log(log_level, "INFO"):
        print(f"[INFO] {test_name}: unexpected_rows={left_minus_right}, missing_rows={right_minus_left}")
    if _should_log(log_level, "DEBUG"):
        print(f"[DEBUG] {test_name}: actual_rows={actual_rows}")
        print(f"[DEBUG] {test_name}: expected_rows={expected_rows_log}")
    return CaseResult(
        left_minus_right=left_minus_right,
        right_minus_left=right_minus_left,
        actual_rows=actual_rows,
        expected_rows=expected_rows_log,
    )


def _load_pipeline_defaults(
    spec_file: Path, spec: dict[str, Any], context: dict[str, Any]
) -> tuple[dict[str, str], dict[str, Any]]:
    pipeline_cfg = spec.get("pipeline")
    if not pipeline_cfg:
        raise ValueError("Spec must include a 'pipeline' section")

    resources = context.get("resources") or {}
    pipelines = resources.get("pipelines") or {}

    pipeline_key: str | None = None
    pipeline_data_origin = "bundle resources"

    if isinstance(pipeline_cfg, str):
        if not pipeline_cfg.startswith("pipelines."):
            raise ValueError("String pipeline reference must be like 'pipelines.<key>'")
        pipeline_key = pipeline_cfg.split(".", 1)[1]
    elif isinstance(pipeline_cfg, dict):
        ref_value = pipeline_cfg.get("ref")
        if ref_value:
            if not isinstance(ref_value, str) or not ref_value.startswith("pipelines."):
                raise ValueError("Spec 'pipeline.ref' must be like 'pipelines.<key>'")
            pipeline_key = ref_value.split(".", 1)[1]
        else:
            pipeline_file = pipeline_cfg.get("file")
            key = pipeline_cfg.get("key")
            if not pipeline_file:
                raise ValueError("Spec 'pipeline' requires either a string/ref form, file+key, or file (open source)")
            pipeline_path = (
                (spec_file.parent / pipeline_file).resolve()
                if not Path(pipeline_file).is_absolute()
                else Path(pipeline_file)
            )
            pipeline_data = yaml.safe_load(pipeline_path.read_text()) or {}
            pipeline_data = resolve_template(pipeline_data, context)

            if key:
                # Databricks bundle resource format: resources.pipelines.<key>
                pipelines = (pipeline_data.get("resources") or {}).get("pipelines") or {}
                pipeline_key = key
                pipeline_data_origin = str(pipeline_path)
            else:
                # Open source spark-pipeline.yml format: pipeline definition at top level.
                pipeline_def = pipeline_data
                pipeline_def["__pipeline_spec_dir"] = str(pipeline_path.parent)
                return _extract_pipeline_defaults(pipeline_def), pipeline_def
    else:
        raise ValueError("Spec 'pipeline' must be a string or object")

    if not pipeline_key:
        raise ValueError("Could not determine pipeline key from spec")
    if pipeline_key not in pipelines:
        raise ValueError(f"Could not find pipeline key '{pipeline_key}' in {pipeline_data_origin}")

    pipeline_def = pipelines[pipeline_key]
    return _extract_pipeline_defaults(pipeline_def), pipeline_def


def _extract_pipeline_defaults(pipeline_def: dict[str, Any]) -> dict[str, str]:
    """Extract default substitutions from a pipeline definition.

    Supports both Databricks bundle format and open source spark-pipeline.yml format.
    In open source format, ``database`` is used as an alias for ``schema``.
    """
    pipeline_configuration = pipeline_def.get("configuration") or {}
    schema = pipeline_def.get("schema") or pipeline_def.get("database")

    return {
        **pipeline_configuration,
        "catalog": pipeline_def.get("catalog"),
        "pipeline_schema": schema,
        "pipeline_name": pipeline_def.get("name"),
    }


def _discover_unit_spec_files(pipeline_def: dict[str, Any], context: dict[str, Any]) -> list[Path]:
    # Prefer bundle library globs as the single source of truth for pipeline code locations.
    library_roots: set[Path] = set()
    unit_files: set[Path] = set()

    # For open source spark-pipeline.yml, library paths are relative to the spec file.
    # For Databricks bundles, library paths are relative to the resources/ directory.
    pipeline_spec_dir = pipeline_def.get("__pipeline_spec_dir")
    if pipeline_spec_dir:
        base_dir = Path(pipeline_spec_dir)
    else:
        base_dir = Path(str(context["workspace"]["file_path"])) / "resources"

    libraries = pipeline_def.get("libraries") or []

    for library in libraries:
        file_entry = (library or {}).get("file")
        if file_entry:
            resolved_file = resolve_template(file_entry, context)
            candidate_file = (base_dir / str(resolved_file)).resolve()
            if candidate_file.is_dir():
                library_roots.add(candidate_file)
            elif candidate_file.exists():
                if candidate_file.suffix in {".yml", ".yaml"} and (
                    candidate_file.name.endswith(".unit_tests.yml") or candidate_file.name.endswith(".unit_tests.yaml")
                ):
                    unit_files.add(candidate_file)
                else:
                    sibling_yml = candidate_file.with_suffix(".unit_tests.yml")
                    sibling_yaml = candidate_file.with_suffix(".unit_tests.yaml")
                    if sibling_yml.exists():
                        unit_files.add(sibling_yml)
                    if sibling_yaml.exists():
                        unit_files.add(sibling_yaml)

        # Open source format: libraries can be a simple string path (e.g. "- transformations/**")
        if isinstance(library, str):
            resolved_lib = resolve_template(library, context)
            base_pattern = str(resolved_lib).split("**", 1)[0].rstrip("/")
            if base_pattern:
                candidate = (base_dir / base_pattern).resolve()
                if candidate.exists():
                    library_roots.add(candidate)
            continue

        glob_cfg = (library or {}).get("glob") or {}
        include_pattern = glob_cfg.get("include")
        if not include_pattern:
            continue
        resolved_include = resolve_template(include_pattern, context)
        base_pattern = str(resolved_include).split("**", 1)[0].rstrip("/")
        if not base_pattern:
            continue
        candidate = (base_dir / base_pattern).resolve()
        if candidate.exists():
            library_roots.add(candidate)

    # Fallback for pipelines that do not define libraries globs and no file-based unit specs were found.
    if not library_roots and not unit_files:
        root_path = pipeline_def.get("root_path")
        if not root_path:
            raise ValueError("Pipeline definition must include libraries.glob.include or root_path")
        resolved_root = resolve_template(root_path, context) if isinstance(root_path, str) else root_path
        root_dir = Path(str(resolved_root))
        if not root_dir.exists():
            raise FileNotFoundError(f"Pipeline root_path does not exist: {root_dir}")
        library_roots.add(root_dir)

    files = set()
    for root_dir in library_roots:
        files.update(root_dir.glob("**/*.unit_tests.yml"))
        files.update(root_dir.glob("**/*.unit_tests.yaml"))
    files.update(unit_files)
    return sorted(files)


def _extract_table_alias_map(query: str) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+([`A-Za-z0-9_\.]+)\s+(?:AS\s+)?([`A-Za-z0-9_]+)",
        query,
        flags=re.IGNORECASE,
    ):
        table_name = match.group(1).replace("`", "")
        alias = match.group(2).replace("`", "")
        if table_name.startswith("("):
            continue
        parts = table_name.split(".")
        if len(parts) >= 2:
            table_name = f"{parts[-2]}.{parts[-1]}"
        alias_map[alias] = table_name
    return alias_map


def _parse_unresolved_column(error_text: str) -> tuple[str | None, str | None]:
    match = UNRESOLVED_WITH_ALIAS_RE.search(error_text)
    if match:
        return match.group("alias"), match.group("column")
    match = UNRESOLVED_NO_ALIAS_RE.search(error_text)
    if match:
        return None, match.group("column")
    return None, None


def _infer_column_type(column_name: str, query: str) -> str:
    lower_column = column_name.lower()
    lower_query = query.lower()
    if re.search(rf"case\s+when\s+{re.escape(lower_column)}\b", lower_query):
        return "BOOLEAN"
    if re.search(rf"sum\s*\(\s*{re.escape(lower_column)}\s*\)", lower_query):
        return "DOUBLE"
    if lower_column.startswith("is_"):
        return "BOOLEAN"
    if lower_column.endswith("_at"):
        return "TIMESTAMP"
    if any(token in lower_column for token in ("cost", "price", "amount", "subtotal", "total", "tax", "rate")):
        return "DOUBLE"
    return "STRING"


def _table_has_column(spark, qualified_table: str, column_name: str) -> bool:
    for col in spark.sql(f"DESCRIBE TABLE {qualified_table}").collect():
        name = col.col_name
        if not name or name.startswith("#"):
            continue
        if name.lower() == column_name.lower():
            return True
    return False


def _run_query_with_auto_missing_columns(spark, query: str, registered_tables: set[str]):
    alias_map = _extract_table_alias_map(query)
    for _ in range(30):
        try:
            return spark.sql(query)
        except Exception as exc:  # noqa: BLE001
            alias, column_name = _parse_unresolved_column(str(exc))
            if not column_name:
                raise
            target_tables: list[str] = []
            if alias and alias in alias_map and alias_map[alias] in registered_tables:
                target_tables = [alias_map[alias]]
            else:
                target_tables = sorted(registered_tables)
            column_type = _infer_column_type(column_name, query)
            changed = False
            for qualified_table in target_tables:
                if _table_has_column(spark, qualified_table, column_name):
                    continue
                spark.sql(f"ALTER TABLE {qualified_table} ADD COLUMNS (`{column_name}` {column_type})")
                changed = True
            if not changed:
                raise
    raise RuntimeError("Exceeded unresolved-column auto-repair attempts")


def _coerce_value_to_field(value: Any, field) -> Any:
    if value is None:
        return None
    if isinstance(field.dataType, DecimalType):
        return Decimal(str(value))
    if isinstance(field.dataType, DateType):
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))
    if isinstance(field.dataType, TimestampType):
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value).replace("T", " "))
    return value


def _coerce_expected_rows(expect_rows: list[dict[str, Any]], schema):
    coerced: list[dict[str, Any]] = []
    for row in expect_rows:
        coerced_row: dict[str, Any] = {}
        for field in schema.fields:
            coerced_row[field.name] = _coerce_value_to_field(row.get(field.name), field)
        coerced.append(coerced_row)
    return coerced


def _clear_schemas(spark, schema_names: set[str]) -> None:
    for schema in sorted(schema_names):
        spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")


def _schema_map_from_case(case: dict[str, Any]) -> dict[str, str]:
    schema_map: dict[str, str] = {}
    for key in ("bronze_schema", "silver_schema", "gold_schema"):
        if key in case:
            schema_map[key] = case[key]
    return schema_map


def _model_path(case: dict[str, Any]) -> Path:
    model = case.get("model")
    if not model:
        raise ValueError("Test case missing required key: model")
    model_path = Path(model)
    if model_path.is_absolute():
        return model_path

    spec_dir = Path(case.get("__spec_dir", ""))
    if spec_dir and (spec_dir / model_path).exists():
        return (spec_dir / model_path).resolve()

    return (Path.cwd() / model).resolve()


def _set_model_runtime_conf(spark, case: dict[str, Any]) -> None:
    for key in ("bronze_schema", "silver_schema", "gold_schema", "source_base_path"):
        if case.get(key):
            spark.conf.set(key, case[key])


def _run_python_model(model_path: Path, case: dict[str, Any]):
    module_name = f"_sdp_model_{model_path.stem}_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, str(model_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load Python model from {model_path}")

    module = importlib.util.module_from_spec(spec)

    # Local pytest execution runs outside declarative pipeline context.
    # Temporarily shim dp decorators so Python model files can be imported and executed.
    saved_pipelines_module = sys.modules.get("pyspark.pipelines")
    from .pipelines_shim import dp as shim_dp

    shim_module = types.ModuleType("pyspark.pipelines")
    setattr(shim_module, "table", shim_dp.table)
    setattr(shim_module, "materialized_view", shim_dp.materialized_view)
    setattr(shim_module, "expect_or_fail", shim_dp.expect_or_fail)
    sys.modules["pyspark.pipelines"] = shim_module
    try:
        spec.loader.exec_module(module)
    finally:
        if saved_pipelines_module is not None:
            sys.modules["pyspark.pipelines"] = saved_pipelines_module
        else:
            sys.modules.pop("pyspark.pipelines", None)

    callable_name = case.get("callable") or model_path.stem
    model_callable = getattr(module, callable_name, None)
    if model_callable is None:
        raise ValueError(f"Could not find callable '{callable_name}' in Python model file: {model_path}")
    if not callable(model_callable):
        raise ValueError(f"Resolved attribute '{callable_name}' in {model_path} is not callable")

    return model_callable()


def _normalize_log_level(value: str | None, default: str = "ERROR") -> str:
    if value is None:
        return default
    normalized = str(value).upper()
    if normalized not in LOG_LEVEL_ORDER:
        raise ValueError(f"Unsupported log_level '{value}'. Use one of: {', '.join(LOG_LEVEL_ORDER.keys())}")
    return normalized


def _should_log(current_level: str, desired_level: str) -> bool:
    return LOG_LEVEL_ORDER[current_level] <= LOG_LEVEL_ORDER[desired_level]


def _rows_for_log(df, columns: list[str], limit: int = 50) -> list[dict[str, Any]]:
    rows = [{col: row[col] for col in columns} for row in df.limit(limit).collect()]
    return sorted(rows, key=lambda row: tuple("" if row[col] is None else str(row[col]) for col in columns))
