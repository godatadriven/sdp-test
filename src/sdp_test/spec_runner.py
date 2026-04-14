from __future__ import annotations

import logging
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

from .bundle import load_bundle_context, load_pipeline_test_spec, resolve_template
from .model_sql import register_df_as_view, render_model_query
from .spec_models import PipelineEntrySpec, UnitSpec

logger = logging.getLogger("sdp_test")

UNRESOLVED_WITH_ALIAS_RE = re.compile(
    r"with name [`'](?P<alias>[A-Za-z_]\w*)[`']\.[`'](?P<column>[A-Za-z_]\w*)[`']",
    flags=re.IGNORECASE,
)
UNRESOLVED_NO_ALIAS_RE = re.compile(
    r"with name [`'](?P<column>[A-Za-z_]\w*)[`'] cannot be resolved",
    flags=re.IGNORECASE,
)


@dataclass
class CaseResult:
    left_minus_right: int
    right_minus_left: int
    actual_rows: list[dict[str, Any]]
    expected_rows: list[dict[str, Any]]


def find_spec_files(search_dir: Path | None = None) -> list[Path]:
    if search_dir is None:  # pragma: no cover
        search_dir = Path.cwd()
    files = set()
    files.update(search_dir.rglob("*_pipeline_tests.yml"))
    files.update(search_dir.rglob("*_pipeline_tests.yaml"))
    return sorted(files)


def cases_from_spec(
    spec_path: Path,
    default_bundle_file: Path | None = None,
    variable_resolution_depth: int = 5,
) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    """Process a single *_pipeline_tests.yml spec and return all test cases."""
    pipeline_spec = PipelineEntrySpec.model_validate(load_pipeline_test_spec(str(spec_path)))
    pipeline_spec_data = pipeline_spec.model_dump(exclude_none=True)
    bundle_cfg = pipeline_spec_data.get("bundle") or {}
    bundle_file = bundle_cfg.get("file") or (str(default_bundle_file) if default_bundle_file else "databricks.yml")
    bundle_path = (
        (spec_path.parent / bundle_file).resolve() if not Path(bundle_file).is_absolute() else Path(bundle_file)
    )
    bundle_target = bundle_cfg.get("target")
    bundle_vars = bundle_cfg.get("variables") or {}
    if bundle_path.exists():
        context = load_bundle_context(
            str(bundle_path),
            target=bundle_target,
            variable_overrides=bundle_vars,
            variable_resolution_depth=variable_resolution_depth,
        )
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
    for test in pipeline_spec_data.get("tests") or []:
        merged = {**defaults, **test}
        merged_context = {**context, **defaults}
        case = resolve_template(merged, merged_context)
        case["__spec_dir"] = str(spec_path.parent)
        cases.append((spec_path, case, context))

    for unit_spec_file in _discover_unit_spec_files(pipeline_def, context):
        unit_spec = UnitSpec.model_validate(load_pipeline_test_spec(str(unit_spec_file)))
        unit_spec_data = unit_spec.model_dump(exclude_none=True)
        for test in unit_spec_data.get("tests") or []:
            merged = {**defaults, **test}
            merged_context = {**context, **defaults}
            case = resolve_template(merged, merged_context)
            case["__spec_dir"] = str(unit_spec_file.parent)
            cases.append((unit_spec_file, case, context))
    return cases


def cases_from_pipeline_def(
    pipeline_def: dict[str, Any],
    context: dict[str, Any],
    source_path: Path,
    *,
    lenient: bool = False,
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
        for test in unit_spec_data.get("tests") or []:
            merged = {**defaults, **test}
            merged_context = {**context, **defaults}
            case = resolve_template(merged, merged_context, lenient=lenient)
            case["__spec_dir"] = str(unit_spec_file.parent)
            cases.append((unit_spec_file, case, context))
    return cases


def cases_from_bundle(
    bundle_path: Path, variable_resolution_depth: int = 5
) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    """Load a ``databricks.yml`` and return test cases for all pipelines."""
    logger.debug("Loading bundle: %s", bundle_path)
    context = load_bundle_context(str(bundle_path), variable_resolution_depth=variable_resolution_depth)
    pipelines = (context.get("resources") or {}).get("pipelines") or {}
    logger.info("Found %d pipeline(s) in bundle %s", len(pipelines), bundle_path.name)
    cases: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    for pipeline_def in pipelines.values():
        cases.extend(cases_from_pipeline_def(pipeline_def, context, bundle_path))
    return cases


def _find_bundle_file(start: Path) -> Path | None:
    """Walk up from *start* looking for ``databricks.yml``."""
    current = start if start.is_dir() else start.parent
    for _ in range(10):  # safety limit
        candidate = current / "databricks.yml"
        if candidate.exists():
            return candidate
        parent = current.parent
        if parent == current:
            break
        current = parent
    return None


def cases_from_pipeline_file(
    pipeline_path: Path, variable_resolution_depth: int = 5
) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    """Load a pipeline definition file and return test cases.

    Supports three formats:
    - Open source ``spark-pipeline.yml`` — top-level pipeline definition
    - Databricks resource file (``*.pipeline.yml``) — ``resources.pipelines.*``
    - Databricks bundle (``databricks.yml``) — delegates to :func:`cases_from_bundle`
    """
    pipeline_data = yaml.safe_load(pipeline_path.read_text()) or {}

    # Try to load context from a nearby databricks.yml for full resource resolution.
    bundle_file = _find_bundle_file(pipeline_path)
    if bundle_file:
        try:
            context = load_bundle_context(str(bundle_file), variable_resolution_depth=variable_resolution_depth)
        except Exception:  # noqa: BLE001
            context = _minimal_context(pipeline_path)
    else:
        context = _minimal_context(pipeline_path)

    # Databricks resource file: resources.pipelines.<key>
    resource_pipelines = (pipeline_data.get("resources") or {}).get("pipelines")
    if resource_pipelines:
        cases: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
        for pipeline_def in resource_pipelines.values():
            resolved_def = resolve_template(pipeline_def, context, lenient=True)
            resolved_def["__pipeline_spec_dir"] = str(pipeline_path.parent)
            cases.extend(cases_from_pipeline_def(resolved_def, context, pipeline_path, lenient=True))
        return cases

    # Open source spark-pipeline.yml: top-level pipeline definition
    pipeline_data["__pipeline_spec_dir"] = str(pipeline_path.parent)
    pipeline_data = resolve_template(pipeline_data, context, lenient=True)
    return cases_from_pipeline_def(pipeline_data, context, pipeline_path, lenient=True)


def _minimal_context(pipeline_path: Path) -> dict[str, Any]:
    """Build a minimal context when no bundle file is available."""
    return {
        "bundle": {"name": "default", "uuid": None, "target": "local"},
        "var": {},
        "resources": {},
        "workspace": {"file_path": str(pipeline_path.parent)},
    }


def all_cases(
    search_dir: Path | None = None,
    default_bundle_file: Path | None = None,
) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
    if search_dir is None:  # pragma: no cover
        search_dir = Path.cwd()
    cases: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    for spec_file in find_spec_files(search_dir):
        cases.extend(cases_from_spec(spec_file, default_bundle_file))
    return cases


def case_id(spec_file: Path, case: dict[str, Any]) -> str:
    suite = case.get("__suite") or case.get("pipeline_name") or spec_file.stem
    return f"{suite}::{case.get('name', 'unnamed')}"


def _create_df_with_fallback_schema(spark, rows: list[dict[str, Any]], column_types: dict[str, str] | None = None):
    """Create a DataFrame from row dicts, optionally with explicit column types.

    When *column_types* is provided, an explicit schema is built: specified
    types take precedence, unspecified columns are inferred from data.
    Variant columns are serialised to JSON strings before DataFrame creation
    and converted via ``parse_json()`` afterwards.

    When *column_types* is not provided, Spark infers the schema.  If inference
    fails (e.g. all-null columns), a fallback schema defaults those columns to
    ``StringType``.
    """
    import json as _json

    from pyspark.sql import functions as sf
    from pyspark.sql.types import StringType, StructField, StructType, _parse_datatype_string

    column_types = column_types or {}
    variant_cols = {col for col, typ in column_types.items() if typ.lower() == "variant"}

    # Pre-process: serialise variant columns to JSON strings.
    if variant_cols:
        rows = [
            {k: _json.dumps(v) if k in variant_cols and v is not None else v for k, v in row.items()} for row in rows
        ]

    if column_types:
        # Build explicit schema: specified types take precedence, rest inferred.
        columns = list(rows[0].keys())
        fields: list[StructField] = []
        for col in columns:
            if col in variant_cols:
                fields.append(StructField(col, StringType(), True))
            elif col in column_types:
                fields.append(StructField(col, _parse_datatype_string(column_types[col]), True))
            else:
                sample = next((r[col] for r in rows if r.get(col) is not None), None)
                if sample is None:
                    fields.append(StructField(col, StringType(), True))
                else:
                    probe = spark.createDataFrame([{col: sample}])
                    fields.append(StructField(col, probe.schema[0].dataType, True))
        df = spark.createDataFrame(rows, schema=StructType(fields))
    else:
        # No column_types: let Spark infer everything.
        try:
            df = spark.createDataFrame(rows)
        except Exception:  # noqa: BLE001
            # Fallback for all-null columns: default to StringType.
            columns = list(rows[0].keys())
            fields = []
            for col in columns:
                sample = next((r[col] for r in rows if r.get(col) is not None), None)
                if sample is None:
                    fields.append(StructField(col, StringType(), True))
                else:
                    probe = spark.createDataFrame([{col: sample}])
                    fields.append(StructField(col, probe.schema[0].dataType, True))
            df = spark.createDataFrame(rows, schema=StructType(fields))

    # Post-process: convert JSON strings to VARIANT via parse_json().
    for col in variant_cols:
        if col in df.columns:
            df = df.withColumn(col, sf.parse_json(sf.col(col)))

    return df


def run_case(spark, case: dict[str, Any]) -> CaseResult:
    schema_names: set[str] = set()
    # Collect schemas from well-known keys (backward compatibility).
    for key in ("bronze_schema", "silver_schema", "gold_schema"):
        if key in case:
            schema_names.add(case[key])
    # Also collect schemas referenced in the given inputs so they are cleared
    # even when the pipeline uses custom configuration key names.
    for input_spec in case.get("given") or []:
        table = input_spec.get("table", "")
        if "." in table:
            schema_names.add(table.split(".", 1)[0])
    _clear_schemas(spark, schema_names)

    test_name = case.get("name", "unnamed")
    logger.debug("Running test case: %s", test_name)
    given = case.get("given") or []
    expect_rows = (case.get("expect") or {}).get("rows") or []
    registered_tables: set[str] = set()

    for input_spec in given:
        table = input_spec["table"]
        rows = input_spec.get("rows") or []
        column_types = input_spec.get("schema") or {}
        if "." not in table:
            # Non-schema-qualified table: register as a temporary view so the
            # model can read it with just the table name.
            if not rows:
                spark.sql(f"CREATE OR REPLACE TEMP VIEW {table} AS SELECT CAST(NULL AS STRING) AS _placeholder")
            else:
                df = _create_df_with_fallback_schema(spark, rows, column_types)
                df.createOrReplaceTempView(table)
            registered_tables.add(table)
        else:
            schema_name, table_name = table.split(".", 1)
            if not rows:
                # Spark cannot infer schema from an empty list.  Create a stub table
                # so SQL JOINs can reference it; the auto-missing-columns logic will
                # add any columns the query needs.
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
                spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
                spark.sql(f"CREATE TABLE {schema_name}.{table_name} (_placeholder STRING)")
            else:
                df = _create_df_with_fallback_schema(spark, rows, column_types)
                register_df_as_view(spark, df, schema_name, table_name)
            registered_tables.add(f"{schema_name}.{table_name}")

    model_path = _model_path(case)
    logger.debug("Executing model: %s", model_path)
    _set_model_runtime_conf(spark, case)
    if model_path.suffix.lower() == ".py":
        result_df = _run_python_model(model_path, case)
    else:
        query = render_model_query(str(model_path), _schema_map_from_case(case))
        result_df = _run_query_with_auto_missing_columns(spark, query, registered_tables)

    if not expect_rows:
        actual_count = result_df.count()
        logger.info("%s: actual_rows=%d, expected_rows=0", test_name, actual_count)
        return CaseResult(
            left_minus_right=actual_count,
            right_minus_left=0,
            actual_rows=_rows_for_log(result_df, result_df.columns),
            expected_rows=[],
        )

    expected_columns = list(expect_rows[0].keys())
    actual_subset = result_df.select(*expected_columns)
    # Materialize the result to break the lazy plan chain.  This works around
    # a PySpark 4.1 optimizer bug where dropDuplicates(subset) + exceptAll
    # produces an invalid physical plan (INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND).
    actual_subset = spark.createDataFrame(actual_subset.collect(), schema=actual_subset.schema)
    expected_df = spark.createDataFrame(
        _coerce_expected_rows(expect_rows, actual_subset.schema),
        schema=actual_subset.schema,
    )

    left_minus_right = actual_subset.exceptAll(expected_df).count()
    right_minus_left = expected_df.exceptAll(actual_subset).count()
    actual_rows = _rows_for_log(actual_subset, expected_columns)
    expected_rows_log = _rows_for_log(expected_df, expected_columns)
    logger.info("%s: unexpected_rows=%d, missing_rows=%d", test_name, left_minus_right, right_minus_left)
    logger.debug("%s: actual_rows=%s", test_name, actual_rows)
    logger.debug("%s: expected_rows=%s", test_name, expected_rows_log)
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

    if pipeline_key not in pipelines:
        raise ValueError(f"Could not find pipeline key '{pipeline_key}' in {pipeline_data_origin}")

    pipeline_def = pipelines[pipeline_key]
    return _extract_pipeline_defaults(pipeline_def), pipeline_def


def _extract_pipeline_defaults(pipeline_def: dict[str, Any]) -> dict[str, Any]:
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
        # Open source format: libraries can be a simple string path (e.g. "- transformations/**")
        if isinstance(library, str):
            resolved_lib = resolve_template(library, context, lenient=True)
            base_pattern = str(resolved_lib).split("**", 1)[0].rstrip("/")
            if base_pattern:
                candidate = (base_dir / base_pattern).resolve()
                if candidate.exists():
                    library_roots.add(candidate)
            continue

        file_entry = (library or {}).get("file")
        if file_entry:
            resolved_file = resolve_template(file_entry, context, lenient=True)
            # file_entry can be a string or {"path": "..."} dict.
            if isinstance(resolved_file, dict):
                resolved_file = resolved_file.get("path") or ""
            file_str = str(resolved_file)
            # Strip glob suffixes to get the base directory.
            if "**" in file_str:
                file_str = file_str.split("**", 1)[0].rstrip("/")
            elif "*" in file_str:
                file_str = str(Path(file_str).parent)
            candidate_file = (base_dir / file_str).resolve()
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

        glob_cfg = (library or {}).get("glob") or {}
        include_pattern = glob_cfg.get("include")
        if not include_pattern:
            continue
        resolved_include = resolve_template(include_pattern, context)
        base_pattern = str(resolved_include).split("**", 1)[0].rstrip("/")
        if not base_pattern:  # pragma: no cover
            continue
        candidate = (base_dir / base_pattern).resolve()
        if candidate.exists():
            library_roots.add(candidate)

    # Fallback for pipelines that do not define libraries globs and no file-based unit specs were found.
    if not library_roots and not unit_files:
        root_path = pipeline_def.get("root_path")
        if not root_path:
            return []
        resolved_root = resolve_template(root_path, context, lenient=True) if isinstance(root_path, str) else root_path
        root_dir = Path(str(resolved_root))
        if not root_dir.exists():
            return []
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
    return "STRING"


def _table_has_column(spark, qualified_table: str, column_name: str) -> bool:
    for col in spark.sql(f"DESCRIBE TABLE {qualified_table}").collect():
        name = col.col_name
        if not name or name.startswith("#"):  # pragma: no cover – Spark partition metadata rows
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
            if not column_name:  # pragma: no cover – non-column Spark errors
                raise
            target_tables: list[str] = []
            if alias and alias in alias_map and alias_map[alias] in registered_tables:
                target_tables = [alias_map[alias]]  # pragma: no cover – alias-based resolution
            else:
                target_tables = sorted(registered_tables)
            column_type = _infer_column_type(column_name, query)
            changed = False
            for qualified_table in target_tables:
                if _table_has_column(spark, qualified_table, column_name):
                    continue  # pragma: no cover – column already exists
                spark.sql(f"ALTER TABLE {qualified_table} ADD COLUMNS (`{column_name}` {column_type})")
                changed = True
            if not changed:  # pragma: no cover – column exists in all tables
                raise
    raise RuntimeError("Exceeded unresolved-column auto-repair attempts")  # pragma: no cover


def _coerce_value_to_field(value: Any, field) -> Any:
    from pyspark.sql.types import DateType, DecimalType, TimestampType

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
    import shutil

    for schema in sorted(schema_names):
        spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")

    # Spark's local mode may leave behind physical directories after
    # DROP DATABASE CASCADE, causing LOCATION_ALREADY_EXISTS on subsequent
    # saveAsTable calls.  Remove them explicitly.
    try:
        warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
        warehouse_path = Path(warehouse_dir.removeprefix("file:"))
        for schema in sorted(schema_names):
            schema_dir = warehouse_path / f"{schema}.db"
            if schema_dir.exists():
                shutil.rmtree(schema_dir)
    except Exception:  # noqa: BLE001
        pass  # Best-effort cleanup; don't fail the test if this doesn't work.


def _schema_map_from_case(case: dict[str, Any]) -> dict[str, str]:
    """Build a substitution map from all string-valued pipeline config in the case.

    Pipeline configuration keys (e.g. ``gold_schema_name``, ``silver_schema``) are used
    to replace ``${key}`` placeholders in SQL model files.  Rather than hard-coding a
    fixed set of key names we include every string-valued entry from the case dict,
    except for internal/structural keys that are never meant as SQL placeholders.
    """
    _skip_keys = frozenset(
        {
            "model",
            "name",
            "given",
            "expect",
            "callable",
            "__spec_dir",
            "__suite",
            "__pipeline_spec_dir",
            "pipeline_name",
            "pipeline_schema",
            "catalog",
        }
    )
    return {k: str(v) for k, v in case.items() if isinstance(v, str) and k not in _skip_keys}


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
    for key, value in (case.get("spark_conf") or {}).items():
        spark.conf.set(key, value)


def _patch_readstream():
    """Monkey-patch ``spark.readStream`` to return ``spark.read`` for local batch testing.

    Streaming sources are not available locally, so we redirect
    ``spark.readStream`` to the regular batch ``DataFrameReader``.  This
    mirrors the ``STREAM()`` stripping done for SQL models.

    Returns a callable that restores the original property.
    """
    from pyspark.sql import SparkSession

    original_property = SparkSession.readStream.fget

    SparkSession.readStream = property(lambda self: self.read)  # ty: ignore[invalid-assignment]

    def _restore():
        SparkSession.readStream = property(original_property)

    return _restore


def _patch_qualify_sql():
    """Monkey-patch ``SparkSession.sql`` to rewrite QUALIFY clauses for local testing.

    Open-source PySpark does not support the QUALIFY clause (a Databricks SQL
    extension).  This patch applies the same ``_rewrite_qualify`` transpilation
    used for ``.sql`` model files so that Python models calling ``spark.sql()``
    with QUALIFY also work locally.

    Returns a callable that restores the original method.
    """
    from pyspark.sql import SparkSession
    from .model_sql import _rewrite_qualify

    original_sql = SparkSession.sql

    def _patched_sql(self, sqlQuery, *args, **kwargs):
        return original_sql(self, _rewrite_qualify(sqlQuery), *args, **kwargs)

    SparkSession.sql = _patched_sql  # ty: ignore[invalid-assignment]

    def _restore():
        SparkSession.sql = original_sql

    return _restore


def _run_python_model(model_path: Path, case: dict[str, Any]):
    module_name = f"_sdp_model_{model_path.stem}_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, str(model_path))
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load module spec for: {model_path}")
    module = importlib.util.module_from_spec(spec)

    # Local pytest execution runs outside declarative pipeline context.
    # Temporarily shim dp decorators so Python model files can be imported and executed.
    saved_pipelines_module = sys.modules.get("pyspark.pipelines")
    from . import pipelines_shim

    shim_module = types.ModuleType("pyspark.pipelines")
    for attr in (
        # Core decorators
        "table",
        "materialized_view",
        "temporary_view",
        "append_flow",
        "view",
        # Expectation decorators
        "expect",
        "expect_or_drop",
        "expect_or_fail",
        # Sink decorator
        "foreach_batch_sink",
        # No-op functions
        "create_streaming_table",
        "create_sink",
        "apply_changes",
        "create_auto_cdc_flow",
        "create_auto_cdc_from_snapshot_flow",
    ):
        setattr(shim_module, attr, getattr(pipelines_shim, attr))
    sys.modules["pyspark.pipelines"] = shim_module

    # Redirect spark.readStream to spark.read so streaming models can run
    # locally as batch queries (mirrors STREAM() stripping for SQL models).
    restore_readstream = _patch_readstream()
    # Rewrite QUALIFY clauses in spark.sql() calls, mirroring the transpilation
    # already applied to .sql model files.
    restore_qualify_sql = _patch_qualify_sql()
    try:
        spec.loader.exec_module(module)

        callable_name = case.get("callable") or model_path.stem
        model_callable = getattr(module, callable_name, None)
        if model_callable is None:
            raise ValueError(f"Could not find callable '{callable_name}' in Python model file: {model_path}")
        if not callable(model_callable):
            raise ValueError(f"Resolved attribute '{callable_name}' in {model_path} is not callable")

        return model_callable()
    finally:
        restore_qualify_sql()
        restore_readstream()
        if saved_pipelines_module is not None:  # pragma: no cover – Databricks runtime only
            sys.modules["pyspark.pipelines"] = saved_pipelines_module
        else:
            sys.modules.pop("pyspark.pipelines", None)


def _rows_for_log(df, columns: list[str], limit: int = 50) -> list[dict[str, Any]]:
    rows = [{col: row[col] for col in columns} for row in df.limit(limit).collect()]
    return sorted(rows, key=lambda row: tuple("" if row[col] is None else str(row[col]) for col in columns))
