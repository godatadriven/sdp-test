"""Unit tests for spec_runner module."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from sdp_test import spec_runner
from sdp_test.model_sql import _model_query, rows_as_dicts
from sdp_test.pipelines_shim import _noop_decorator
from sdp_test.spec_models import PipelineEntrySpec, PipelineRefSpec, UnitSpec


def test_case_id_formats_name() -> None:
    spec_file = Path("pipeline_tests/jaffle_shop_sql_pipeline_tests.yml")
    case = {"name": "my_test", "pipeline_name": "jaffle_shop_sql"}
    assert spec_runner.case_id(spec_file, case) == "jaffle_shop_sql::my_test"
    assert spec_runner.case_id(spec_file, {"pipeline_name": "jaffle_shop_sql"}) == "jaffle_shop_sql::unnamed"


def test_find_spec_files_reads_pipeline_specs_only(tmp_path: Path) -> None:
    pipeline_dir = tmp_path / "pipeline_tests"
    pipeline_dir.mkdir(parents=True)

    pipeline_spec = pipeline_dir / "jaffle_shop_pipeline_tests.yml"
    pipeline_spec.write_text("tests: []\n")

    files = spec_runner.find_spec_files(pipeline_dir)

    assert files == [pipeline_spec]


def test_load_pipeline_defaults_from_string_ref() -> None:
    context = {
        "resources": {
            "pipelines": {
                "jaffle_shop_sql": {
                    "name": "jaffle_shop_sql",
                    "catalog": "lakeflow_foundation",
                    "schema": "jaffle_shop_gold",
                    "configuration": {
                        "bronze_schema": "jaffle_shop_bronze",
                        "silver_schema": "jaffle_shop_silver",
                        "gold_schema": "jaffle_shop_gold",
                    },
                }
            }
        }
    }

    defaults, pipeline_def = spec_runner._load_pipeline_defaults(
        Path("spec.yml"), {"pipeline": "pipelines.jaffle_shop_sql"}, context
    )

    assert defaults["bronze_schema"] == "jaffle_shop_bronze"
    assert defaults["silver_schema"] == "jaffle_shop_silver"
    assert defaults["gold_schema"] == "jaffle_shop_gold"
    assert defaults["catalog"] == "lakeflow_foundation"
    assert defaults["pipeline_schema"] == "jaffle_shop_gold"
    assert defaults["pipeline_name"] == "jaffle_shop_sql"
    assert pipeline_def["name"] == "jaffle_shop_sql"


def test_load_pipeline_defaults_rejects_invalid_string_ref() -> None:
    with pytest.raises(ValueError, match="pipelines\\.<key>"):
        spec_runner._load_pipeline_defaults(Path("spec.yml"), {"pipeline": "jaffle_shop"}, {"resources": {}})


def test_pipeline_entry_spec_requires_pipeline() -> None:
    with pytest.raises(ValidationError, match="pipeline"):
        PipelineEntrySpec.model_validate({"bundle": {"file": "databricks.yml"}})


def test_unit_spec_requires_model_in_each_test() -> None:
    with pytest.raises(ValidationError, match="model"):
        UnitSpec.model_validate({"tests": [{"name": "missing_model"}]})


def test_discover_unit_spec_files_from_libraries_glob(tmp_path: Path) -> None:
    resources_dir = tmp_path / "resources"
    root_path = tmp_path / "src" / "jaffle_shop_sql" / "transformations"
    resources_dir.mkdir(parents=True)
    (root_path / "silver").mkdir(parents=True)
    unit_file = root_path / "silver" / "stg_locations.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"glob": {"include": "../src/jaffle_shop_sql/transformations/**"}}]},
        {"workspace": {"file_path": str(tmp_path)}},
    )

    assert files == [unit_file]


def test_discover_unit_spec_files_from_pipeline_root(tmp_path: Path) -> None:
    root_path = tmp_path / "transformations"
    (root_path / "silver").mkdir(parents=True)
    unit_file = root_path / "silver" / "stg_locations.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"root_path": str(root_path)},
        {"workspace": {"file_path": str(tmp_path)}},
    )

    assert files == [unit_file]


def test_discover_unit_spec_files_from_file_library(tmp_path: Path) -> None:
    resources_dir = tmp_path / "resources"
    sql_dir = tmp_path / "src" / "jaffle_shop_sql" / "transformations" / "silver"
    resources_dir.mkdir(parents=True)
    sql_dir.mkdir(parents=True)

    sql_file = sql_dir / "stg_locations.sql"
    sql_file.write_text("SELECT 1")
    unit_file = sql_dir / "stg_locations.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"file": "../src/jaffle_shop_sql/transformations/silver/stg_locations.sql"}]},
        {"workspace": {"file_path": str(tmp_path)}},
    )

    assert files == [unit_file]


def test_discover_unit_spec_files_from_python_file_library(tmp_path: Path) -> None:
    resources_dir = tmp_path / "resources"
    py_dir = tmp_path / "src" / "jaffle_shop_python" / "transformations" / "silver"
    resources_dir.mkdir(parents=True)
    py_dir.mkdir(parents=True)

    py_file = py_dir / "stg_locations.py"
    py_file.write_text("def stg_locations():\n    return None\n")
    unit_file = py_dir / "stg_locations.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"file": "../src/jaffle_shop_python/transformations/silver/stg_locations.py"}]},
        {"workspace": {"file_path": str(tmp_path)}},
    )

    assert files == [unit_file]


def test_run_case_executes_model_sql(spark, tmp_path: Path) -> None:
    model_sql = tmp_path / "simple_model.sql"
    model_sql.write_text(
        """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.simple_model
(
    id STRING
)
CLUSTER BY AUTO
AS
SELECT CAST(id AS STRING) AS id
FROM ${bronze_schema}.raw_input;
""".strip()
    )

    case = {
        "name": "simple_model_returns_rows",
        "bronze_schema": "ut_bronze",
        "silver_schema": "ut_silver",
        "gold_schema": "ut_gold",
        "model": str(model_sql),
        "given": [
            {
                "table": "ut_bronze.raw_input",
                "rows": [{"id": "1"}, {"id": "2"}],
            }
        ],
        "expect": {
            "rows": [{"id": "1"}, {"id": "2"}],
        },
    }

    result = spec_runner.run_case(spark, case)

    assert result.left_minus_right == 0
    assert result.right_minus_left == 0


def test_run_case_rejects_unqualified_input_table(spark, tmp_path: Path) -> None:
    model_sql = tmp_path / "simple_model.sql"
    model_sql.write_text(
        """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.simple_model
(
    id STRING
)
CLUSTER BY AUTO
AS
SELECT CAST(id AS STRING) AS id
FROM ${bronze_schema}.raw_input;
""".strip()
    )

    case = {
        "name": "bad_table_name",
        "bronze_schema": "ut_bronze",
        "silver_schema": "ut_silver",
        "gold_schema": "ut_gold",
        "model": str(model_sql),
        "given": [
            {
                "table": "raw_input",
                "rows": [{"id": "1"}],
            }
        ],
        "expect": {"rows": [{"id": "1"}]},
    }

    with pytest.raises(ValueError, match="schema-qualified"):
        spec_runner.run_case(spark, case)


def test_run_case_executes_model_python(spark, tmp_path: Path) -> None:
    model_py = tmp_path / "simple_model.py"
    model_py.write_text(
        """
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
BRONZE_SCHEMA = spark.conf.get("bronze_schema")

def simple_model():
    return spark.read.table(f"{BRONZE_SCHEMA}.raw_input").select(
        F.col("id").cast("string").alias("id")
    )
""".strip()
    )

    case = {
        "name": "simple_model_python_returns_rows",
        "bronze_schema": "ut_bronze",
        "silver_schema": "ut_silver",
        "gold_schema": "ut_gold",
        "model": str(model_py),
        "given": [
            {
                "table": "ut_bronze.raw_input",
                "rows": [{"id": "1"}, {"id": "2"}],
            }
        ],
        "expect": {
            "rows": [{"id": "1"}, {"id": "2"}],
        },
    }

    result = spec_runner.run_case(spark, case)

    assert result.left_minus_right == 0
    assert result.right_minus_left == 0


# ---------------------------------------------------------------------------
# model_sql tests
# ---------------------------------------------------------------------------


def test_model_query_raises_on_missing_as_select() -> None:
    with pytest.raises(ValueError, match="Could not find a query body"):
        _model_query("SELECT id FROM table")


def test_model_query_strips_stream_wrapper() -> None:
    sql = (
        "CREATE OR REFRESH STREAMING TABLE t AS\n"
        "WITH cte AS (\n"
        "  SELECT * FROM STREAM(silver.orders)\n"
        ")\n"
        "SELECT cte.id, s.name\n"
        "FROM cte\n"
        "JOIN STREAM(silver.products) s ON cte.product_id = s.id"
    )
    query = _model_query(sql)
    assert "STREAM(" not in query
    assert "FROM silver.orders" in query
    assert "JOIN silver.products s" in query


def test_rows_as_dicts_extracts_columns(spark) -> None:
    df = spark.createDataFrame([{"a": "1", "b": "2"}, {"a": "3", "b": "4"}])
    result = rows_as_dicts(df, ["a"])
    assert result == [{"a": "1"}, {"a": "3"}]


# ---------------------------------------------------------------------------
# pipelines_shim tests
# ---------------------------------------------------------------------------


def test_noop_decorator_without_parens() -> None:
    """@table without parentheses returns the function unchanged."""

    @_noop_decorator
    def my_func():
        return 42

    assert my_func() == 42


# ---------------------------------------------------------------------------
# spec_models tests
# ---------------------------------------------------------------------------


def test_pipeline_ref_spec_requires_ref_or_file() -> None:
    with pytest.raises(ValidationError, match="ref.*file"):
        PipelineRefSpec.model_validate({})


def test_pipeline_ref_spec_with_ref() -> None:
    spec = PipelineRefSpec.model_validate({"ref": "pipelines.my_pipe"})
    assert spec.ref == "pipelines.my_pipe"


# ---------------------------------------------------------------------------
# spec_runner: _load_pipeline_defaults edge cases
# ---------------------------------------------------------------------------


def test_load_pipeline_defaults_missing_pipeline_section() -> None:
    with pytest.raises(ValueError, match="pipeline"):
        spec_runner._load_pipeline_defaults(Path("spec.yml"), {}, {})


def test_load_pipeline_defaults_invalid_ref_object() -> None:
    with pytest.raises(ValueError, match="pipelines\\.<key>"):
        spec_runner._load_pipeline_defaults(
            Path("spec.yml"),
            {"pipeline": {"ref": "bad_ref"}},
            {"resources": {}},
        )


def test_load_pipeline_defaults_ref_object_form() -> None:
    context = {
        "resources": {
            "pipelines": {
                "my_pipe": {
                    "name": "my_pipe",
                    "configuration": {"bronze_schema": "b"},
                }
            }
        }
    }
    defaults, pipeline_def = spec_runner._load_pipeline_defaults(
        Path("spec.yml"),
        {"pipeline": {"ref": "pipelines.my_pipe"}},
        context,
    )
    assert defaults["pipeline_name"] == "my_pipe"
    assert defaults["bronze_schema"] == "b"


def test_load_pipeline_defaults_file_with_key(tmp_path: Path) -> None:
    resource_file = tmp_path / "pipeline.yml"
    resource_file.write_text(
        """
resources:
  pipelines:
    my_pipe:
      name: my_pipe
      catalog: test
      configuration:
        bronze_schema: bronze
"""
    )
    defaults, pipeline_def = spec_runner._load_pipeline_defaults(
        tmp_path / "spec.yml",
        {"pipeline": {"file": "pipeline.yml", "key": "my_pipe"}},
        {"resources": {}},
    )
    assert defaults["pipeline_name"] == "my_pipe"
    assert defaults["bronze_schema"] == "bronze"


def test_load_pipeline_defaults_open_source_file(tmp_path: Path) -> None:
    pipeline_file = tmp_path / "spark-pipeline.yml"
    pipeline_file.write_text(
        """
name: oss_pipe
catalog: cat
database: db
configuration:
  bronze_schema: b
"""
    )
    defaults, pipeline_def = spec_runner._load_pipeline_defaults(
        tmp_path / "spec.yml",
        {"pipeline": {"file": "spark-pipeline.yml"}},
        {"resources": {}},
    )
    assert defaults["pipeline_name"] == "oss_pipe"
    assert defaults["pipeline_schema"] == "db"


def test_load_pipeline_defaults_missing_file_in_dict() -> None:
    with pytest.raises(ValueError, match="file"):
        spec_runner._load_pipeline_defaults(
            Path("spec.yml"),
            {"pipeline": {"key": "some_key"}},  # dict with key but no file
            {"resources": {}},
        )


def test_load_pipeline_defaults_non_string_non_dict() -> None:
    with pytest.raises(ValueError, match="string or object"):
        spec_runner._load_pipeline_defaults(Path("spec.yml"), {"pipeline": 42}, {"resources": {}})


def test_load_pipeline_defaults_missing_key_in_resources() -> None:
    context = {"resources": {"pipelines": {}}}
    with pytest.raises(ValueError, match="Could not find pipeline key"):
        spec_runner._load_pipeline_defaults(
            Path("spec.yml"),
            {"pipeline": "pipelines.nonexistent"},
            context,
        )


# ---------------------------------------------------------------------------
# spec_runner: cases_from_bundle
# ---------------------------------------------------------------------------


def test_cases_from_bundle(tmp_path: Path) -> None:
    bundle = tmp_path / "databricks.yml"
    bundle.write_text(
        """
bundle:
  name: test

include:
  - "resources/*.yml"
"""
    )
    resources = tmp_path / "resources"
    resources.mkdir()
    (resources / "pipeline.yml").write_text(
        """
resources:
  pipelines:
    p:
      name: p
      configuration:
        bronze_schema: b
      libraries: []
"""
    )
    cases = spec_runner.cases_from_bundle(bundle)
    assert cases == []  # no unit test files to discover


# ---------------------------------------------------------------------------
# spec_runner: cases_from_pipeline_file
# ---------------------------------------------------------------------------


def test_cases_from_pipeline_file_resource_format(tmp_path: Path) -> None:
    pipeline_file = tmp_path / "my.pipeline.yml"
    pipeline_file.write_text(
        """
resources:
  pipelines:
    p:
      name: p
      configuration:
        bronze_schema: b
      libraries: []
"""
    )
    cases = spec_runner.cases_from_pipeline_file(pipeline_file)
    assert cases == []


def test_cases_from_pipeline_file_open_source_format(tmp_path: Path) -> None:
    (tmp_path / "transformations").mkdir()
    pipeline_file = tmp_path / "spark-pipeline.yml"
    pipeline_file.write_text(
        """
name: p
catalog: c
database: d
configuration:
  bronze_schema: b
libraries:
  - transformations/**
"""
    )
    cases = spec_runner.cases_from_pipeline_file(pipeline_file)
    assert cases == []


def test_cases_from_pipeline_file_with_nearby_bundle(tmp_path: Path) -> None:
    (tmp_path / "databricks.yml").write_text("bundle:\n  name: nearby\n")
    pipeline_file = tmp_path / "spark-pipeline.yml"
    pipeline_file.write_text(
        """
name: p
configuration:
  bronze_schema: b
libraries: []
"""
    )
    cases = spec_runner.cases_from_pipeline_file(pipeline_file)
    assert cases == []


# ---------------------------------------------------------------------------
# spec_runner: _find_bundle_file
# ---------------------------------------------------------------------------


def test_find_bundle_file_walks_up(tmp_path: Path) -> None:
    (tmp_path / "databricks.yml").write_text("bundle:\n  name: x\n")
    child = tmp_path / "a" / "b"
    child.mkdir(parents=True)
    result = spec_runner._find_bundle_file(child)
    assert result == tmp_path / "databricks.yml"


def test_find_bundle_file_not_found(tmp_path: Path) -> None:
    result = spec_runner._find_bundle_file(tmp_path)
    assert result is None


def test_find_bundle_file_from_file_path(tmp_path: Path) -> None:
    (tmp_path / "databricks.yml").write_text("bundle:\n  name: x\n")
    file_in_dir = tmp_path / "pipeline.yml"
    file_in_dir.write_text("name: p\n")
    result = spec_runner._find_bundle_file(file_in_dir)
    assert result == tmp_path / "databricks.yml"


# ---------------------------------------------------------------------------
# spec_runner: _discover_unit_spec_files with string library paths
# ---------------------------------------------------------------------------


def test_discover_unit_spec_files_string_library(tmp_path: Path) -> None:
    sql_dir = tmp_path / "transformations" / "silver"
    sql_dir.mkdir(parents=True)
    unit_file = sql_dir / "m.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": ["transformations/**"], "__pipeline_spec_dir": str(tmp_path)},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == [unit_file]


def test_discover_unit_spec_files_empty_returns_empty(tmp_path: Path) -> None:
    files = spec_runner._discover_unit_spec_files(
        {"libraries": []},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == []


# ---------------------------------------------------------------------------
# spec_runner: _infer_column_type branches
# ---------------------------------------------------------------------------


def test_infer_column_type_case_when() -> None:
    assert spec_runner._infer_column_type("flag", "CASE WHEN flag THEN 1 END") == "BOOLEAN"


def test_infer_column_type_sum() -> None:
    assert spec_runner._infer_column_type("val", "SUM(val)") == "DOUBLE"


def test_infer_column_type_is_prefix() -> None:
    assert spec_runner._infer_column_type("is_active", "") == "BOOLEAN"


def test_infer_column_type_default() -> None:
    assert spec_runner._infer_column_type("name", "") == "STRING"


# ---------------------------------------------------------------------------
# spec_runner: _parse_unresolved_column
# ---------------------------------------------------------------------------


def test_parse_unresolved_column_with_alias() -> None:
    error_text = "A column with name `oi`.`missing_col` cannot be resolved"
    alias, column = spec_runner._parse_unresolved_column(error_text)
    assert alias == "oi"
    assert column == "missing_col"


def test_parse_unresolved_column_without_alias() -> None:
    error_text = "column with name `some_col` cannot be resolved"
    alias, column = spec_runner._parse_unresolved_column(error_text)
    assert alias is None
    assert column == "some_col"


def test_parse_unresolved_column_no_match() -> None:
    alias, column = spec_runner._parse_unresolved_column("some random error")
    assert alias is None
    assert column is None


# ---------------------------------------------------------------------------
# spec_runner: _extract_table_alias_map
# ---------------------------------------------------------------------------


def test_extract_table_alias_map() -> None:
    query = "SELECT * FROM schema.table AS t JOIN schema.other o ON t.id = o.id"
    result = spec_runner._extract_table_alias_map(query)
    assert result == {"t": "schema.table", "o": "schema.other"}


# ---------------------------------------------------------------------------
# spec_runner: _model_path edge cases
# ---------------------------------------------------------------------------


def test_model_path_missing_model() -> None:
    with pytest.raises(ValueError, match="model"):
        spec_runner._model_path({})


def test_model_path_relative_fallback_to_cwd(tmp_path: Path) -> None:
    """When spec_dir doesn't contain the model, falls back to cwd."""
    path = spec_runner._model_path({"model": "nonexistent.sql", "__spec_dir": str(tmp_path)})
    assert path.name == "nonexistent.sql"


# ---------------------------------------------------------------------------
# spec_runner: _run_python_model error paths
# ---------------------------------------------------------------------------


def test_run_python_model_missing_callable(spark, tmp_path: Path) -> None:
    model = tmp_path / "no_func.py"
    model.write_text("x = 1\n")
    with pytest.raises(ValueError, match="Could not find callable"):
        spec_runner._run_python_model(model, {"callable": "nonexistent"})


def test_run_python_model_not_callable(spark, tmp_path: Path) -> None:
    model = tmp_path / "not_callable.py"
    model.write_text("my_var = 42\n")
    with pytest.raises(ValueError, match="is not callable"):
        spec_runner._run_python_model(model, {"callable": "my_var"})


# ---------------------------------------------------------------------------
# spec_runner: run_case with empty expect
# ---------------------------------------------------------------------------


def test_run_case_empty_expect(spark, tmp_path: Path) -> None:
    model_sql = tmp_path / "model_ee.sql"
    model_sql.write_text(
        "CREATE MATERIALIZED VIEW ${silver_schema}.m AS\n"
        "SELECT CAST(id AS STRING) AS id FROM ${bronze_schema}.src;"
    )
    case = {
        "name": "empty_expect",
        "bronze_schema": "ee_bronze",
        "silver_schema": "ee_silver",
        "model": str(model_sql),
        "given": [{"table": "ee_bronze.src", "rows": [{"id": "1"}]}],
        "expect": {"rows": []},
    }
    result = spec_runner.run_case(spark, case)
    assert result.left_minus_right == 1  # 1 actual row, 0 expected
    assert result.right_minus_left == 0


# ---------------------------------------------------------------------------
# spec_runner: _coerce_value_to_field
# ---------------------------------------------------------------------------


def test_coerce_value_to_field_none() -> None:
    from unittest.mock import MagicMock

    field = MagicMock()
    assert spec_runner._coerce_value_to_field(None, field) is None


def test_coerce_value_to_field_date_passthrough() -> None:
    from datetime import date
    from unittest.mock import MagicMock

    from pyspark.sql.types import DateType

    field = MagicMock()
    field.dataType = DateType()
    result = spec_runner._coerce_value_to_field(date(2024, 1, 1), field)
    assert result == date(2024, 1, 1)


def test_coerce_value_to_field_timestamp_passthrough() -> None:
    from datetime import datetime
    from unittest.mock import MagicMock

    from pyspark.sql.types import TimestampType

    field = MagicMock()
    field.dataType = TimestampType()
    result = spec_runner._coerce_value_to_field(datetime(2024, 1, 1, 10, 30), field)
    assert result == datetime(2024, 1, 1, 10, 30)


# ---------------------------------------------------------------------------
# spec_runner: _table_has_column
# ---------------------------------------------------------------------------


def test_table_has_column(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS thc_test")
    spark.createDataFrame([{"col_a": "x"}]).write.mode("overwrite").saveAsTable("thc_test.my_tbl")
    assert spec_runner._table_has_column(spark, "thc_test.my_tbl", "col_a") is True
    assert spec_runner._table_has_column(spark, "thc_test.my_tbl", "nonexistent") is False
    spark.sql("DROP DATABASE IF EXISTS thc_test CASCADE")


# ---------------------------------------------------------------------------
# spec_runner: cases_from_pipeline_def (line 115-122)
# ---------------------------------------------------------------------------


def test_cases_from_pipeline_def_with_unit_tests(tmp_path: Path) -> None:
    """cases_from_pipeline_def discovers and returns cases from unit test files."""
    sql_dir = tmp_path / "transformations" / "silver"
    sql_dir.mkdir(parents=True)
    sql_file = sql_dir / "my_model.sql"
    sql_file.write_text(
        "CREATE MATERIALIZED VIEW ${silver_schema}.my_model AS\n"
        "SELECT id FROM ${bronze_schema}.src;"
    )
    unit_file = sql_dir / "my_model.unit_tests.yml"
    unit_file.write_text(
        "tests:\n"
        "  - name: test1\n"
        "    model: my_model.sql\n"
        "    given:\n"
        "      - table: bronze.src\n"
        "        rows: [{id: '1'}]\n"
        "    expect:\n"
        "      rows: [{id: '1'}]\n"
    )
    pipeline_def = {
        "name": "p",
        "configuration": {"bronze_schema": "bronze", "silver_schema": "silver"},
        "libraries": ["transformations/**"],
        "__pipeline_spec_dir": str(tmp_path),
    }
    context = {"workspace": {"file_path": str(tmp_path)}}
    cases = spec_runner.cases_from_pipeline_def(pipeline_def, context, tmp_path / "pipeline.yml")
    assert len(cases) == 1
    assert cases[0][1]["name"] == "test1"


# ---------------------------------------------------------------------------
# spec_runner: cases_from_pipeline_file exception fallback (lines 167-168)
# ---------------------------------------------------------------------------


def test_cases_from_pipeline_file_bad_bundle_fallback(tmp_path: Path) -> None:
    """When nearby databricks.yml can't be loaded, falls back to minimal context."""
    (tmp_path / "databricks.yml").write_text("not: valid: bundle: yaml:")
    pipeline_file = tmp_path / "spark-pipeline.yml"
    pipeline_file.write_text(
        "name: p\n"
        "configuration:\n"
        "  bronze_schema: b\n"
        "libraries: []\n"
    )
    cases = spec_runner.cases_from_pipeline_file(pipeline_file)
    assert cases == []


# ---------------------------------------------------------------------------
# spec_runner: _discover_unit_spec_files edge cases
# ---------------------------------------------------------------------------


def test_discover_unit_spec_files_file_as_dict_path(tmp_path: Path) -> None:
    """Library file entry with dict path format (resolved_file is a dict)."""
    sql_dir = tmp_path / "src"
    sql_dir.mkdir()
    sql_file = sql_dir / "model.sql"
    sql_file.write_text("SELECT 1")
    unit_file = sql_dir / "model.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    # Use __pipeline_spec_dir so base_dir = tmp_path
    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"file": {"path": "src/model.sql"}}], "__pipeline_spec_dir": str(tmp_path)},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == [unit_file]


def test_discover_unit_spec_files_file_glob_double_star(tmp_path: Path) -> None:
    """Library file entry with ** glob gets base dir extracted."""
    sql_dir = tmp_path / "src" / "silver"
    sql_dir.mkdir(parents=True)
    unit_file = sql_dir / "m.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"file": "src/**"}], "__pipeline_spec_dir": str(tmp_path)},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == [unit_file]


def test_discover_unit_spec_files_file_glob_single_star(tmp_path: Path) -> None:
    """Library file entry with single * glob uses parent dir."""
    sql_dir = tmp_path / "src"
    sql_dir.mkdir()
    unit_file = sql_dir / "m.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"file": "src/*.sql"}], "__pipeline_spec_dir": str(tmp_path)},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == [unit_file]


def test_discover_unit_spec_files_file_is_directory(tmp_path: Path) -> None:
    """Library file entry that resolves to a directory."""
    sql_dir = tmp_path / "src" / "models"
    sql_dir.mkdir(parents=True)
    unit_file = sql_dir / "m.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"file": "src/models"}], "__pipeline_spec_dir": str(tmp_path)},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == [unit_file]


def test_discover_unit_spec_files_file_is_yaml_unit_test(tmp_path: Path) -> None:
    """Library file entry that is directly a .unit_tests.yml file."""
    sql_dir = tmp_path / "src"
    sql_dir.mkdir()
    unit_file = sql_dir / "model.unit_tests.yml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"file": "src/model.unit_tests.yml"}], "__pipeline_spec_dir": str(tmp_path)},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == [unit_file]


def test_discover_unit_spec_files_file_sibling_yaml(tmp_path: Path) -> None:
    """Library file entry with sibling .unit_tests.yaml (not .yml)."""
    sql_dir = tmp_path / "src"
    sql_dir.mkdir()
    sql_file = sql_dir / "model.sql"
    sql_file.write_text("SELECT 1")
    unit_file = sql_dir / "model.unit_tests.yaml"
    unit_file.write_text("tests: []\n")

    files = spec_runner._discover_unit_spec_files(
        {"libraries": [{"file": "src/model.sql"}], "__pipeline_spec_dir": str(tmp_path)},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == [unit_file]


def test_discover_unit_spec_files_root_path_nonexistent(tmp_path: Path) -> None:
    """root_path fallback returns empty when path doesn't exist."""
    files = spec_runner._discover_unit_spec_files(
        {"root_path": str(tmp_path / "nonexistent")},
        {"workspace": {"file_path": str(tmp_path)}},
    )
    assert files == []


# ---------------------------------------------------------------------------
# spec_runner: _coerce_value_to_field timestamp string
# ---------------------------------------------------------------------------


def test_coerce_value_to_field_timestamp_string() -> None:
    from unittest.mock import MagicMock

    from pyspark.sql.types import TimestampType

    field = MagicMock()
    field.dataType = TimestampType()
    result = spec_runner._coerce_value_to_field("2024-01-15 10:30:00", field)
    from datetime import datetime

    assert result == datetime(2024, 1, 15, 10, 30, 0)


# ---------------------------------------------------------------------------
# spec_runner: _coerce_value_to_field date string (line 523)
# ---------------------------------------------------------------------------


def test_coerce_value_to_field_date_string() -> None:
    from unittest.mock import MagicMock

    from pyspark.sql.types import DateType

    field = MagicMock()
    field.dataType = DateType()
    result = spec_runner._coerce_value_to_field("2024-06-15", field)
    from datetime import date

    assert result == date(2024, 6, 15)


