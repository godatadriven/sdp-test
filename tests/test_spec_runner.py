"""Unit tests for spec_runner module."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from sdp_test import spec_runner
from sdp_test.spec_models import PipelineEntrySpec, UnitSpec


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


def test_normalize_log_level_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="Unsupported log_level"):
        spec_runner._normalize_log_level("TRACE")
