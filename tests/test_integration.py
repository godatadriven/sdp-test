"""Integration tests exercising the full sdp-test flow with open source PySpark.

These tests create a complete mini-project on disk (bundle, pipeline config,
SQL/Python models, and YAML test specs) and run the full discovery → execution
→ assertion pipeline using a local Spark session.
"""

from __future__ import annotations

from pathlib import Path

from sdp_test import all_cases, case_id, run_case


def _create_project(tmp_path: Path) -> Path:
    """Scaffold a minimal project with bundle, pipeline, models, and test specs."""

    # databricks.yml
    (tmp_path / "databricks.yml").write_text(
        """
bundle:
  name: integration_test

variables:
  catalog:
    default: test_catalog

include:
  - "resources/*.yml"
"""
    )

    # resources/
    resources = tmp_path / "resources"
    resources.mkdir()

    (resources / "schemas.yml").write_text(
        """
resources:
  schemas:
    bronze:
      name: it_bronze
    silver:
      name: it_silver
    gold:
      name: it_gold
"""
    )

    (resources / "pipeline.yml").write_text(
        """
resources:
  pipelines:
    shop:
      name: shop
      catalog: ${var.catalog}
      schema: ${resources.schemas.gold.name}
      configuration:
        bronze_schema: ${resources.schemas.bronze.name}
        silver_schema: ${resources.schemas.silver.name}
        gold_schema: ${resources.schemas.gold.name}
      libraries:
        - glob:
            include: ../src/shop/transformations/**
"""
    )

    # SQL model
    sql_dir = tmp_path / "src" / "shop" / "transformations" / "silver"
    sql_dir.mkdir(parents=True)

    (sql_dir / "stg_products.sql").write_text(
        """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.stg_products
(
    product_id STRING,
    product_name STRING,
    price DOUBLE
)
CLUSTER BY AUTO
AS
SELECT
    CAST(id AS STRING) AS product_id,
    name AS product_name,
    CAST(price AS DOUBLE) AS price
FROM ${bronze_schema}.raw_products;
"""
    )

    # Colocated unit test spec for SQL model
    (sql_dir / "stg_products.unit_tests.yml").write_text(
        """
tests:
  - name: maps_product_fields
    model: stg_products.sql
    given:
      - table: ${bronze_schema}.raw_products
        rows:
          - id: "1"
            name: Widget
            price: "9.99"
          - id: "2"
            name: Gadget
            price: "19.99"
    expect:
      rows:
        - product_id: "1"
          product_name: Widget
          price: 9.99
        - product_id: "2"
          product_name: Gadget
          price: 19.99

  - name: handles_single_product
    model: stg_products.sql
    given:
      - table: ${bronze_schema}.raw_products
        rows:
          - id: "42"
            name: Solo
            price: "0.01"
    expect:
      rows:
        - product_id: "42"
          product_name: Solo
          price: 0.01
"""
    )

    # Python model
    py_dir = tmp_path / "src" / "shop" / "transformations" / "gold"
    py_dir.mkdir(parents=True)

    (py_dir / "product_summary.py").write_text(
        """
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
SILVER_SCHEMA = spark.conf.get("silver_schema")

def product_summary():
    return (
        spark.read.table(f"{SILVER_SCHEMA}.stg_products")
        .groupBy("product_name")
        .agg(F.sum("price").alias("total_price"), F.count("*").alias("count"))
    )
"""
    )

    # Colocated unit test spec for Python model
    (py_dir / "product_summary.unit_tests.yml").write_text(
        """
tests:
  - name: aggregates_products
    model: product_summary.py
    given:
      - table: ${silver_schema}.stg_products
        rows:
          - product_id: "1"
            product_name: Widget
            price: 10.0
          - product_id: "2"
            product_name: Widget
            price: 5.0
          - product_id: "3"
            product_name: Gadget
            price: 20.0
    expect:
      rows:
        - product_name: Widget
          total_price: 15.0
          count: 2
        - product_name: Gadget
          total_price: 20.0
          count: 1
"""
    )

    # Pipeline test entry spec
    pipeline_tests = tmp_path / "pipeline_tests"
    pipeline_tests.mkdir()

    (pipeline_tests / "shop_pipeline_tests.yml").write_text(
        """
suite: shop_pipeline_tests
log_level: DEBUG

bundle:
  file: ../databricks.yml
  variables:
    catalog: test_catalog

pipeline: pipelines.shop
"""
    )

    return tmp_path


class TestFullDiscoveryAndExecution:
    """End-to-end tests: discover specs from a project, run them with Spark."""

    def test_all_cases_discovers_all_specs(self, tmp_path: Path) -> None:
        project = _create_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        # 2 SQL tests + 1 Python test = 3 total
        assert len(cases) == 3
        names = sorted(case.get("name", "unnamed") for _, case, _ in cases)
        assert names == ["aggregates_products", "handles_single_product", "maps_product_fields"]

    def test_case_ids_are_formatted(self, tmp_path: Path) -> None:
        project = _create_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        ids = [case_id(spec_file, case) for spec_file, case, _ in cases]
        assert all("::" in id_ for id_ in ids)

    def test_sql_model_tests_pass(self, spark, tmp_path: Path) -> None:
        project = _create_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        sql_cases = [(sf, c, ctx) for sf, c, ctx in cases if c.get("model", "").endswith(".sql")]
        assert len(sql_cases) == 2

        for spec_file, case, _context in sql_cases:
            result = run_case(spark, case)
            assert result.left_minus_right == 0 and result.right_minus_left == 0, (
                f"Test failed: {case.get('name')} from {spec_file}\n"
                f"Actual: {result.actual_rows}\nExpected: {result.expected_rows}"
            )

    def test_python_model_tests_pass(self, spark, tmp_path: Path) -> None:
        project = _create_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        py_cases = [(sf, c, ctx) for sf, c, ctx in cases if c.get("model", "").endswith(".py")]
        assert len(py_cases) == 1

        for spec_file, case, _context in py_cases:
            result = run_case(spark, case)
            assert result.left_minus_right == 0 and result.right_minus_left == 0, (
                f"Test failed: {case.get('name')} from {spec_file}\n"
                f"Actual: {result.actual_rows}\nExpected: {result.expected_rows}"
            )


class TestSQLModelAutoColumnRepair:
    """Test that missing columns in fixtures are auto-added."""

    def test_auto_adds_missing_column(self, spark, tmp_path: Path) -> None:
        model_sql = tmp_path / "model_with_extra_col.sql"
        model_sql.write_text(
            """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.model_extra
(
    id STRING,
    name STRING
)
CLUSTER BY AUTO
AS
SELECT
    CAST(id AS STRING) AS id,
    COALESCE(name, 'unknown') AS name,
    COALESCE(description, 'none') AS description
FROM ${bronze_schema}.raw_items;
""".strip()
        )

        case = {
            "name": "auto_repair_missing_column",
            "bronze_schema": "ar_bronze",
            "silver_schema": "ar_silver",
            "gold_schema": "ar_gold",
            "model": str(model_sql),
            "given": [
                {
                    "table": "ar_bronze.raw_items",
                    "rows": [{"id": "1", "name": "Test"}],  # 'description' missing
                }
            ],
            "expect": {
                "rows": [{"id": "1", "name": "Test"}],
            },
        }

        result = run_case(spark, case)
        assert result.left_minus_right == 0
        assert result.right_minus_left == 0


class TestBundleVariableResolution:
    """Test that template variables are resolved correctly in specs."""

    def test_schema_variables_are_resolved(self, tmp_path: Path) -> None:
        project = _create_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        for _, case, _ in cases:
            # All schema variables should be resolved (no ${...} remaining)
            assert "${" not in str(case.get("given", []))
            assert case.get("bronze_schema") == "it_bronze"
            assert case.get("silver_schema") == "it_silver"
            assert case.get("gold_schema") == "it_gold"


class TestFailingSpec:
    """Test that failing specs correctly report mismatches."""

    def test_detects_missing_rows(self, spark, tmp_path: Path) -> None:
        model_sql = tmp_path / "pass_through.sql"
        model_sql.write_text(
            """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.pass_through
(
    id STRING
)
CLUSTER BY AUTO
AS
SELECT CAST(id AS STRING) AS id
FROM ${bronze_schema}.raw_data;
""".strip()
        )

        case = {
            "name": "detect_missing_rows",
            "bronze_schema": "fail_bronze",
            "silver_schema": "fail_silver",
            "gold_schema": "fail_gold",
            "model": str(model_sql),
            "given": [
                {
                    "table": "fail_bronze.raw_data",
                    "rows": [{"id": "1"}],
                }
            ],
            "expect": {
                "rows": [{"id": "1"}, {"id": "2"}],  # "2" doesn't exist in input
            },
        }

        result = run_case(spark, case)
        assert result.left_minus_right == 0  # no unexpected rows
        assert result.right_minus_left == 1  # one missing row

    def test_detects_unexpected_rows(self, spark, tmp_path: Path) -> None:
        model_sql = tmp_path / "pass_through.sql"
        model_sql.write_text(
            """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.pass_through
(
    id STRING
)
CLUSTER BY AUTO
AS
SELECT CAST(id AS STRING) AS id
FROM ${bronze_schema}.raw_data;
""".strip()
        )

        case = {
            "name": "detect_unexpected_rows",
            "bronze_schema": "fail2_bronze",
            "silver_schema": "fail2_silver",
            "gold_schema": "fail2_gold",
            "model": str(model_sql),
            "given": [
                {
                    "table": "fail2_bronze.raw_data",
                    "rows": [{"id": "1"}, {"id": "2"}, {"id": "3"}],
                }
            ],
            "expect": {
                "rows": [{"id": "1"}, {"id": "2"}],  # "3" is unexpected
            },
        }

        result = run_case(spark, case)
        assert result.left_minus_right == 1  # one unexpected row
        assert result.right_minus_left == 0  # no missing rows


class TestInlineTests:
    """Test inline tests defined directly in pipeline spec."""

    def test_inline_test_execution(self, spark, tmp_path: Path) -> None:
        (tmp_path / "databricks.yml").write_text(
            """
bundle:
  name: inline_test

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
    inline_pipe:
      name: inline_pipe
      configuration:
        bronze_schema: inl_bronze
        silver_schema: inl_silver
        gold_schema: inl_gold
      libraries: []
"""
        )

        # Create the model
        model = tmp_path / "passthrough.sql"
        model.write_text(
            """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.passthrough
(
    val STRING
)
CLUSTER BY AUTO
AS
SELECT CAST(val AS STRING) AS val FROM ${bronze_schema}.source;
""".strip()
        )

        # Pipeline spec with inline tests
        pipeline_tests = tmp_path / "pipeline_tests"
        pipeline_tests.mkdir()
        (pipeline_tests / "inline_pipeline_tests.yml").write_text(
            f"""
suite: inline_tests
log_level: DEBUG

bundle:
  file: ../databricks.yml

pipeline: pipelines.inline_pipe

tests:
  - name: inline_passthrough
    model: {model}
    given:
      - table: inl_bronze.source
        rows:
          - val: hello
    expect:
      rows:
        - val: hello
"""
        )

        cases = all_cases(
            pipeline_tests_dir=pipeline_tests,
            default_bundle_file=tmp_path / "databricks.yml",
        )
        assert len(cases) == 1

        result = run_case(spark, cases[0][1])
        assert result.left_minus_right == 0
        assert result.right_minus_left == 0
