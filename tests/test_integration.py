"""Integration tests exercising the full sdp-test flow with open source PySpark.

These tests create complete mini-projects on disk (bundle, pipeline config,
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


def _create_lakeflow_project(tmp_path: Path) -> Path:
    """Scaffold a Databricks Lakeflow-style project with bronze/silver/gold layers.

    This mirrors the jaffle_shop pattern from lakeflow-foundation with:
    - Multi-layer SQL transformations (bronze → silver → gold)
    - Python models using @dp.table decorators
    - Schema variables resolved from bundle configuration
    - Multiple input tables with joins
    - Type coercion (dates, decimals, booleans)
    """

    # databricks.yml
    (tmp_path / "databricks.yml").write_text(
        """
bundle:
  name: jaffle_shop

variables:
  catalog:
    default: jaffle_catalog

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
    jaffle_bronze:
      name: jaffle_shop_bronze
    jaffle_silver:
      name: jaffle_shop_silver
    jaffle_gold:
      name: jaffle_shop_gold
"""
    )

    (resources / "pipeline.yml").write_text(
        """
resources:
  pipelines:
    jaffle_shop:
      name: jaffle_shop
      catalog: ${var.catalog}
      schema: ${resources.schemas.jaffle_gold.name}
      configuration:
        bronze_schema: ${resources.schemas.jaffle_bronze.name}
        silver_schema: ${resources.schemas.jaffle_silver.name}
        gold_schema: ${resources.schemas.jaffle_gold.name}
      libraries:
        - glob:
            include: ../src/jaffle_shop/transformations/**
"""
    )

    # --- Silver layer: SQL models ---
    silver_dir = tmp_path / "src" / "jaffle_shop" / "transformations" / "silver"
    silver_dir.mkdir(parents=True)

    # stg_locations.sql — Silver location model (date truncation, type casting)
    (silver_dir / "stg_locations.sql").write_text(
        """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.stg_locations
(
    location_id STRING NOT NULL PRIMARY KEY RELY COMMENT 'Canonical location identifier.',
    location_name STRING COMMENT 'Cleaned location name.',
    tax_rate DOUBLE COMMENT 'Location tax rate as numeric value.',
    opened_date DATE COMMENT 'Store opening date.'
)
COMMENT 'Locations with basic cleaning applied, one row per location.'
TBLPROPERTIES (
    'layer' = 'silver',
    'dbt_model' = 'stg_locations'
)
CLUSTER BY AUTO
AS
SELECT
    CAST(id AS STRING) AS location_id,
    CAST(name AS STRING) AS location_name,
    CAST(tax_rate AS DOUBLE) AS tax_rate,
    CAST(TO_DATE(opened_at) AS DATE) AS opened_date
FROM ${bronze_schema}.raw_stores;
"""
    )

    # stg_locations unit tests
    (silver_dir / "stg_locations.unit_tests.yml").write_text(
        """
tests:
  - name: truncates_opened_at_to_date
    model: stg_locations.sql
    given:
      - table: ${bronze_schema}.raw_stores
        rows:
          - id: "1"
            name: Vice City
            tax_rate: 0.2
            opened_at: "2016-09-01T00:00:00"
          - id: "2"
            name: San Andreas
            tax_rate: 0.1
            opened_at: "2079-10-27T23:59:59.9999"
    expect:
      rows:
        - location_id: "1"
          location_name: Vice City
          tax_rate: 0.2
          opened_date: "2016-09-01"
        - location_id: "2"
          location_name: San Andreas
          tax_rate: 0.1
          opened_date: "2079-10-27"
"""
    )

    # --- Silver layer: Python model (using @dp.table decorator) ---

    # stg_products.py — Python model with @dp.table decorator
    (silver_dir / "stg_products.py").write_text(
        """
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "jaffle_shop_bronze")
SILVER_SCHEMA = spark.conf.get("silver_schema", "jaffle_shop_silver")


@dp.table(
    cluster_by_auto=True,
    name=f"{SILVER_SCHEMA}.stg_products",
    comment="Product data with cleaning and transformation, one row per product.",
    table_properties={"layer": "silver", "dbt_model": "stg_products"},
)
def stg_products():
    return spark.read.table(f"{BRONZE_SCHEMA}.raw_products").select(
        F.col("sku").cast("string").alias("product_id"),
        F.col("name").cast("string").alias("product_name"),
        F.col("type").cast("string").alias("product_type"),
        F.round(F.col("price").cast("double") / F.lit(100), 2).cast("decimal(16,2)").alias("product_price"),
        F.coalesce((F.col("type") == F.lit("jaffle")), F.lit(False)).cast("boolean").alias("is_food_item"),
        F.coalesce((F.col("type") == F.lit("beverage")), F.lit(False)).cast("boolean").alias("is_drink_item"),
    )
"""
    )

    # stg_products unit tests
    (silver_dir / "stg_products.unit_tests.yml").write_text(
        """
tests:
  - name: maps_product_type_flags_and_price
    model: stg_products.py
    given:
      - table: ${bronze_schema}.raw_products
        rows:
          - sku: "1"
            name: Veggie Jaffle
            type: jaffle
            price: "900"
          - sku: "2"
            name: Cola
            type: beverage
            price: "350"
          - sku: "3"
            name: Mystery Item
            type: merch
            price: "1299"
    expect:
      rows:
        - product_id: "1"
          product_price: 9.00
          is_food_item: true
          is_drink_item: false
        - product_id: "2"
          product_price: 3.50
          is_food_item: false
          is_drink_item: true
        - product_id: "3"
          product_price: 12.99
          is_food_item: false
          is_drink_item: false
"""
    )

    # --- Gold layer: SQL model with joins and aggregation ---
    gold_dir = tmp_path / "src" / "jaffle_shop" / "transformations" / "gold"
    gold_dir.mkdir(parents=True)

    # order_items.sql — Gold model joining products with order items and computing supply costs
    (gold_dir / "order_items.sql").write_text(
        """
CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.order_items
(
    order_item_id STRING NOT NULL PRIMARY KEY RELY,
    order_id BIGINT,
    product_id BIGINT,
    product_name STRING,
    product_price DECIMAL(16,2),
    supply_cost DECIMAL(38,2),
    is_food_item BOOLEAN,
    is_drink_item BOOLEAN
)
COMMENT 'Order items enriched with product details and aggregated supply costs.'
TBLPROPERTIES ('layer' = 'gold')
CLUSTER BY AUTO
AS
WITH supply_costs AS (
    SELECT
        product_id,
        CAST(SUM(supply_cost) AS DECIMAL(38,2)) AS supply_cost
    FROM ${silver_schema}.stg_supplies
    GROUP BY product_id
)
SELECT
    CAST(oi.order_item_id AS STRING) AS order_item_id,
    oi.order_id,
    oi.product_id,
    p.product_name,
    p.product_price,
    sc.supply_cost,
    p.is_food_item,
    p.is_drink_item
FROM ${silver_schema}.stg_order_items oi
JOIN ${silver_schema}.stg_products p ON oi.product_id = p.product_id
LEFT JOIN supply_costs sc ON oi.product_id = sc.product_id;
"""
    )

    # order_items unit tests
    (gold_dir / "order_items.unit_tests.yml").write_text(
        """
tests:
  - name: supply_costs_sum_correctly
    model: order_items.sql
    given:
      - table: ${silver_schema}.stg_supplies
        rows:
          - product_id: 1
            supply_cost: 4.50
          - product_id: 2
            supply_cost: 3.50
          - product_id: 2
            supply_cost: 5.00
      - table: ${silver_schema}.stg_products
        rows:
          - product_id: 1
            product_name: Veggie Jaffle
            product_price: 9.00
            is_food_item: true
            is_drink_item: false
          - product_id: 2
            product_name: Cola
            product_price: 3.50
            is_food_item: false
            is_drink_item: true
      - table: ${silver_schema}.stg_order_items
        rows:
          - order_item_id: 1
            order_id: 1
            product_id: 1
          - order_item_id: 2
            order_id: 2
            product_id: 2
      - table: ${silver_schema}.stg_orders
        rows:
          - order_id: 1
            ordered_at: "2024-01-01T00:00:00"
          - order_id: 2
            ordered_at: "2024-01-02T00:00:00"
    expect:
      rows:
        - order_item_id: "1"
          order_id: 1
          product_id: 1
          supply_cost: 4.50
        - order_item_id: "2"
          order_id: 2
          product_id: 2
          supply_cost: 8.50
"""
    )

    # Pipeline test entry spec
    pipeline_tests = tmp_path / "pipeline_tests"
    pipeline_tests.mkdir()

    (pipeline_tests / "jaffle_shop_pipeline_tests.yml").write_text(
        """
suite: jaffle_shop_pipeline_tests
log_level: DEBUG

bundle:
  file: ../databricks.yml
  variables:
    catalog: jaffle_catalog

pipeline: pipelines.jaffle_shop
"""
    )

    return tmp_path


class TestLakeflowJaffleShop:
    """End-to-end tests modeled after the jaffle_shop Lakeflow Declarative Pipeline.

    Covers:
    - Multi-layer transformations (bronze → silver → gold)
    - SQL models with CLUSTER BY AUTO, type casting, date truncation
    - Python models using @dp.table decorators (shimmed for local testing)
    - Gold-layer joins across multiple silver tables
    - Aggregated supply costs
    - Template variable resolution from bundle configuration
    - Boolean flag computation
    - Decimal type coercion
    """

    def test_discovers_all_jaffle_shop_specs(self, tmp_path: Path) -> None:
        project = _create_lakeflow_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        # 1 SQL silver + 1 Python silver + 1 SQL gold = 3
        assert len(cases) == 3
        names = sorted(case.get("name", "unnamed") for _, case, _ in cases)
        assert names == [
            "maps_product_type_flags_and_price",
            "supply_costs_sum_correctly",
            "truncates_opened_at_to_date",
        ]

    def test_schema_variables_resolved_from_bundle(self, tmp_path: Path) -> None:
        project = _create_lakeflow_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        for _, case, _ in cases:
            assert case["bronze_schema"] == "jaffle_shop_bronze"
            assert case["silver_schema"] == "jaffle_shop_silver"
            assert case["gold_schema"] == "jaffle_shop_gold"
            assert case["catalog"] == "jaffle_catalog"

    def test_silver_sql_stg_locations(self, spark, tmp_path: Path) -> None:
        """Test silver SQL model: date truncation and type casting."""
        project = _create_lakeflow_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        stg_locations = [c for _, c, _ in cases if c.get("name") == "truncates_opened_at_to_date"]
        assert len(stg_locations) == 1

        result = run_case(spark, stg_locations[0])
        assert result.left_minus_right == 0 and result.right_minus_left == 0

    def test_silver_python_stg_products(self, spark, tmp_path: Path) -> None:
        """Test Python model with @dp.table decorator, price conversion, boolean flags."""
        project = _create_lakeflow_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        stg_products = [c for _, c, _ in cases if c.get("name") == "maps_product_type_flags_and_price"]
        assert len(stg_products) == 1

        result = run_case(spark, stg_products[0])
        assert result.left_minus_right == 0 and result.right_minus_left == 0

    def test_gold_sql_order_items_with_joins(self, spark, tmp_path: Path) -> None:
        """Test gold SQL model: multi-table joins and aggregated supply costs."""
        project = _create_lakeflow_project(tmp_path)
        cases = all_cases(
            pipeline_tests_dir=project / "pipeline_tests",
            default_bundle_file=project / "databricks.yml",
        )

        order_items = [c for _, c, _ in cases if c.get("name") == "supply_costs_sum_correctly"]
        assert len(order_items) == 1

        result = run_case(spark, order_items[0])
        assert result.left_minus_right == 0 and result.right_minus_left == 0


def _create_opensource_sdp_project(tmp_path: Path) -> Path:
    """Scaffold a project using the open source Spark Declarative Pipelines format.

    Uses spark-pipeline.yml (no databricks.yml bundle) with:
    - Top-level pipeline definition (name, libraries, catalog, database, configuration)
    - Library paths relative to the pipeline spec file
    - SQL and Python models
    """

    # spark-pipeline.yml (open source SDP format)
    (tmp_path / "spark-pipeline.yml").write_text(
        """
name: my_pipeline
catalog: my_catalog
database: my_db
libraries:
  - glob:
      include: transformations/**
configuration:
  bronze_schema: oss_bronze
  silver_schema: oss_silver
  gold_schema: oss_gold
"""
    )

    # SQL model
    silver_dir = tmp_path / "transformations" / "silver"
    silver_dir.mkdir(parents=True)

    (silver_dir / "stg_customers.sql").write_text(
        """
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.stg_customers
(
    customer_id STRING,
    full_name STRING
)
CLUSTER BY AUTO
AS
SELECT
    CAST(id AS STRING) AS customer_id,
    CONCAT(first_name, ' ', last_name) AS full_name
FROM ${bronze_schema}.raw_customers;
"""
    )

    (silver_dir / "stg_customers.unit_tests.yml").write_text(
        """
tests:
  - name: concatenates_first_and_last_name
    model: stg_customers.sql
    given:
      - table: ${bronze_schema}.raw_customers
        rows:
          - id: "1"
            first_name: John
            last_name: Doe
          - id: "2"
            first_name: Jane
            last_name: Smith
    expect:
      rows:
        - customer_id: "1"
          full_name: John Doe
        - customer_id: "2"
          full_name: Jane Smith
"""
    )

    # Python model
    gold_dir = tmp_path / "transformations" / "gold"
    gold_dir.mkdir(parents=True)

    (gold_dir / "customer_count.py").write_text(
        """
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
SILVER_SCHEMA = spark.conf.get("silver_schema")

def customer_count():
    return (
        spark.read.table(f"{SILVER_SCHEMA}.stg_customers")
        .agg(F.count("*").alias("total_customers"))
    )
"""
    )

    (gold_dir / "customer_count.unit_tests.yml").write_text(
        """
tests:
  - name: counts_customers
    model: customer_count.py
    given:
      - table: ${silver_schema}.stg_customers
        rows:
          - customer_id: "1"
            full_name: John Doe
          - customer_id: "2"
            full_name: Jane Smith
          - customer_id: "3"
            full_name: Bob Jones
    expect:
      rows:
        - total_customers: 3
"""
    )

    # Pipeline test spec (references spark-pipeline.yml directly, no bundle)
    pipeline_tests = tmp_path / "pipeline_tests"
    pipeline_tests.mkdir()

    (pipeline_tests / "my_pipeline_tests.yml").write_text(
        """
suite: opensource_sdp_tests
log_level: DEBUG

pipeline:
  file: ../spark-pipeline.yml
"""
    )

    return tmp_path


class TestOpenSourceSDP:
    """Tests using the open source Spark Declarative Pipelines format (spark-pipeline.yml).

    Validates that sdp-test works without a Databricks bundle, using the open source
    pipeline spec format with top-level name, libraries, catalog, database, and
    configuration fields.
    """

    def test_discovers_specs_from_spark_pipeline_yml(self, tmp_path: Path) -> None:
        project = _create_opensource_sdp_project(tmp_path)
        cases = all_cases(pipeline_tests_dir=project / "pipeline_tests")

        assert len(cases) == 2
        names = sorted(case.get("name", "unnamed") for _, case, _ in cases)
        assert names == ["concatenates_first_and_last_name", "counts_customers"]

    def test_resolves_configuration_as_defaults(self, tmp_path: Path) -> None:
        project = _create_opensource_sdp_project(tmp_path)
        cases = all_cases(pipeline_tests_dir=project / "pipeline_tests")

        for _, case, _ in cases:
            assert case.get("bronze_schema") == "oss_bronze"
            assert case.get("silver_schema") == "oss_silver"
            assert case.get("gold_schema") == "oss_gold"

    def test_resolves_database_as_pipeline_schema(self, tmp_path: Path) -> None:
        project = _create_opensource_sdp_project(tmp_path)
        cases = all_cases(pipeline_tests_dir=project / "pipeline_tests")

        for _, case, _ in cases:
            assert case.get("pipeline_schema") == "my_db"
            assert case.get("catalog") == "my_catalog"
            assert case.get("pipeline_name") == "my_pipeline"

    def test_sql_model_passes(self, spark, tmp_path: Path) -> None:
        """Test SQL model in open source SDP project."""
        project = _create_opensource_sdp_project(tmp_path)
        cases = all_cases(pipeline_tests_dir=project / "pipeline_tests")

        sql_case = [c for _, c, _ in cases if c.get("name") == "concatenates_first_and_last_name"]
        assert len(sql_case) == 1

        result = run_case(spark, sql_case[0])
        assert result.left_minus_right == 0 and result.right_minus_left == 0

    def test_python_model_passes(self, spark, tmp_path: Path) -> None:
        """Test Python model in open source SDP project."""
        project = _create_opensource_sdp_project(tmp_path)
        cases = all_cases(pipeline_tests_dir=project / "pipeline_tests")

        py_case = [c for _, c, _ in cases if c.get("name") == "counts_customers"]
        assert len(py_case) == 1

        result = run_case(spark, py_case[0])
        assert result.left_minus_right == 0 and result.right_minus_left == 0
