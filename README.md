# sdp-test

Declarative unit testing framework for **Spark Declarative Pipelines** (Databricks Lakeflow).

Write your pipeline unit tests as YAML specs — no boilerplate Python test code needed.

## Features

- **Zero config** — install and run `pytest`; tests are discovered automatically from your pipeline definitions
- **Declarative YAML test specs** — define inputs and expected outputs, not imperative test code
- **SQL and Python model support** — test both SQL (`CLUSTER BY AUTO AS ...`) and Python (`@dp.table`) models
- **Auto-discovery** — colocate `*.unit_tests.yml` files next to your models; they're found automatically
- **Bundle-aware** — reads `databricks.yml` to resolve schemas, variables, and pipeline configuration
- **Template variables** — use `${var.catalog}`, `${bronze_schema}`, etc. in your test specs
- **Auto column repair** — missing columns in test fixtures are inferred and added automatically
- **Type coercion** — handles Decimal, Date, Timestamp conversions between YAML and Spark
- **Works with open source PySpark** (>=4.1) and Databricks PySpark

## Installation

```bash
pip install sdp-test
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add sdp-test --dev
```

## Quick Start

### 1. Write a unit test spec

Create a `*.unit_tests.yml` file next to the model you want to test. For example, given a SQL model at `src/transformations/silver/stg_customers.sql`:

Create `src/transformations/silver/stg_customers.unit_tests.yml`:

```yaml
tests:
  - name: maps_customer_fields
    model: stg_customers.sql
    given:
      - table: ${bronze_schema}.raw_customers
        rows:
          - id: "1"
            first_name: John
            last_name: Doe
    expect:
      rows:
        - customer_id: "1"
          customer_name: John Doe
```

### 2. Run tests

```bash
pytest
```

That's it. No `conftest.py`, no `test_*.py`, no pipeline test spec file needed.

The plugin automatically:
- Finds `databricks.yml` in your project root (or `spark-pipeline.yml` for open source SDP)
- Reads all pipeline definitions and resolves `configuration` variables (like `${bronze_schema}`)
- Discovers `*.unit_tests.yml` files from the pipeline's `libraries` paths
- Creates a local SparkSession and runs each test case
- Reports results with clear failure messages

## How It Works

sdp-test reads your pipeline definition to understand your project structure. A pipeline definition tells sdp-test two things:

1. **Where your models live** — the `libraries` section points to your SQL/Python files
2. **What variables to use** — the `configuration` section provides schema names and other defaults

sdp-test scans those library paths for `*.unit_tests.yml` files colocated with your models, merges in the configuration variables, and runs each test case.

## Usage

There are three ways to use sdp-test, from simplest to most configurable.

### Option A: Automatic from `databricks.yml` (recommended)

Just run:

```bash
pytest
```

sdp-test automatically finds `databricks.yml` in your project root, reads all pipeline definitions (including any `include`-ed resource files), and auto-discovers tests for each one.

Example output:

```
databricks.yml::jaffle_shop_sql::stg_products::maps_product_type_flags PASSED
databricks.yml::jaffle_shop_sql::orders::order_items_compute_to_bools  PASSED
databricks.yml::jaffle_shop_python::stg_products::maps_price_correctly PASSED
```

### Option B: Automatic from `spark-pipeline.yml` (open source SDP)

If you're using open source Spark Declarative Pipelines (without Databricks bundles), pytest auto-discovers `spark-pipeline.yml` files when they're inside the scanned tree:

```bash
pytest
```

A `spark-pipeline.yml` looks like:

```yaml
name: my_pipeline
catalog: my_catalog
database: my_db
configuration:
  bronze_schema: bronze
  silver_schema: silver
  gold_schema: gold
libraries:
  - transformations/**
```

### Option C: Using a pipeline test spec (advanced)

For more control, create a `*_pipeline_tests.yml` file that wires up a pipeline with custom variable overrides and defaults:

```yaml
suite: my_pipeline_tests
log_level: DEBUG

bundle:
  file: ../databricks.yml
  variables:
    catalog: test_catalog

pipeline: pipelines.my_pipeline

defaults:
  bronze_schema: ${bronze_schema}
  silver_schema: ${silver_schema}
  gold_schema: ${gold_schema}

tests:
  - name: inline_test
    model: path/to/model.sql
    given:
      - table: ${bronze_schema}.raw_data
        rows:
          - col1: value1
    expect:
      rows:
        - out_col: expected
```

Run it:

```bash
pytest tests/my_pipeline_tests.yml
```

This approach is useful when you need to:
- Override bundle variables for test-specific values
- Add inline tests alongside discovered ones
- Select a specific bundle target
- Set a default log level for all tests

## Writing Unit Tests

### Test file naming

Unit test files must be named `*.unit_tests.yml` (or `.yaml`) and placed next to the model they test:

```
src/transformations/silver/
    stg_customers.sql              # your model
    stg_customers.unit_tests.yml   # tests for this model
    stg_products.py                # another model
    stg_products.unit_tests.yml    # tests for this model
```

### Unit test spec format

```yaml
log_level: DEBUG                        # optional: DEBUG, INFO, WARN, ERROR, NONE

tests:
  - name: descriptive_test_name        # name shown in pytest output
    model: stg_customers.sql            # model file to test (.sql or .py)
    given:                              # input fixtures
      - table: ${bronze_schema}.raw_customers
        rows:
          - id: "1"
            first_name: John
            last_name: Doe
          - id: "2"
            first_name: Jane
            last_name: Smith
    expect:                             # expected output
      rows:
        - customer_id: "1"
          customer_name: John Doe
        - customer_id: "2"
          customer_name: Jane Smith
```

### Key rules

- **`table` must be schema-qualified** — use `schema.table_name`, e.g. `${bronze_schema}.raw_customers`
- **`model` is resolved relative to the test file** — `stg_customers.sql` looks for the file in the same directory
- **`expect.rows` checks only listed columns** — you don't need to specify every output column, only the ones you want to verify
- **One SQL statement per `.sql` file** — each file should define a single table/view (this is the SDP convention)

### Testing SQL models

A SQL model file typically looks like:

```sql
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.stg_products
CLUSTER BY AUTO
AS
SELECT
    CAST(sku AS STRING) AS product_id,
    CAST(name AS STRING) AS product_name,
    CAST(type AS STRING) AS product_type,
    CAST(ROUND(CAST(price AS DOUBLE) / 100, 2) AS DECIMAL(16, 2)) AS product_price,
    CAST(COALESCE(type = 'jaffle', FALSE) AS BOOLEAN) AS is_food_item,
    CAST(COALESCE(type = 'beverage', FALSE) AS BOOLEAN) AS is_drink_item
FROM ${bronze_schema}.raw_products;
```

sdp-test extracts the `SELECT` query (everything after `AS`), substitutes schema variables, and runs it against the test fixtures.

Unit test for this model:

```yaml
tests:
  - name: maps_product_type_flags_and_price
    model: stg_products.sql
    given:
      - table: ${bronze_schema}.raw_products
        rows:
          - sku: "1"
            name: Latte
            type: beverage
            price: 350
    expect:
      rows:
        - product_id: "1"
          product_name: Latte
          product_price: 3.50
          is_food_item: false
          is_drink_item: true
```

### Testing Python models

A Python model file uses the `@dp.table` decorator:

```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "bronze")

@dp.table(name="silver.stg_products")
def stg_products():
    return spark.read.table(f"{BRONZE_SCHEMA}.raw_products").select(
        F.col("sku").cast("string").alias("product_id"),
        F.col("name").cast("string").alias("product_name"),
        F.round(F.col("price").cast("double") / F.lit(100), 2)
            .cast("decimal(16,2)").alias("product_price"),
    )
```

Unit test:

```yaml
tests:
  - name: maps_price_correctly
    model: stg_products.py
    given:
      - table: ${bronze_schema}.raw_products
        rows:
          - sku: "1"
            name: Latte
            price: 350
    expect:
      rows:
        - product_id: "1"
          product_name: Latte
          product_price: 3.50
```

For Python files with **multiple functions**, use the `callable` field to specify which one to test:

```yaml
tests:
  - name: test_customers
    model: models.py
    callable: stg_customers      # calls the stg_customers() function
    given: [...]
    expect:
      rows: [...]
  - name: test_orders
    model: models.py
    callable: stg_orders         # calls the stg_orders() function
    given: [...]
    expect:
      rows: [...]
```

If `callable` is omitted, it defaults to the file stem (e.g. `stg_products` for `stg_products.py`).

### Testing models with joins

When a model reads from multiple tables, provide all of them in `given`:

```yaml
tests:
  - name: supply_costs_sum_correctly
    model: order_items.sql
    given:
      - table: ${silver_schema}.stg_orders
        rows:
          - order_id: 1
            customer_id: 100
          - order_id: 2
            customer_id: 200
      - table: ${silver_schema}.stg_products
        rows:
          - product_id: "1"
            product_price: 10.00
            is_food_item: true
            is_drink_item: false
      - table: ${silver_schema}.stg_supplies
        rows:
          - product_id: "1"
            supply_cost: 4.50
    expect:
      rows:
        - order_id: 1
          supply_cost: 4.50
```

### Auto column repair

If your model's SQL query references a column that isn't in your test fixture rows, sdp-test will automatically add that column with a sensible default type. This means you only need to provide the columns that are relevant to your test — not every column the model touches.

Type inference uses naming conventions:
- `is_*` columns are inferred as `BOOLEAN`
- `*_at` columns are inferred as `TIMESTAMP`
- `cost`, `price`, `amount`, `total`, `tax`, `rate` columns are inferred as `DOUBLE`
- Everything else defaults to `STRING`

### Type coercion

YAML values are automatically coerced to match Spark schema types:

| YAML value | Spark type | Result |
|---|---|---|
| `3.50` | `DecimalType` | `Decimal("3.50")` |
| `"2024-01-15"` | `DateType` | `date(2024, 1, 15)` |
| `"2024-01-15T10:30:00"` | `TimestampType` | `datetime(2024, 1, 15, 10, 30)` |

### Log levels

Control test output verbosity at three levels (most specific wins):

```yaml
# In *_pipeline_tests.yml — applies to all tests in this pipeline
log_level: DEBUG

# In *.unit_tests.yml — applies to all tests in this file
log_level: INFO

# In a test case — applies to this test only
tests:
  - name: my_test
    log_level: WARN
```

Levels: `DEBUG` (most verbose), `INFO`, `WARN`, `ERROR`, `NONE` (silent).

## Project Structure

### Databricks bundle project

```
my-project/
    databricks.yml                              # bundle definition
    resources/
        my_pipeline.pipeline.yml                # pipeline resource
    src/
        my_pipeline/
            transformations/
                bronze/
                    raw_customers.sql
                silver/
                    stg_customers.sql
                    stg_customers.unit_tests.yml     # test for stg_customers
                    stg_products.py
                    stg_products.unit_tests.yml      # test for stg_products
                gold/
                    orders.sql
                    orders.unit_tests.yml            # test for orders
```

The pipeline resource file (`resources/my_pipeline.pipeline.yml`) tells sdp-test where to find models:

```yaml
resources:
  pipelines:
    my_pipeline:
      name: my_pipeline
      catalog: ${var.catalog}
      schema: gold

      configuration:
        bronze_schema: bronze
        silver_schema: silver
        gold_schema: gold

      libraries:
        - file:
            path: ../src/my_pipeline/transformations/**/*.sql
```

Run tests:

```bash
pytest databricks.yml
```

### Open source SDP project

```
my-project/
    spark-pipeline.yml                          # pipeline definition
    transformations/
        silver/
            stg_customers.sql
            stg_customers.unit_tests.yml
        gold/
            orders.sql
            orders.unit_tests.yml
```

Run tests:

```bash
pytest spark-pipeline.yml
```

## Configuration

### pyproject.toml

```toml
[tool.sdp-test]
bundle_file = "databricks.yml"   # default bundle file path
```

### Disabling the plugin

```bash
pytest -p no:sdp_test
```

## Pipeline Test Spec Reference

The `*_pipeline_tests.yml` format provides full control over test configuration:

```yaml
suite: optional_suite_name                     # used in test IDs
log_level: DEBUG                               # DEBUG, INFO, WARN, ERROR, NONE

bundle:
  file: ../databricks.yml                      # path to bundle file
  target: personal                             # bundle target (default: local)
  variables:                                   # variable overrides
    catalog: test_catalog

pipeline: pipelines.my_pipeline                # string reference form

# Or use the object reference form:
# pipeline:
#   ref: pipelines.my_pipeline                 # same as string form
#
# Or point to a resource file:
# pipeline:
#   file: ../resources/my_pipeline.yml         # Databricks resource file
#   key: my_pipeline                           # pipeline key within the file
#
# Or point to an open source pipeline:
# pipeline:
#   file: ../spark-pipeline.yml                # open source format (no key needed)

defaults:                                      # shared defaults for all tests
  bronze_schema: ${bronze_schema}
  silver_schema: ${silver_schema}
  gold_schema: ${gold_schema}

tests:                                         # optional inline tests
  - name: inline_test
    model: path/to/model.sql
    given:
      - table: ${bronze_schema}.raw_data
        rows:
          - col1: value1
    expect:
      rows:
        - out_col: expected
```

## Template Variables

Template variables use `${...}` syntax and are resolved from the pipeline configuration and bundle context:

| Variable | Source | Example |
|---|---|---|
| `${bronze_schema}` | Pipeline `configuration` | `my_bronze` |
| `${silver_schema}` | Pipeline `configuration` | `my_silver` |
| `${gold_schema}` | Pipeline `configuration` | `my_gold` |
| `${var.catalog}` | Bundle `variables` | `my_catalog` |
| `${bundle.name}` | Bundle metadata | `my_project` |
| `${bundle.target}` | Bundle target | `personal` |

## Python API

For programmatic use:

```python
from sdp_test import cases_from_bundle, cases_from_pipeline_file, run_case, CaseResult
from sdp_test import cases_from_spec, all_cases, case_id, find_spec_files
from sdp_test import load_bundle_context, resolve_template
from sdp_test import PipelineEntrySpec, UnitSpec, TestCaseSpec
```

### Key functions

| Function | Description |
|---|---|
| `cases_from_bundle(bundle_path)` | Discover tests from a `databricks.yml` file (all pipelines) |
| `cases_from_pipeline_file(pipeline_path)` | Discover tests from a `spark-pipeline.yml` file |
| `cases_from_spec(spec_path, default_bundle_file)` | Process a `*_pipeline_tests.yml` spec file |
| `all_cases(search_dir, default_bundle_file)` | Recursively find and process all `*_pipeline_tests.yml` files |
| `run_case(spark, case)` | Execute a single test case, returns `CaseResult` |
| `case_id(spec_file, case)` | Format a test ID for pytest parametrization |
| `find_spec_files(search_dir)` | Recursively find all `*_pipeline_tests.yml` files |
| `load_bundle_context(bundle_file, target, variable_overrides)` | Load and resolve a Databricks bundle |

## Development

```bash
git clone https://github.com/godatadriven/sdp-test.git
cd sdp-test
uv sync --dev
uv run pytest
uv run ruff check src/ tests/
```

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
