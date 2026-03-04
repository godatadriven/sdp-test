# sdp-test

Declarative unit testing for [Spark Declarative Pipelines (SDP)](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html) and [Lakeflow Declarative Pipelines (LDP)](https://docs.databricks.com/aws/en/ldp/).

Write pipeline tests as YAML and run them locally with pytest — no boilerplate Python test code, no remote cluster or Databricks workspace required.

```yaml
# stg_customers.unit_tests.yml
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

```
$ pytest
jaffle_shop_sql::stg_customers::maps_customer_fields  PASSED
```

## Installation

```bash
pip install sdp-test
```

PySpark is **not** bundled — install it separately or use the extra:

```bash
pip install "sdp-test[spark]"           # includes pyspark[pipelines]>=4.1
pip install "pyspark>=4.1"              # or install open source PySpark yourself
```

## Quick start

**1.** Place a `*.unit_tests.yml` file next to the model it tests:

```
src/transformations/silver/
    stg_customers.sql
    stg_customers.unit_tests.yml   # <-- test file
```

**2.** Run pytest:

```bash
pytest
```

sdp-test automatically discovers your pipeline definition (`databricks.yml` or `spark-pipeline.yml`), resolves configuration variables, finds all `*.unit_tests.yml` files, and runs them.

## How it works

sdp-test reads your pipeline definition to learn two things:

1. **Where models live** — the `libraries` paths
2. **What variables to use** — the `configuration` section (schema names, etc.)

It scans those paths for `*.unit_tests.yml` files, substitutes variables, and runs each test case against a local SparkSession. For SQL models it strips the DDL preamble (`CREATE OR REFRESH MATERIALIZED VIEW … AS`) and executes only the `SELECT` query. For Python models it shims the pipeline decorators (`@dp.table`, `@dp.view`, etc.) so your model functions run without a live pipeline or remote connection.

## Writing tests

### Unit test format

```yaml
tests:
  - name: descriptive_test_name
    model: stg_customers.sql          # model file (.sql or .py)
    given:                            # input fixtures
      - table: ${bronze_schema}.raw_customers
        rows:
          - id: "1"
            first_name: John
    expect:                           # expected output
      rows:
        - customer_id: "1"
          customer_name: John
```

### Key rules

- **`table` must be schema-qualified** — e.g. `${bronze_schema}.raw_customers`
- **`model` is relative to the test file** — `stg_customers.sql` resolves from the same directory
- **Only listed columns are checked** — you don't need to specify every output column
- **One model per `.sql` file** — the SDP convention

### SQL models

Given a SQL model:

```sql
CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.stg_products AS
SELECT
    CAST(sku AS STRING) AS product_id,
    CAST(ROUND(CAST(price AS DOUBLE) / 100, 2) AS DECIMAL(16, 2)) AS product_price,
    CAST(COALESCE(type = 'beverage', FALSE) AS BOOLEAN) AS is_drink_item
FROM ${bronze_schema}.raw_products;
```

Test it:

```yaml
tests:
  - name: maps_product_type_flags_and_price
    model: stg_products.sql
    given:
      - table: ${bronze_schema}.raw_products
        rows:
          - sku: "1"
            type: beverage
            price: 350
    expect:
      rows:
        - product_id: "1"
          product_price: 3.50
          is_drink_item: true
```

### Python models

Given a Python model:

```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

@dp.table(name="stg_products")
def stg_products():
    return spark.read.table(f"{spark.conf.get('bronze_schema')}.raw_products").select(
        F.col("sku").cast("string").alias("product_id"),
        F.round(F.col("price").cast("double") / 100, 2).cast("decimal(16,2)").alias("product_price"),
    )
```

Test it:

```yaml
tests:
  - name: maps_price_correctly
    model: stg_products.py
    given:
      - table: ${bronze_schema}.raw_products
        rows:
          - sku: "1"
            price: 350
    expect:
      rows:
        - product_id: "1"
          product_price: 3.50
```

For files with multiple functions, use `callable` to select which one to run:

```yaml
tests:
  - name: test_customers
    model: models.py
    callable: stg_customers
```

### Models with joins

Provide all source tables in `given`:

```yaml
tests:
  - name: supply_costs_sum_correctly
    model: order_items.sql
    given:
      - table: ${silver_schema}.stg_order_items
        rows:
          - order_id: 1
            product_id: "1"
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

If your model references a column not in the test fixtures, sdp-test adds it automatically with an inferred type:

| Column pattern | Inferred type |
|---|---|
| Used in `CASE WHEN <col>` | `BOOLEAN` |
| Used in `SUM(<col>)` | `DOUBLE` |
| `is_*` | `BOOLEAN` |
| Everything else | `STRING` |

### Type coercion

YAML values are coerced to match the Spark schema:

| YAML | Spark type | Result |
|---|---|---|
| `3.50` | `DecimalType` | `Decimal("3.50")` |
| `"2024-01-15"` | `DateType` | `date(2024, 1, 15)` |
| `"2024-01-15T10:30:00"` | `TimestampType` | `datetime(2024, 1, 15, 10, 30)` |

## Pipeline definitions

sdp-test supports two pipeline formats. Both are auto-discovered from the project root.

### Databricks bundle (`databricks.yml`)

```yaml
# resources/my_pipeline.pipeline.yml
resources:
  pipelines:
    my_pipeline:
      name: my_pipeline
      catalog: my_catalog
      schema: gold
      configuration:
        bronze_schema: bronze
        silver_schema: silver
        gold_schema: gold
      libraries:
        - file:
            path: ../src/transformations/**/*.sql
```

### Open source SDP (`spark-pipeline.yml`)

```yaml
name: my_pipeline
catalog: my_catalog
database: gold
configuration:
  bronze_schema: bronze
  silver_schema: silver
  gold_schema: gold
libraries:
  - transformations/**
```

## Configuration

### `pyproject.toml`

```toml
[tool.sdp-test]
auto_discover = true    # default: true

[tool.pytest.ini_options]
testpaths = [
    "tests",
    "spark-pipeline.yml",              # collect a specific pipeline file
    "resources/my_pipeline.pipeline.yml",
]
```

| Option | Default | Description |
|---|---|---|
| `auto_discover` | `true` | Auto-discover `databricks.yml` and `spark-pipeline.yml` from the project root |

To collect specific pipeline files, add them to `testpaths` in `[tool.pytest.ini_options]` — or pass them as CLI arguments (`pytest spark-pipeline.yml`).

### Logging

sdp-test uses Python's `logging` module under the `sdp_test` logger. Control verbosity with pytest's standard settings:

```toml
[tool.pytest.ini_options]
log_cli = true
log_cli_level = "DEBUG"    # shows detailed test execution info
```

### Disabling the plugin

```bash
pytest -p no:sdp_test
```

## Advanced: pipeline test specs

For full control over bundle resolution and variable overrides, create a `*_pipeline_tests.yml` file:

```yaml
bundle:
  file: ../databricks.yml
  target: dev
  variables:
    catalog: test_catalog

pipeline: pipelines.my_pipeline

defaults:
  bronze_schema: ${bronze_schema}
  silver_schema: ${silver_schema}

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

The `pipeline` field supports several forms:

```yaml
# String reference to a bundle pipeline
pipeline: pipelines.my_pipeline

# Object reference
pipeline:
  ref: pipelines.my_pipeline

# Databricks resource file
pipeline:
  file: ../resources/my_pipeline.yml
  key: my_pipeline

# Open source pipeline file
pipeline:
  file: ../spark-pipeline.yml
```

## Template variables

Variables use `${...}` syntax and resolve from the pipeline configuration and bundle context:

| Variable | Source |
|---|---|
| `${bronze_schema}` | Pipeline `configuration` |
| `${var.catalog}` | Bundle `variables` |
| `${bundle.target}` | Bundle metadata |

## Project structure

```
my-project/
    databricks.yml
    resources/
        my_pipeline.pipeline.yml
    src/
        transformations/
            silver/
                stg_customers.sql
                stg_customers.unit_tests.yml
                stg_products.py
                stg_products.unit_tests.yml
            gold/
                orders.sql
                orders.unit_tests.yml
```

## Development

```bash
git clone https://github.com/godatadriven/sdp-test.git
cd sdp-test
uv sync --dev
uv run pytest
uv run ruff check src/ tests/
```

## License

Apache License 2.0
