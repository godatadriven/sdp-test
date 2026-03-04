# sdp-test

Declarative unit testing framework for **Spark Declarative Pipelines** (Databricks Lakeflow).

Write your pipeline unit tests as YAML specs — no boilerplate Python test code needed.

## Features

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

### 1. Create a pipeline test spec

Create a pipeline test spec anywhere in your project (e.g. `tests/my_pipeline_tests.yml`):

```yaml
suite: my_pipeline_tests
log_level: DEBUG

bundle:
  file: ../databricks.yml
  variables:
    catalog: my_catalog

pipeline: pipelines.my_pipeline

defaults:
  bronze_schema: ${bronze_schema}
  silver_schema: ${silver_schema}
  gold_schema: ${gold_schema}
```

### 2. Create unit test specs (colocated with your models)

Create `src/my_pipeline/transformations/silver/stg_customers.unit_tests.yml`:

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

### 3. Run tests

That's it — no `conftest.py` or `test_*.py` needed. The pytest plugin auto-discovers your specs:

```bash
pytest
```

The plugin automatically:
- Finds all `*_pipeline_tests.yml` files in your project
- Discovers colocated `*.unit_tests.yml` specs from your pipeline's library paths
- Creates a local SparkSession and runs each test case
- Reports results with clear failure messages

### Configuration (optional)

Configure via `pyproject.toml`:

```toml
[tool.sdp-test]
bundle_file = "databricks.yml"   # default bundle file path
```

To disable the plugin (e.g. if you prefer the manual approach):

```bash
pytest -p no:sdp_test
```

## YAML Spec Format

### Pipeline Entry Spec (`*_pipeline_tests.yml`)

```yaml
suite: optional_suite_name        # Used in test IDs
log_level: DEBUG                   # DEBUG, INFO, WARN, ERROR, NONE

bundle:
  file: ../databricks.yml          # Path to bundle file (default: databricks.yml)
  target: personal                 # Bundle target (default: local)
  variables:                       # Variable overrides
    catalog: my_catalog

pipeline: pipelines.my_pipeline    # Pipeline reference

defaults:                          # Shared defaults for all tests
  bronze_schema: ${bronze_schema}
  silver_schema: ${silver_schema}
  gold_schema: ${gold_schema}

tests:                             # Optional inline tests
  - name: inline_test
    model: path/to/model.sql
    given: [...]
    expect:
      rows: [...]
```

### Unit Test Spec (`*.unit_tests.yml`)

```yaml
log_level: DEBUG

tests:
  - name: test_name
    model: my_model.sql            # .sql or .py
    callable: my_function          # For Python models (defaults to file stem)
    given:
      - table: ${bronze_schema}.raw_data
        rows:
          - col1: value1
            col2: value2
    expect:
      rows:
        - out_col1: expected1
          out_col2: expected2
```

## Python API

```python
from sdp_test import all_cases, run_case, case_id, CaseResult
from sdp_test import load_bundle_context, resolve_template
from sdp_test import PipelineEntrySpec, UnitSpec, TestCaseSpec
```

### Key functions

- `all_cases(search_dir, default_bundle_file)` — discover and load all test cases (recursively searches for `*_pipeline_tests.yml`)
- `cases_from_spec(spec_path, default_bundle_file)` — process a single spec file, returns test cases
- `run_case(spark, case)` — execute a single test case, returns `CaseResult`
- `case_id(spec_file, case)` — format a test ID for pytest parametrization
- `find_spec_files(search_dir)` — recursively find all `*_pipeline_tests.yml` files
- `load_bundle_context(bundle_file, target, variable_overrides)` — load bundle configuration

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
