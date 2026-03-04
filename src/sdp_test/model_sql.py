from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession


def _model_query(sql_text: str) -> str:
    """Extract the SELECT query from a Lakeflow/DDL model SQL file.

    Tries the following patterns in order:
    1. ``CLUSTER BY AUTO AS <query>`` (Lakeflow Declarative Pipeline style)
    2. ``CLUSTER BY (...) AS <query>`` (explicit clustering)
    3. Final top-level ``AS`` preceding the SELECT query in a CREATE ... VIEW/TABLE statement
    """
    # Try CLUSTER BY AUTO AS first (most common in Lakeflow).
    match = re.search(r"\bCLUSTER\s+BY\s+AUTO\s+AS\s", sql_text, flags=re.IGNORECASE)
    if not match:
        # Try CLUSTER BY (...) AS.
        match = re.search(r"\bCLUSTER\s+BY\s+[^;]+?\bAS\s", sql_text, flags=re.IGNORECASE)
    if not match:
        # Fallback: find the last top-level AS before a SELECT/WITH in a CREATE statement.
        match = re.search(r"\bAS\s+(?=(?:SELECT|WITH)\b)", sql_text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not find a query body (AS SELECT/WITH) in model SQL")
    query = sql_text[match.end() :].strip()
    if query.endswith(";"):
        query = query[:-1]
    return query


def render_model_query(model_path: str, schema_map: dict[str, str]) -> str:
    sql_text = Path(model_path).read_text()
    for key, value in schema_map.items():
        sql_text = sql_text.replace(f"${{{key}}}", value)
    return _model_query(sql_text)


def register_df_as_view(spark: SparkSession, df: DataFrame, schema_name: str, table_name: str) -> None:
    """Register a dataframe as a real table <schema>.<table> for SQL model execution."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    os.makedirs(".tmp_spark_tables", exist_ok=True)
    temp_path = tempfile.mkdtemp(prefix=f"{schema_name}_{table_name}_", dir=".tmp_spark_tables")
    spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
    (df.write.mode("overwrite").option("path", temp_path).saveAsTable(f"{schema_name}.{table_name}"))


def rows_as_dicts(df: DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    return [{col: row[col] for col in columns} for row in df.select(*columns).collect()]
