from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession


def _model_query(sql_text: str) -> str:
    """Extract the query after CLUSTER BY AUTO AS from a Lakeflow model SQL file."""
    match = re.search(r"\bCLUSTER BY AUTO\s+AS\s", sql_text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not find 'CLUSTER BY AUTO AS' section in model SQL")
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
