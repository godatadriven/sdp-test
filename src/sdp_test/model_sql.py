from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _rewrite_qualify(query: str) -> str:
    """Rewrite ``QUALIFY <expr> <op> <value>`` into a subquery with ``WHERE``.

    Open-source Spark does not support the ``QUALIFY`` clause.  This function
    emulates it by wrapping the original query in a subquery, projecting the
    qualify expression as a hidden column, and filtering with ``WHERE``.
    """
    # Flatten nested parentheses by replacing innermost (...) groups with
    # spaces of equal length.  This preserves character positions so that a
    # simple regex can find top-level QUALIFY without a manual depth loop.
    flattened = query
    while True:
        replaced = re.sub(r"\([^()]*\)", lambda m: " " * len(m.group()), flattened)
        if replaced == flattened:
            break
        flattened = replaced

    match = re.search(r"\bQUALIFY\b", flattened, flags=re.IGNORECASE)
    if match is None:
        return query

    inner_query = query[: match.start()].rstrip()
    qualify_expr = query[match.end() :].strip()

    return (
        f"SELECT * FROM (\n"
        f"  SELECT *, {qualify_expr} AS _sdp_qualify_cond\n"
        f"  FROM (\n"
        f"    {inner_query}\n"
        f"  ) _sdp_inner\n"
        f") _sdp_qualified\n"
        f"WHERE _sdp_qualify_cond"
    )


def _model_query(sql_text: str) -> str:
    """Extract the SELECT query from a Lakeflow/DDL model SQL file.

    Also strips ``STREAM(table_ref)`` wrappers so that streaming-table
    models can be tested with regular (batch) tables locally, and rewrites
    ``QUALIFY`` clauses for open-source Spark compatibility.
    """
    match = re.search(r"\bAS\s+(?=(?:SELECT|WITH)\b)", sql_text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not find a query body (AS SELECT/WITH) in model SQL")
    query = sql_text[match.end() :].strip()
    if query.endswith(";"):
        query = query[:-1]
    # Replace STREAM(table_ref) with just table_ref for local batch execution.
    query = re.sub(r"\bSTREAM\s*\(([^)]+)\)", r"\1", query, flags=re.IGNORECASE)
    # Rewrite QUALIFY clause for open-source Spark compatibility.
    query = _rewrite_qualify(query)
    return query


def render_model_query(model_path: str, schema_map: dict[str, str]) -> str:
    sql_text = Path(model_path).read_text()
    for key, value in schema_map.items():
        sql_text = sql_text.replace(f"${{{key}}}", value)
    return _model_query(sql_text)


def register_df_as_view(spark: SparkSession, df: DataFrame, schema_name: str, table_name: str) -> None:
    """Register a dataframe as a real table <schema>.<table> for SQL model execution."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
    df.write.mode("overwrite").saveAsTable(f"{schema_name}.{table_name}")


def rows_as_dicts(df: DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    return [{col: row[col] for col in columns} for row in df.select(*columns).collect()]
