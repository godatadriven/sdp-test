from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping

from .identifiers import rewrite_table_references

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# ``${catalog}.`` used as a qualifier, matched while it is still a *placeholder*.
#
# This is not the same job as the catalog stripping in ``_model_query``, which
# runs after substitution and works on real SQL identifiers.  The two only
# overlap when the pipeline actually defines a catalog; when it does not,
# ``_schema_map_from_case`` drops the key (it keeps string values only), so the
# literal text ``${catalog}`` survives substitution — and ``${catalog}`` is not
# a legal SQL identifier, so no strip rule can match it.  Removing the prefix
# here is the only thing that keeps such a model runnable.
_CATALOG_PLACEHOLDER_PREFIX_RE = re.compile(r"`?\$\{\s*catalog\s*\}`?\s*\.\s*")


def _rewrite_qualify(query: str) -> str:
    """Rewrite ``QUALIFY`` clauses for open-source Spark compatibility.

    Uses sqlglot to parse the query as Databricks SQL and transpile it to
    Spark SQL, which automatically eliminates ``QUALIFY`` by rewriting it
    into an equivalent subquery with ``WHERE``.

    Only invokes the SQL parser when the query actually contains QUALIFY.
    """
    if not re.search(r"\bQUALIFY\b", query, flags=re.IGNORECASE):
        return query

    import sqlglot

    results = sqlglot.transpile(query, read="databricks", write="spark")
    return results[0]


def _model_query(sql_text: str, *, catalogs: Iterable[str] = (), mapping: Mapping[str, str] | None = None) -> str:
    """Extract the SELECT query from a Lakeflow/DDL model SQL file.

    Also strips ``STREAM(table_ref)`` wrappers so that streaming-table
    models can be tested with regular (batch) tables locally, removes catalog
    prefixes (which the local session catalog cannot resolve), and rewrites
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
    # Resolve table references before the QUALIFY rewrite, so the transpiler
    # only ever sees (and can only ever re-emit) local one-namespace names.
    query = rewrite_table_references(query, mapping, catalogs)
    # Rewrite QUALIFY clause for open-source Spark compatibility.
    query = _rewrite_qualify(query)
    return query


def render_model_query(
    model_path: str,
    schema_map: dict[str, str],
    *,
    catalogs: Iterable[str] = (),
    mapping: Mapping[str, str] | None = None,
) -> str:
    sql_text = Path(model_path).read_text()
    # Must run before substitution: once ``${catalog}`` has a value the stripping
    # in _model_query takes over, but when it has none nothing else can remove it.
    sql_text = _CATALOG_PLACEHOLDER_PREFIX_RE.sub("", sql_text)
    for key, value in schema_map.items():
        sql_text = sql_text.replace(f"${{{key}}}", value)
    return _model_query(sql_text, catalogs=catalogs, mapping=mapping)


def register_df_as_view(spark: SparkSession, df: DataFrame, schema_name: str, table_name: str) -> None:
    """Register a dataframe as a real table <schema>.<table> for SQL model execution."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
    df.write.mode("overwrite").saveAsTable(f"{schema_name}.{table_name}")


def rows_as_dicts(df: DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    return [{col: row[col] for col in columns} for row in df.select(*columns).collect()]
