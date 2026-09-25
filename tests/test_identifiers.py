"""Tests for table identifier parsing and catalog stripping."""

from __future__ import annotations

import pytest

from sdp_test.identifiers import (
    build_identifier_map,
    catalog_of,
    fold_table_name,
    local_name_for,
    rewrite_table_references,
    local_schema_name,
    schema_of,
    local_table_name,
    split_identifier,
    strip_catalog_prefixes,
)

# ---------------------------------------------------------------------------
# split_identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("raw", (None, None, "raw")),
        ("bronze.raw", (None, "bronze", "raw")),
        ("main.bronze.raw", ("main", "bronze", "raw")),
        ("`main`.`bronze`.`raw`", ("main", "bronze", "raw")),
        ("main . bronze . raw", ("main", "bronze", "raw")),
        ("  main.bronze.raw  ", ("main", "bronze", "raw")),
        ("`my.cat`.raw", (None, "my.cat", "raw")),
    ],
)
def test_split_identifier(name: str, expected: tuple) -> None:
    assert split_identifier(name) == expected


def test_split_identifier_rejects_more_than_three_parts() -> None:
    with pytest.raises(ValueError, match="expected at most 3"):
        split_identifier("a.b.c.d")


def test_split_identifier_does_not_split_inside_backticks() -> None:
    """The one case a plain ``split(".")`` gets wrong — it would see three parts."""
    assert split_identifier("`my.cat`.raw") == (None, "my.cat", "raw")
    assert split_identifier("main.`odd.schema`.raw") == ("main", "odd.schema", "raw")
    assert local_table_name("main.`odd.schema`.raw") == "odd.schema.raw"


# ---------------------------------------------------------------------------
# catalog_of / local_table_name / schema_of
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [("raw", None), ("bronze.raw", None), ("main.bronze.raw", "main")],
)
def test_catalog_of(name: str, expected: str | None) -> None:
    assert catalog_of(name) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("raw", "raw"),
        ("bronze.raw", "bronze.raw"),
        ("main.bronze.raw", "bronze.raw"),
        ("`main`.`bronze`.`raw`", "bronze.raw"),
    ],
)
def test_local_table_name(name: str, expected: str) -> None:
    assert local_table_name(name) == expected


def test_local_table_name_is_idempotent() -> None:
    once = local_table_name("main.bronze.raw")
    assert local_table_name(once) == once


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("raw", None),
        ("bronze.raw", "bronze"),
        ("main.bronze.raw", "bronze"),
        ("`main`.`bronze`.`raw`", "bronze"),
    ],
)
def test_schema_of(table: str, expected: str | None) -> None:
    assert schema_of(table) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("bronze", "bronze"),
        ("main.bronze", "bronze"),
        ("`main`.`bronze`", "bronze"),
    ],
)
def test_local_schema_name(value: str, expected: str) -> None:
    assert local_schema_name(value) == expected


def test_schema_of_and_local_schema_name_read_two_parts_differently() -> None:
    """``main.bronze`` is schema ``main`` read as a table, schema ``bronze`` read as a schema."""
    assert schema_of("main.bronze") == "main"
    assert local_schema_name("main.bronze") == "bronze"


def test_local_schema_name_rejects_three_parts() -> None:
    with pytest.raises(ValueError, match="expected at most 2"):
        local_schema_name("main.bronze.raw")


# ---------------------------------------------------------------------------
# strip_catalog_prefixes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "sql", "catalogs", "expected"),
    [
        (
            "plain 3-part FROM",
            "SELECT * FROM main.bronze.orders",
            ["main"],
            "SELECT * FROM bronze.orders",
        ),
        (
            "keyword rule works without known catalogs",
            "SELECT * FROM main.bronze.orders",
            [],
            "SELECT * FROM bronze.orders",
        ),
        (
            "join and alias",
            "SELECT o.id FROM main.bronze.orders o JOIN main.silver.cust c ON o.id = c.id",
            ["main"],
            "SELECT o.id FROM bronze.orders o JOIN silver.cust c ON o.id = c.id",
        ),
        (
            "struct access untouched",
            "SELECT t.payload.field FROM main.bronze.t t",
            ["main"],
            "SELECT t.payload.field FROM bronze.t t",
        ),
        (
            "four-part column reference",
            "SELECT main.bronze.orders.id FROM main.bronze.orders",
            ["main"],
            "SELECT bronze.orders.id FROM bronze.orders",
        ),
        (
            "string literal untouched",
            "SELECT * FROM main.bronze.t WHERE src = 'main.bronze.t'",
            ["main"],
            "SELECT * FROM bronze.t WHERE src = 'main.bronze.t'",
        ),
        (
            "line comment untouched",
            "-- main.bronze.t\nSELECT * FROM main.bronze.t",
            ["main"],
            "-- main.bronze.t\nSELECT * FROM bronze.t",
        ),
        (
            "block comment untouched",
            "/* main.bronze.t */ SELECT * FROM main.bronze.t",
            ["main"],
            "/* main.bronze.t */ SELECT * FROM bronze.t",
        ),
        (
            "variant colon path preserved",
            "SELECT data:field.sub AS x FROM main.bronze.t",
            ["main"],
            "SELECT data:field.sub AS x FROM bronze.t",
        ),
        (
            "backticked parts",
            "SELECT * FROM `main`.`bronze`.`orders`",
            ["main"],
            "SELECT * FROM `bronze`.`orders`",
        ),
        (
            "lowercase keywords",
            "select * from main.bronze.orders",
            [],
            "select * from bronze.orders",
        ),
        (
            "newline between FROM and name",
            "SELECT *\nFROM\n  main.bronze.orders",
            [],
            "SELECT *\nFROM\n  bronze.orders",
        ),
        (
            "two-part name left alone",
            "SELECT * FROM bronze.orders",
            ["main"],
            "SELECT * FROM bronze.orders",
        ),
        (
            "unqualified name left alone",
            "SELECT * FROM orders",
            ["main"],
            "SELECT * FROM orders",
        ),
        (
            "JOIN ... USING not matched",
            "SELECT * FROM bronze.a JOIN bronze.b USING (id)",
            ["main"],
            "SELECT * FROM bronze.a JOIN bronze.b USING (id)",
        ),
        (
            "column sharing a catalog name untouched",
            "SELECT main.id FROM bronze.main main",
            ["main"],
            "SELECT main.id FROM bronze.main main",
        ),
        (
            "nested subquery",
            "SELECT * FROM (SELECT * FROM main.bronze.t) x",
            ["main"],
            "SELECT * FROM (SELECT * FROM bronze.t) x",
        ),
        (
            "unknown catalog outside FROM is left alone",
            "SELECT other.bronze.t.id FROM main.bronze.t",
            ["main"],
            "SELECT other.bronze.t.id FROM bronze.t",
        ),
    ],
)
def test_strip_catalog_prefixes(label: str, sql: str, catalogs: list[str], expected: str) -> None:
    assert strip_catalog_prefixes(sql, catalogs) == expected, label


@pytest.mark.parametrize(
    ("sql", "catalogs"),
    [
        ("SELECT * FROM main.bronze.orders", ["main"]),
        ("SELECT main.bronze.orders.id FROM main.bronze.orders", ["main"]),
        ("SELECT * FROM `main`.`bronze`.`orders`", ["main"]),
    ],
)
def test_strip_catalog_prefixes_is_idempotent(sql: str, catalogs: list[str]) -> None:
    once = strip_catalog_prefixes(sql, catalogs)
    assert strip_catalog_prefixes(once, catalogs) == once


def test_strip_catalog_prefixes_noop_below_two_dots() -> None:
    sql = "SELECT a.b FROM t"
    assert strip_catalog_prefixes(sql, ["main"]) is sql


# ---------------------------------------------------------------------------
# build_identifier_map / fold_table_name / rewrite_table_references
# ---------------------------------------------------------------------------


def test_fold_table_name() -> None:
    assert fold_table_name("cat_a.sales.orders") == "cat_a__sales.orders"
    assert fold_table_name("sales.orders") == "sales.orders"
    assert fold_table_name("orders") == "orders"


def test_build_identifier_map_strips_when_unambiguous() -> None:
    mapping = build_identifier_map(["main.bronze.raw", "main.silver.cust", "gold.summary"])
    assert mapping == {
        "main.bronze.raw": "bronze.raw",
        "main.silver.cust": "silver.cust",
        "gold.summary": "gold.summary",
    }


def test_build_identifier_map_folds_on_collision() -> None:
    mapping = build_identifier_map(["cat_a.sales.orders", "cat_b.sales.orders"])
    assert mapping == {
        "cat_a.sales.orders": "cat_a__sales.orders",
        "cat_b.sales.orders": "cat_b__sales.orders",
    }


def test_build_identifier_map_folds_only_the_colliding_names() -> None:
    """A collision must not change how unrelated fixtures are resolved."""
    mapping = build_identifier_map(["cat_a.sales.orders", "cat_b.sales.orders", "main.bronze.raw"])
    assert mapping["main.bronze.raw"] == "bronze.raw"
    assert mapping["cat_a.sales.orders"] == "cat_a__sales.orders"


def test_build_identifier_map_ignores_duplicates() -> None:
    """The same table listed twice is one fixture, not a collision."""
    assert build_identifier_map(["main.bronze.raw", "main.bronze.raw"]) == {"main.bronze.raw": "bronze.raw"}


def test_build_identifier_map_rejects_collision_without_catalog() -> None:
    with pytest.raises(ValueError, match="names no catalog"):
        build_identifier_map(["cat_a.sales.orders", "sales.orders"])


def test_local_name_for_prefers_the_map() -> None:
    mapping = {"cat_a.sales.orders": "cat_a__sales.orders"}
    assert local_name_for("cat_a.sales.orders", mapping) == "cat_a__sales.orders"
    assert local_name_for("main.bronze.raw", mapping) == "bronze.raw"
    assert local_name_for("main.bronze.raw", None) == "bronze.raw"


def test_local_name_for_is_idempotent_on_folded_names() -> None:
    mapping = {"cat_a.sales.orders": "cat_a__sales.orders"}
    once = local_name_for("cat_a.sales.orders", mapping)
    assert local_name_for(once, mapping) == once


@pytest.mark.parametrize(
    ("label", "sql", "tables", "expected"),
    [
        (
            "union across catalogs",
            "SELECT id FROM cat_a.sales.orders UNION ALL SELECT id FROM cat_b.sales.orders",
            ["cat_a.sales.orders", "cat_b.sales.orders"],
            "SELECT id FROM cat_a__sales.orders UNION ALL SELECT id FROM cat_b__sales.orders",
        ),
        (
            "no collision behaves exactly as a strip",
            "SELECT * FROM main.bronze.raw o JOIN main.silver.cust c ON o.id = c.id",
            ["main.bronze.raw", "main.silver.cust"],
            "SELECT * FROM bronze.raw o JOIN silver.cust c ON o.id = c.id",
        ),
        (
            "struct access survives folding",
            "SELECT t.payload.field FROM cat_a.sales.orders t",
            ["cat_a.sales.orders", "cat_b.sales.orders"],
            "SELECT t.payload.field FROM cat_a__sales.orders t",
        ),
        (
            "four-part column reference is folded",
            "SELECT cat_a.sales.orders.id FROM cat_a.sales.orders",
            ["cat_a.sales.orders", "cat_b.sales.orders"],
            "SELECT cat_a__sales.orders.id FROM cat_a__sales.orders",
        ),
        (
            "string literal untouched while folding",
            "SELECT * FROM cat_a.sales.orders WHERE s = 'cat_a.sales.orders'",
            ["cat_a.sales.orders", "cat_b.sales.orders"],
            "SELECT * FROM cat_a__sales.orders WHERE s = 'cat_a.sales.orders'",
        ),
        (
            "unmapped three-part name still stripped",
            "SELECT * FROM other.bronze.t",
            ["cat_a.sales.orders", "cat_b.sales.orders"],
            "SELECT * FROM bronze.t",
        ),
    ],
)
def test_rewrite_table_references(label: str, sql: str, tables: list[str], expected: str) -> None:
    mapping = build_identifier_map(tables)
    assert rewrite_table_references(sql, mapping) == expected, label


def test_rewrite_table_references_is_idempotent() -> None:
    mapping = build_identifier_map(["cat_a.sales.orders", "cat_b.sales.orders"])
    sql = "SELECT id FROM cat_a.sales.orders UNION ALL SELECT id FROM cat_b.sales.orders"
    once = rewrite_table_references(sql, mapping)
    assert rewrite_table_references(once, mapping) == once
