"""Table identifier parsing and catalog normalisation.

Local open source Spark exposes a single session catalog (``spark_catalog``)
that accepts exactly one namespace part, so a three-part
``catalog.schema.table`` reference cannot be resolved:

    [REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part
    namespace, but got identifier `main`.`bronze`.`raw`

Registering a second named catalog is not possible either — the only
``TableCatalog`` in the PySpark wheel with a public no-arg constructor is
``JDBCTableCatalog``, which cannot store ``STRUCT`` or ``VARIANT`` columns.

So catalogs are *stripped*: ``main.bronze.raw`` is registered locally as
``bronze.raw``, and model SQL is rewritten to match.  A pleasant side effect is
that two-part and three-part references converge on the same local table, so a
fixture written ``${catalog}.${bronze_schema}.raw`` works against a model that
reads ``${bronze_schema}.raw``.

"""

from __future__ import annotations

import re
from typing import Iterable, Mapping

# A single identifier part: either backtick-quoted (dots inside are literal)
# or a bare SQL identifier.
_IDENT = r"(?:`[^`]*`|[A-Za-z_]\w*)"

# Spans whose contents must never be rewritten: string literals and comments.
_SKIP_RE = re.compile(
    r"""('(?:[^'\\]|\\.|'')*'
        |"(?:[^"\\]|\\.|"")*"
        |--[^\n]*
        |/\*.*?\*/)""",
    re.VERBOSE | re.DOTALL,
)

# A three-part identifier directly after FROM/JOIN.  Spark identifiers are at
# most three parts, so the leading part is always a catalog.  Group 3 keeps the
# raw ``schema . table`` text so stripping preserves backticks and spacing.
_FROM_JOIN_RE = re.compile(
    rf"(\b(?:FROM|JOIN)\s+)({_IDENT})\s*\.\s*(({_IDENT})\s*\.\s*({_IDENT}))",
    flags=re.IGNORECASE,
)

# Separator between a folded catalog and schema, e.g. ``cat_a`` + ``sales``
# becomes the local database ``cat_a__sales``.
_FOLD_SEPARATOR = "__"


def _unquote(part: str) -> str:
    """Strip surrounding backticks from a single identifier part."""
    if len(part) >= 2 and part.startswith("`") and part.endswith("`"):
        return part[1:-1]
    return part


def _parts(name: str) -> list[str]:
    """Split a dotted identifier into its parts.

    A plain split is correct unless a backtick-quoted part contains a dot
    (``\\`my.cat\\`.raw`` is two parts, not three), so only that case needs to
    match parts instead of splitting on every dot.
    """
    text = name.strip()
    if "`" not in text:
        return [part.strip() for part in text.split(".")]
    return re.findall(_IDENT, text) or [text]


def split_identifier(name: str) -> tuple[str | None, str | None, str]:
    """Split *name* into ``(catalog, schema, table)``.

    Tolerates whitespace around dots and backtick-quoted parts; dots inside
    backticks are not separators.  Backticks are stripped from the result.

    >>> split_identifier("raw")
    (None, None, 'raw')
    >>> split_identifier("bronze.raw")
    (None, 'bronze', 'raw')
    >>> split_identifier("main.bronze.raw")
    ('main', 'bronze', 'raw')
    """
    parts = [_unquote(p) for p in _parts(name)]
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ValueError(f"Table identifier has {len(parts)} parts, expected at most 3 (catalog.schema.table): {name!r}")


def catalog_of(name: str) -> str | None:
    """Return the catalog part of *name*, or ``None`` when it has fewer than three parts."""
    return split_identifier(name)[0]


def local_table_name(name: str) -> str:
    """Drop the catalog, returning the name to use against the local session catalog.

    Idempotent: a name that already has no catalog passes through unchanged.

    >>> local_table_name("main.bronze.raw")
    'bronze.raw'
    >>> local_table_name("bronze.raw")
    'bronze.raw'
    >>> local_table_name("raw")
    'raw'
    """
    _, schema, table = split_identifier(name)
    return f"{schema}.{table}" if schema else table


def schema_of(name: str) -> str | None:
    """Return the schema part of the **table identifier** *name*.

    Like :func:`catalog_of`, this reads *name* as a table, so its last part is
    the table and ``main.bronze`` means schema ``main``, table ``bronze``.
    To normalise a configured schema *value* use :func:`local_schema_name`,
    which reads the same string the other way round.

    >>> schema_of("main.bronze.raw")
    'bronze'
    >>> schema_of("bronze.raw")
    'bronze'
    >>> schema_of("raw") is None
    True
    """
    return split_identifier(name)[1]


def local_schema_name(schema: str) -> str | None:
    """Drop the catalog from a **schema value**, e.g. ``bronze_schema: main.bronze``.

    Like :func:`local_table_name`, this takes one kind of name and returns the
    same kind, normalised for the local session catalog.  *schema* names a
    schema, so its last part is the schema itself — the opposite reading from
    :func:`schema_of`.

    >>> local_schema_name("bronze")
    'bronze'
    >>> local_schema_name("main.bronze")
    'bronze'
    """
    parts = [_unquote(p) for p in _parts(schema)]
    if len(parts) > 2:
        raise ValueError(f"Schema has {len(parts)} parts, expected at most 2 (catalog.schema): {schema!r}")
    return parts[-1] if parts and parts[-1] else None


def fold_table_name(name: str) -> str:
    """Fold the catalog into the schema, keeping catalogs distinguishable locally.

    >>> fold_table_name("cat_a.sales.orders")
    'cat_a__sales.orders'
    """
    catalog, schema, table = split_identifier(name)
    if not catalog:
        return local_table_name(name)
    return f"{catalog}{_FOLD_SEPARATOR}{schema}.{table}"


def build_identifier_map(tables: Iterable[str]) -> dict[str, str]:
    """Map each declared fixture name to the name to register locally.

    A name whose stripped form is unique simply loses its catalog, which keeps
    two-part and three-part references interoperable.  Names that would collide
    keep their catalog, folded into the schema, so a model can read from two
    catalogs at once (e.g. a ``UNION`` across ``cat_a`` and ``cat_b``).

    >>> build_identifier_map(["main.bronze.raw"])
    {'main.bronze.raw': 'bronze.raw'}
    >>> sorted(build_identifier_map(["a.s.t", "b.s.t"]).items())
    [('a.s.t', 'a__s.t'), ('b.s.t', 'b__s.t')]
    """
    groups: dict[str, set[str]] = {}
    for table in tables:
        if table:
            groups.setdefault(local_table_name(table), set()).add(table)

    mapping: dict[str, str] = {}
    for local, declared in groups.items():
        if len(declared) == 1:
            mapping[next(iter(declared))] = local
            continue
        unqualified = sorted(n for n in declared if not catalog_of(n))
        if unqualified:
            # Nothing to fold, and no way to tell which catalog was meant.
            raise ValueError(
                f"Input tables {sorted(declared)} all resolve to {local!r} locally, "
                f"but {unqualified[0]!r} names no catalog so they cannot be told apart. "
                "Give every colliding fixture a catalog, or rename one."
            )
        for name in declared:
            mapping[name] = fold_table_name(name)
    return mapping


def local_name_for(name: str, mapping: Mapping[str, str] | None = None) -> str:
    """Resolve *name* to its local form, preferring *mapping* over a plain strip."""
    if mapping:
        mapped = mapping.get(name)
        if mapped is not None:
            return mapped
        lowered = {k.lower(): v for k, v in mapping.items()}
        mapped = lowered.get(name.strip().lower())
        if mapped is not None:
            return mapped
    return local_table_name(name)


def rewrite_table_references(
    sql: str,
    mapping: Mapping[str, str] | None = None,
    catalogs: Iterable[str] = (),
) -> str:
    """Rewrite catalog-qualified table references in *sql* for local execution.

    Each reference is resolved through *mapping* when present, so collided
    fixtures reach their folded names; anything else simply loses its catalog.

    Two rules, applied only outside string literals and comments:

    * a three-part identifier directly after ``FROM``/``JOIN`` — sound because
      Spark identifiers are at most three parts, so the first part is a catalog;
    * a *known* catalog name followed by at least two more dotted parts, which
      catches four-part column references such as ``main.bronze.orders.id`` and
      positions the first rule does not cover.

    Struct access (``t.payload.field``) is never touched: it has only one part
    after the first, and ``t`` is not a known catalog.

    Idempotent.
    """
    if sql.count(".") < 2:
        return sql

    lowered = {k.strip().lower(): v for k, v in (mapping or {}).items()}
    # R1 applies to catalogs named by the case as well as any seen in the map.
    known = {_unquote(c) for c in catalogs if c}
    known.update(c for c in (catalog_of(k) for k in lowered) if c)
    # Longest first so that overlapping catalog names rewrite predictably.
    ordered: list[str] = list(known)
    ordered.sort(key=len, reverse=True)

    out: list[str] = []
    last = 0
    for match in _SKIP_RE.finditer(sql):
        out.append(_rewrite_segment(sql[last : match.start()], lowered, ordered))
        out.append(match.group(0))  # literal or comment, preserved verbatim
        last = match.end()
    out.append(_rewrite_segment(sql[last:], lowered, ordered))
    return "".join(out)


def strip_catalog_prefixes(sql: str, catalogs: Iterable[str] = ()) -> str:
    """Drop catalog prefixes from table references in *sql*.

    The mapping-free form of :func:`rewrite_table_references`, where every
    three-part name simply loses its catalog.  Runtime callers pass an
    identifier map instead; this is kept as the plain-stripping entry point and
    is what the no-collision behaviour is pinned against.
    """
    return rewrite_table_references(sql, None, catalogs)


def _rewrite_segment(sql: str, lowered: dict[str, str], catalogs: list[str]) -> str:
    """Apply both rules to a span of SQL known to hold no literals or comments."""

    def _from_join(match: re.Match) -> str:
        catalog, schema, table = (_unquote(p) for p in match.group(2, 4, 5))
        mapped = lowered.get(f"{catalog}.{schema}.{table}".lower())
        # Fall back to the raw group so backticks and spacing survive a strip.
        return match.group(1) + (mapped if mapped is not None else match.group(3))

    sql = _FROM_JOIN_RE.sub(_from_join, sql)

    for catalog in catalogs:
        quoted = re.escape(catalog)
        pattern = re.compile(
            rf"(?<![\w`.])(?:`{quoted}`|{quoted})\s*\.\s*({_IDENT})(?=\s*\.\s*({_IDENT}))",
            flags=re.IGNORECASE,
        )

        def _known_catalog(match: re.Match, catalog: str = catalog) -> str:
            schema, table = _unquote(match.group(1)), _unquote(match.group(2))
            mapped = lowered.get(f"{catalog}.{schema}.{table}".lower())
            if mapped is None:
                return match.group(1)  # plain strip, raw text preserved
            # Keep only the database part; the table part is outside this match.
            return mapped.split(".", 1)[0]

        sql = pattern.sub(_known_catalog, sql)
    return sql
