"""No-op shim for ``pyspark.pipelines`` decorators and functions.

When running pipeline models locally under pytest there is no active
pipeline runtime, so the real ``pyspark.pipelines`` decorators would
raise ``GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE``.

This module provides pass-through replacements so that model files can
be imported and their functions called directly to obtain a DataFrame.
"""

from __future__ import annotations

from typing import Any, Callable, TypeVar

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Decorator shims — return the decorated function unchanged
# ---------------------------------------------------------------------------

def _noop_decorator(*args: Any, **kwargs: Any) -> Callable[[T], T] | T:
    """Decorator that accepts any arguments and returns the function as-is.

    Handles both ``@decorator`` and ``@decorator(...)`` call styles.
    """
    if len(args) == 1 and callable(args[0]) and not kwargs:
        # Called without parentheses: @table
        return args[0]

    # Called with parentheses: @table(name="...", ...)
    def _inner(func: T) -> T:
        return func

    return _inner


# Core decorators (open source PySpark 4.1+)
table = _noop_decorator
materialized_view = _noop_decorator
temporary_view = _noop_decorator
append_flow = _noop_decorator
view = temporary_view  # alias

# Expectation decorators
expect = _noop_decorator
expect_or_drop = _noop_decorator
expect_or_fail = _noop_decorator

# Sink decorator
foreach_batch_sink = _noop_decorator


# ---------------------------------------------------------------------------
# Function shims — silently do nothing
# ---------------------------------------------------------------------------

def create_streaming_table(*args: Any, **kwargs: Any) -> None:
    """No-op replacement for ``dp.create_streaming_table(...)``."""


def create_sink(*args: Any, **kwargs: Any) -> None:
    """No-op replacement for ``dp.create_sink(...)``."""


def apply_changes(*args: Any, **kwargs: Any) -> None:
    """No-op replacement for ``dp.apply_changes(...)``."""


create_auto_cdc_flow = apply_changes  # alias


def create_auto_cdc_from_snapshot_flow(*args: Any, **kwargs: Any) -> None:
    """No-op replacement for ``dp.create_auto_cdc_from_snapshot_flow(...)``."""
