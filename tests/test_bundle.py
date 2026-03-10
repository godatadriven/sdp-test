"""Unit tests for bundle module."""

from __future__ import annotations

import pytest
from sdp_test.bundle import resolve_template, load_bundle_context, _deep_merge, _lookup_path


def test_resolve_template_simple() -> None:
    result = resolve_template("${var.catalog}", {"var": {"catalog": "my_catalog"}})
    assert result == "my_catalog"


def test_resolve_template_nested_dict() -> None:
    template = {"schema": "${var.catalog}.${var.schema}"}
    context = {"var": {"catalog": "cat", "schema": "sch"}}
    result = resolve_template(template, context)
    assert result == {"schema": "cat.sch"}


def test_resolve_template_list() -> None:
    template = ["${var.a}", "${var.b}"]
    context = {"var": {"a": "1", "b": "2"}}
    result = resolve_template(template, context)
    assert result == ["1", "2"]


def test_resolve_template_passthrough() -> None:
    assert resolve_template(42, {}) == 42
    assert resolve_template(True, {}) is True
    assert resolve_template(None, {}) is None


def test_load_bundle_context(tmp_path) -> None:
    bundle_file = tmp_path / "databricks.yml"
    bundle_file.write_text(
        """
bundle:
  name: test_bundle

variables:
  catalog:
    default: test_catalog

include:
  - "resources/*.yml"
"""
    )

    resources_dir = tmp_path / "resources"
    resources_dir.mkdir()
    (resources_dir / "pipeline.yml").write_text(
        """
resources:
  pipelines:
    my_pipeline:
      name: my_pipeline
      catalog: ${var.catalog}
      configuration:
        bronze_schema: bronze
"""
    )

    context = load_bundle_context(str(bundle_file))

    assert context["bundle"]["name"] == "test_bundle"
    assert context["var"]["catalog"] == "test_catalog"
    assert context["resources"]["pipelines"]["my_pipeline"]["name"] == "my_pipeline"
    assert context["resources"]["pipelines"]["my_pipeline"]["catalog"] == "test_catalog"


def test_load_bundle_context_with_variable_overrides(tmp_path) -> None:
    bundle_file = tmp_path / "databricks.yml"
    bundle_file.write_text(
        """
bundle:
  name: test_bundle

variables:
  catalog:
    default: default_catalog
"""
    )

    context = load_bundle_context(str(bundle_file), variable_overrides={"catalog": "override_catalog"})

    assert context["var"]["catalog"] == "override_catalog"


def test_deep_merge_nested_dicts() -> None:
    base = {"a": {"x": 1, "y": 2}}
    incoming = {"a": {"y": 3, "z": 4}}
    result = _deep_merge(base, incoming)
    assert result == {"a": {"x": 1, "y": 3, "z": 4}}


def test_lookup_path_unknown_raises_key_error() -> None:
    with pytest.raises(KeyError, match="Unknown placeholder path"):
        _lookup_path({"a": 1}, "a.b.c")


def test_resolve_template_lenient_keeps_unknown_placeholder() -> None:
    result = resolve_template("${missing.var}", {}, lenient=True)
    assert result == "${missing.var}"


def test_resolve_template_complex_type_standalone() -> None:
    """A standalone placeholder resolving to a dict/list returns the raw value."""
    assert resolve_template("${nested}", {"nested": {"a": 1}}) == {"a": 1}
    assert resolve_template("${items}", {"items": [1, 2, 3]}) == [1, 2, 3]


def test_resolve_template_raises_on_non_scalar_interpolation() -> None:
    """Complex types embedded in a larger string still raise."""
    with pytest.raises(ValueError, match="Cannot interpolate non-scalar"):
        resolve_template("prefix_${nested}_suffix", {"nested": {"a": 1}})


def test_resolve_template_raises_on_unknown_placeholder() -> None:
    with pytest.raises(KeyError, match="Unknown placeholder path"):
        resolve_template("${missing.var}", {})


def test_load_bundle_context_plain_variable(tmp_path) -> None:
    """Test bundle with a plain string variable (not a dict with 'default')."""
    bundle_file = tmp_path / "databricks.yml"
    bundle_file.write_text(
        """
bundle:
  name: test_bundle

variables:
  region: us-east-1
"""
    )

    context = load_bundle_context(str(bundle_file))
    assert context["var"]["region"] == "us-east-1"


def test_load_bundle_context_variable_resolution_depth(tmp_path) -> None:
    """Test that variable_resolution_depth controls the number of resolution iterations."""
    bundle_file = tmp_path / "databricks.yml"
    # Chain: var.a -> ${var.b} -> ${var.c} -> "final"
    # Needs 3 iterations to fully resolve.
    bundle_file.write_text(
        """
bundle:
  name: test_bundle

variables:
  a:
    default: "${var.b}"
  b:
    default: "${var.c}"
  c:
    default: final
"""
    )

    # With depth=3, the chain resolves fully.
    context = load_bundle_context(str(bundle_file), variable_resolution_depth=3)
    assert context["var"]["a"] == "final"

    # With depth=1, only one iteration runs: a="${var.b}" resolves to "${var.c}".
    context = load_bundle_context(str(bundle_file), variable_resolution_depth=1)
    assert context["var"]["a"] == "${var.c}"
