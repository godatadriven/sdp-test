"""Unit tests for bundle module."""

from __future__ import annotations

from sdp_test.bundle import resolve_template, load_bundle_context


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
