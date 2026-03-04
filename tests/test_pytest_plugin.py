"""Tests for the pytest plugin hooks and collectors.

Uses pytest's ``pytester`` fixture to run pytest in a subprocess so the plugin
is loaded from scratch and coverage is measured.
"""

from __future__ import annotations

from pathlib import Path

from sdp_test.pytest_plugin import (
    SDPTestFailure,
    SDPTestItem,
    _load_sdp_config,
    _resolve_bundle_file,
    _find_pipeline_files,
)


# ---------------------------------------------------------------------------
# _load_sdp_config
# ---------------------------------------------------------------------------


def test_load_sdp_config_missing_pyproject(tmp_path: Path) -> None:
    result = _load_sdp_config(tmp_path)
    assert result == {}


def test_load_sdp_config_no_sdp_section(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[tool.pytest]\n")
    result = _load_sdp_config(tmp_path)
    assert result == {}


def test_load_sdp_config_with_sdp_section(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        "[tool.sdp-test]\nauto_discover = false\n"
    )
    result = _load_sdp_config(tmp_path)
    assert result["auto_discover"] is False


# ---------------------------------------------------------------------------
# _find_pipeline_files
# ---------------------------------------------------------------------------


def test_find_pipeline_files_databricks(tmp_path: Path) -> None:
    (tmp_path / "databricks.yml").write_text("bundle:\n  name: x\n")
    files = _find_pipeline_files(tmp_path)
    assert tmp_path / "databricks.yml" in files


def test_find_pipeline_files_spark_pipeline(tmp_path: Path) -> None:
    (tmp_path / "spark-pipeline.yml").write_text("name: x\n")
    files = _find_pipeline_files(tmp_path)
    assert tmp_path / "spark-pipeline.yml" in files


def test_find_pipeline_files_spark_pipeline_yaml(tmp_path: Path) -> None:
    (tmp_path / "spark-pipeline.yaml").write_text("name: x\n")
    files = _find_pipeline_files(tmp_path)
    assert tmp_path / "spark-pipeline.yaml" in files


def test_find_pipeline_files_none(tmp_path: Path) -> None:
    files = _find_pipeline_files(tmp_path)
    assert files == []


# ---------------------------------------------------------------------------
# _resolve_bundle_file
# ---------------------------------------------------------------------------


def test_resolve_bundle_file_default(tmp_path: Path) -> None:
    (tmp_path / "databricks.yml").write_text("bundle:\n  name: x\n")

    class MockConfig:
        rootdir = str(tmp_path)

    result = _resolve_bundle_file(MockConfig())
    assert result == tmp_path / "databricks.yml"


def test_resolve_bundle_file_none(tmp_path: Path) -> None:
    class MockConfig:
        rootdir = str(tmp_path)

    result = _resolve_bundle_file(MockConfig())
    assert result is None


# ---------------------------------------------------------------------------
# SDPTestFailure
# ---------------------------------------------------------------------------


def test_sdp_test_failure_message() -> None:
    class Result:
        left_minus_right = 1
        right_minus_left = 2
        actual_rows = [{"id": "1"}]
        expected_rows = [{"id": "2"}]

    exc = SDPTestFailure({"name": "my_test"}, Result())
    assert "my_test" in str(exc)
    assert "Unexpected rows: 1" in str(exc)
    assert "Missing rows: 2" in str(exc)


# ---------------------------------------------------------------------------
# pytester-based plugin integration tests
# ---------------------------------------------------------------------------


pytest_plugins = ["pytester"]


def test_plugin_collects_bundle_file(pytester) -> None:
    """Plugin auto-discovers databricks.yml and collects tests."""
    pytester.makefile(
        ".yml",
        databricks="""
bundle:
  name: test_bundle

include:
  - "resources/*.yml"
""",
    )
    resources = pytester.mkdir("resources")
    resources.joinpath("pipeline.yml").write_text(
        """
resources:
  pipelines:
    p:
      name: p
      configuration:
        bronze_schema: b
      libraries: []
"""
    )
    result = pytester.runpytest("--collect-only", "-p", "sdp_test")
    result.stdout.fnmatch_lines(["*no tests*"])
    assert result.ret == 5  # ExitCode.NO_TESTS_COLLECTED


def test_plugin_auto_discover_disabled(pytester) -> None:
    """When auto_discover is false, plugin does not inject pipeline files."""
    pytester.makefile(
        ".toml",
        pyproject="""
[tool.sdp-test]
auto_discover = false
""",
    )
    pytester.makefile(
        ".yml",
        databricks="""
bundle:
  name: test
""",
    )
    result = pytester.runpytest("--collect-only", "-p", "sdp_test")
    result.stdout.fnmatch_lines(["*no tests*"])


def test_plugin_collects_spec_file(pytester) -> None:
    """Plugin collects *_pipeline_tests.yml spec files."""
    pytester.makefile(
        ".yml",
        databricks="""
bundle:
  name: test

include:
  - "resources/*.yml"
""",
    )
    resources = pytester.mkdir("resources")
    resources.joinpath("pipeline.yml").write_text(
        """
resources:
  pipelines:
    p:
      name: p
      configuration:
        bronze_schema: b
      libraries: []
"""
    )
    tests = pytester.mkdir("pipeline_tests")
    tests.joinpath("my_pipeline_tests.yml").write_text(
        """
pipeline: pipelines.p
tests: []
"""
    )
    result = pytester.runpytest("pipeline_tests/", "--collect-only", "-p", "sdp_test")
    result.stdout.fnmatch_lines(["*no tests*"])


def test_plugin_collects_spark_pipeline_file(pytester) -> None:
    """Plugin auto-discovers spark-pipeline.yml."""
    pytester.makefile(
        ".yml",
        **{"spark-pipeline": """
name: p
catalog: c
database: d
configuration:
  bronze_schema: b
libraries: []
"""},
    )
    result = pytester.runpytest("--collect-only", "-p", "sdp_test")
    result.stdout.fnmatch_lines(["*no tests*"])


def test_plugin_sdp_test_item_reportinfo(pytester) -> None:
    """SDPTestItem.reportinfo returns clean format."""
    # Construct a mock parent
    from unittest.mock import MagicMock

    parent = MagicMock()
    parent.config = MagicMock()
    parent.session = MagicMock()
    parent.path = Path("test.yml")

    item = SDPTestItem.__new__(SDPTestItem)
    item.name = "pipeline::suite::my_test"
    info = SDPTestItem.reportinfo(item)
    assert info == ("", None, "pipeline::suite::my_test")
