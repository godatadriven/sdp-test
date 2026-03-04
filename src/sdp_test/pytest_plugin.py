"""Pytest plugin for automatic SDP test collection.

Registers as a pytest plugin via the ``pytest11`` entry point. When installed,
tests are discovered automatically from pipeline definitions — no conftest.py
or test_*.py boilerplate required.

The plugin collects tests from three file types:
    - ``databricks.yml``           — Databricks Asset Bundle (all pipelines)
    - ``spark-pipeline.yml``       — open source Spark Declarative Pipeline
    - ``*_pipeline_tests.yml``     — explicit pipeline test spec

Auto-discovery: when a ``databricks.yml`` exists in the project root, the
plugin automatically includes it in pytest's collection — even when
``testpaths`` points elsewhere.  No configuration needed.

Disable with ``pytest -p no:sdp_test`` if you prefer the manual approach.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from .spec_runner import cases_from_bundle, cases_from_pipeline_file, cases_from_spec, run_case


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class SDPTestFailure(Exception):
    """Raised when a pipeline test case does not match expected output."""

    def __init__(self, case: dict[str, Any], result):
        self.case = case
        self.result = result
        name = case.get("name", "unnamed")
        super().__init__(
            f"Test '{name}' failed:\n"
            f"  Unexpected rows: {result.left_minus_right}\n"
            f"  Missing rows: {result.right_minus_left}\n"
            f"  Actual: {result.actual_rows}\n"
            f"  Expected: {result.expected_rows}"
        )


def _load_sdp_config(rootdir: Path) -> dict[str, Any]:
    """Load ``[tool.sdp-test]`` from pyproject.toml."""
    pyproject = rootdir / "pyproject.toml"
    if not pyproject.exists():
        return {}
    try:
        import tomllib
    except ModuleNotFoundError:
        return {}
    with open(pyproject, "rb") as f:
        return tomllib.load(f).get("tool", {}).get("sdp-test", {})


def _resolve_bundle_file(config) -> Path | None:
    """Resolve default bundle file from config or convention."""
    cfg = _load_sdp_config(Path(config.rootdir))
    if "bundle_file" in cfg:
        return (Path(config.rootdir) / cfg["bundle_file"]).resolve()
    default = Path(config.rootdir) / "databricks.yml"
    return default if default.exists() else None


# ---------------------------------------------------------------------------
# Pytest hooks
# ---------------------------------------------------------------------------

def _find_pipeline_files(rootdir: Path) -> list[Path]:
    """Find pipeline definition files in the project root."""
    candidates = [
        rootdir / "databricks.yml",
        rootdir / "spark-pipeline.yml",
        rootdir / "spark-pipeline.yaml",
    ]
    # Also check [tool.sdp-test].bundle_file from pyproject.toml.
    cfg = _load_sdp_config(rootdir)
    if "bundle_file" in cfg:
        candidates.insert(0, (rootdir / cfg["bundle_file"]).resolve())
    return [p for p in candidates if p.exists()]


def pytest_configure(config):
    """Ensure pipeline definition files are included in pytest's collection.

    When ``testpaths`` restricts scanning to a subdirectory (e.g. ``tests/``),
    pytest never visits the project root so ``pytest_collect_file`` never sees
    ``databricks.yml`` or ``spark-pipeline.yml``.  This hook adds any pipeline
    definition files found in the project root as extra collection arguments
    so that a bare ``pytest`` just works.

    Disable with ``[tool.sdp-test] auto_discover = false`` in pyproject.toml.
    """
    rootdir = Path(config.rootdir)
    cfg = _load_sdp_config(rootdir)
    if cfg.get("auto_discover") is False:
        return

    resolved_args: set[Path] = set()
    for arg in config.args:
        try:
            resolved_args.add(Path(arg).resolve())
        except (OSError, ValueError):
            pass

    for pipeline_file in _find_pipeline_files(rootdir):
        if pipeline_file.resolve() not in resolved_args:
            config.args.append(str(pipeline_file))


def pytest_collect_file(parent, file_path):
    """Collect SDP test files encountered during directory traversal."""
    if file_path.suffix in (".yml", ".yaml") and file_path.stem.endswith("_pipeline_tests"):
        return SDPSpecFile.from_parent(parent, path=file_path)
    if file_path.name == "databricks.yml":
        return BundleFile.from_parent(parent, path=file_path)
    if file_path.name in ("spark-pipeline.yml", "spark-pipeline.yaml"):
        return PipelineFile.from_parent(parent, path=file_path)
    if file_path.suffix in (".yml", ".yaml") and file_path.stem.endswith(".pipeline"):
        return PipelineFile.from_parent(parent, path=file_path)


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------

class SDPSpecFile(pytest.File):
    """Collector for a ``*_pipeline_tests.yml`` spec file."""

    def collect(self):
        bundle_file = _resolve_bundle_file(self.config)
        for spec_file, case, _context in cases_from_spec(self.path, bundle_file):
            suite = spec_file.stem.replace(".unit_tests", "")
            name = case.get("name", "unnamed")
            test_name = f"{suite}::{name}" if spec_file != self.path else name
            yield SDPTestItem.from_parent(self, name=test_name, case=case)


class BundleFile(pytest.File):
    """Collector for a ``databricks.yml`` bundle file — auto-discovers tests from all pipelines."""

    def collect(self):
        for spec_file, case, _context in cases_from_bundle(self.path):
            pipeline_name = case.get("pipeline_name") or "pipeline"
            suite = spec_file.stem.replace(".unit_tests", "")
            name = case.get("name", "unnamed")
            test_name = f"{pipeline_name}::{suite}::{name}"
            yield SDPTestItem.from_parent(self, name=test_name, case=case)


class PipelineFile(pytest.File):
    """Collector for a ``spark-pipeline.yml`` file — auto-discovers tests."""

    def collect(self):
        for spec_file, case, _context in cases_from_pipeline_file(self.path):
            suite = spec_file.stem.replace(".unit_tests", "")
            name = case.get("name", "unnamed")
            test_name = f"{suite}::{name}"
            yield SDPTestItem.from_parent(self, name=test_name, case=case)


# ---------------------------------------------------------------------------
# Test item
# ---------------------------------------------------------------------------

class SDPTestItem(pytest.Item):
    """A single pipeline test case."""

    def __init__(self, *, case: dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.case = case

    def runtest(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("sdp-test")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("WARN")

        result = run_case(spark, self.case)
        if result.left_minus_right != 0 or result.right_minus_left != 0:
            raise SDPTestFailure(self.case, result)

    def repr_failure(self, excinfo):
        if isinstance(excinfo.value, SDPTestFailure):
            return str(excinfo.value)
        return super().repr_failure(excinfo)

    def reportinfo(self):
        return self.path, None, self.name
