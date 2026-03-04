"""Pytest plugin for automatic SDP test collection.

Registers as a pytest plugin via the ``pytest11`` entry point. When installed,
tests are discovered automatically from pipeline definitions — no conftest.py
or test_*.py boilerplate required.

Auto-discovery: the plugin automatically finds pipeline definition files
(``databricks.yml``, ``spark-pipeline.yml``) in the project root and collects
the tests they define — even when ``testpaths`` points elsewhere.

Disable with ``pytest -p no:sdp_test`` or ``[tool.sdp-test] auto_discover = false``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from .spec_runner import (
    cases_from_bundle,
    cases_from_pipeline_file,
    cases_from_spec,
    run_case,
)


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
    except ModuleNotFoundError:  # pragma: no cover – tomllib is always available on Python 3.11+
        return {}  # pragma: no cover
    with open(pyproject, "rb") as f:
        return tomllib.load(f).get("tool", {}).get("sdp-test", {})


def _resolve_bundle_file(config) -> Path | None:
    """Resolve default bundle file from config or convention."""
    default = Path(config.rootdir) / "databricks.yml"
    return default if default.exists() else None


def _find_pipeline_files(rootdir: Path) -> list[Path]:
    """Find pipeline definition files in the project root."""
    candidates = [
        rootdir / "databricks.yml",
        rootdir / "spark-pipeline.yml",
        rootdir / "spark-pipeline.yaml",
    ]
    return [p for p in candidates if p.exists()]


# ---------------------------------------------------------------------------
# Pytest hooks
# ---------------------------------------------------------------------------

def pytest_configure(config):  # pragma: no cover – runs in subprocess via pytester
    """Auto-discover pipeline definition files and inject them for collection.

    Only auto-discovered files are collected — passing pipeline files as
    explicit CLI arguments is not supported.

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

    discovered: set[Path] = set()
    for pipeline_file in _find_pipeline_files(rootdir):
        resolved = pipeline_file.resolve()
        discovered.add(resolved)
        if resolved not in resolved_args:
            config.args.append(str(pipeline_file))

    config._sdp_discovered_files = discovered


def pytest_collect_file(parent, file_path):  # pragma: no cover – runs in subprocess via pytester
    """Collect SDP test files encountered during directory traversal."""
    # Always collect *_pipeline_tests.yml spec files.
    if file_path.suffix in (".yml", ".yaml") and file_path.stem.endswith("_pipeline_tests"):
        return SDPSpecFile.from_parent(parent, path=file_path)

    # Only collect pipeline definition files that were auto-discovered
    # (not passed explicitly by the user).
    discovered = getattr(parent.config, "_sdp_discovered_files", set())
    if file_path.resolve() not in discovered:
        return None

    if file_path.name == "databricks.yml":
        return BundleFile.from_parent(parent, path=file_path)
    if file_path.name in ("spark-pipeline.yml", "spark-pipeline.yaml"):
        return PipelineFile.from_parent(parent, path=file_path)
    if file_path.suffix in (".yml", ".yaml") and file_path.stem.endswith(".pipeline"):
        return PipelineFile.from_parent(parent, path=file_path)


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------

class SDPSpecFile(pytest.File):  # pragma: no cover – runs in subprocess via pytester
    """Collector for a ``*_pipeline_tests.yml`` spec file."""

    def collect(self):
        bundle_file = _resolve_bundle_file(self.config)
        for spec_file, case, _context in cases_from_spec(self.path, bundle_file):
            suite = spec_file.stem.replace(".unit_tests", "")
            name = case.get("name", "unnamed")
            test_name = f"{suite}::{name}" if spec_file != self.path else name
            yield SDPTestItem.from_parent(self, name=test_name, case=case)


class BundleFile(pytest.File):  # pragma: no cover – runs in subprocess via pytester
    """Collector for a ``databricks.yml`` bundle file — auto-discovers tests from all pipelines."""

    def collect(self):
        for spec_file, case, _context in cases_from_bundle(self.path):
            pipeline_name = case.get("pipeline_name") or "pipeline"
            suite = spec_file.stem.replace(".unit_tests", "")
            name = case.get("name", "unnamed")
            test_name = f"{pipeline_name}::{suite}::{name}"
            yield SDPTestItem.from_parent(self, name=test_name, case=case)


class PipelineFile(pytest.File):  # pragma: no cover – runs in subprocess via pytester
    """Collector for a ``spark-pipeline.yml`` or ``*.pipeline.yml`` file."""

    def collect(self):
        for spec_file, case, _context in cases_from_pipeline_file(self.path):
            pipeline_name = case.get("pipeline_name") or "pipeline"
            suite = spec_file.stem.replace(".unit_tests", "")
            name = case.get("name", "unnamed")
            test_name = f"{pipeline_name}::{suite}::{name}"
            yield SDPTestItem.from_parent(self, name=test_name, case=case)


# ---------------------------------------------------------------------------
# Test item
# ---------------------------------------------------------------------------

class SDPTestItem(pytest.Item):  # pragma: no cover – runs in subprocess via pytester
    """A single pipeline test case."""

    def __init__(self, *, case: dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.case = case
        # Strip the source file (e.g. databricks.yml) from the nodeid so test
        # output shows only pipeline::suite::test_name.
        self._nodeid = self.name

    def runtest(self):
        from pyspark.sql import SparkSession

        # Lazily create a session-level temp dir via pytest's tmp_path_factory.
        if not hasattr(self.config, "_sdp_tmpdir"):
            self.config._sdp_tmpdir = str(
                self.config._tmp_path_factory.mktemp("sdp_test")
            )

        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("sdp-test")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.warehouse.dir", self.config._sdp_tmpdir)
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
        return "", None, self.name
