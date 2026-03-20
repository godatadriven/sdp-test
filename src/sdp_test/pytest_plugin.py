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
        import tomllib  # ty: ignore[unresolved-import]
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

_PIPELINE_SUFFIXES = {".yml", ".yaml"}
_PIPELINE_NAMES = {"databricks.yml", "spark-pipeline.yml", "spark-pipeline.yaml"}


def _is_pipeline_arg(arg: str) -> bool:
    """Check if a CLI arg looks like a pipeline definition file."""
    p = Path(arg)
    if p.name in _PIPELINE_NAMES:
        return True
    if p.suffix in _PIPELINE_SUFFIXES and p.stem.endswith(".pipeline"):
        return True
    return False


def pytest_configure(config):  # pragma: no cover – runs in subprocess via pytester
    """Auto-discover pipeline definition files and inject them for collection.

    Pipeline files passed as explicit CLI arguments (e.g.
    ``pytest sdp/spark-pipeline.yml``) are always collected.  In addition,
    when ``auto_discover`` is enabled (the default), the plugin injects
    pipeline files found in the project root.

    Disable auto-discovery with ``[tool.sdp-test] auto_discover = false``.
    """
    rootdir = Path(config.rootdir)
    cfg: dict[str, Any] = _load_sdp_config(rootdir)
    config._variable_resolution_depth = cfg.get("variable_resolution_depth", 5)

    # Always track explicit CLI args so they can be collected even when
    # auto_discover is disabled.
    explicit_pipeline_args: set[Path] = set()
    for arg in config.args:
        try:
            resolved = Path(arg).resolve()
            if _is_pipeline_arg(arg) and resolved.exists():
                explicit_pipeline_args.add(resolved)
        except (OSError, ValueError):
            pass
    config._sdp_explicit_pipeline_args = explicit_pipeline_args

    # If the user explicitly passed pipeline files, skip auto-discovery
    # to avoid collecting duplicate tests.
    if cfg.get("auto_discover") is False or explicit_pipeline_args:
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

    # Collect pipeline definition files that were auto-discovered OR
    # explicitly passed as CLI arguments.
    resolved = file_path.resolve()
    discovered = getattr(parent.config, "_sdp_discovered_files", set())
    explicit = getattr(parent.config, "_sdp_explicit_pipeline_args", set())
    if resolved not in discovered and resolved not in explicit:
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
        depth = getattr(self.config, "_variable_resolution_depth", 5)
        for spec_file, case, _context in cases_from_spec(self.path, bundle_file, variable_resolution_depth=depth):
            suite = spec_file.stem.replace(".unit_tests", "")
            name = case.get("name", "unnamed")
            test_name = f"{suite}::{name}" if spec_file != self.path else name
            yield SDPTestItem.from_parent(self, name=test_name, case=case)


class BundleFile(pytest.File):  # pragma: no cover – runs in subprocess via pytester
    """Collector for a ``databricks.yml`` bundle file — auto-discovers tests from all pipelines."""

    def collect(self):
        depth = getattr(self.config, "_variable_resolution_depth", 5)
        for spec_file, case, _context in cases_from_bundle(self.path, variable_resolution_depth=depth):
            pipeline_name = case.get("pipeline_name") or "pipeline"
            suite = spec_file.stem.replace(".unit_tests", "")
            name = case.get("name", "unnamed")
            test_name = f"{pipeline_name}::{suite}::{name}"
            yield SDPTestItem.from_parent(self, name=test_name, case=case)


class PipelineFile(pytest.File):  # pragma: no cover – runs in subprocess via pytester
    """Collector for a ``spark-pipeline.yml`` or ``*.pipeline.yml`` file."""

    def collect(self):
        depth = getattr(self.config, "_variable_resolution_depth", 5)
        for spec_file, case, _context in cases_from_pipeline_file(self.path, variable_resolution_depth=depth):
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
            self.config._sdp_tmpdir = str(  # ty: ignore[unresolved-attribute]
                self.config._tmp_path_factory.mktemp("sdp_test")  # ty: ignore[unresolved-attribute]
            )

        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("sdp-test")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.warehouse.dir", self.config._sdp_tmpdir)  # ty: ignore[unresolved-attribute]
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("WARN")

        result = run_case(spark, self.case)
        if result.left_minus_right != 0 or result.right_minus_left != 0:
            raise SDPTestFailure(self.case, result)

    def repr_failure(self, excinfo, style=None):
        if isinstance(excinfo.value, SDPTestFailure):
            return str(excinfo.value)
        return super().repr_failure(excinfo, style=style)

    def reportinfo(self):
        return "", None, self.name
