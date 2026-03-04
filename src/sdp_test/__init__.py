"""sdp-test: Declarative unit testing for Spark Declarative Pipelines."""

from .bundle import load_bundle_context, load_pipeline_test_spec, resolve_template
from .model_sql import register_df_as_view, render_model_query, rows_as_dicts
from . import pipelines_shim
from .spec_models import (
    BundleSpec,
    ExpectSpec,
    GivenInputSpec,
    PipelineEntrySpec,
    PipelineRefSpec,
    TestCaseSpec,
    UnitSpec,
)
from .spec_runner import (
    CaseResult,
    all_cases,
    case_id,
    cases_from_bundle,
    cases_from_pipeline_def,
    cases_from_pipeline_file,
    cases_from_spec,
    find_spec_files,
    run_case,
)

__all__ = [
    "CaseResult",
    "all_cases",
    "case_id",
    "cases_from_bundle",
    "cases_from_pipeline_def",
    "cases_from_pipeline_file",
    "cases_from_spec",
    "pipelines_shim",
    "find_spec_files",
    "load_bundle_context",
    "load_pipeline_test_spec",
    "register_df_as_view",
    "render_model_query",
    "resolve_template",
    "rows_as_dicts",
    "run_case",
    "BundleSpec",
    "ExpectSpec",
    "GivenInputSpec",
    "PipelineEntrySpec",
    "PipelineRefSpec",
    "TestCaseSpec",
    "UnitSpec",
]
