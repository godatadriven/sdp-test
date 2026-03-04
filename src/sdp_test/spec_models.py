from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class GivenInputSpec(BaseModel):
    """Rows to register for a source table before executing a model query."""

    model_config = ConfigDict(extra="forbid")

    table: str = Field(description="Schema-qualified input table name, e.g. bronze.raw_stores.")
    rows: list[dict[str, Any]] = Field(default_factory=list, description="Input rows for the table.")


class ExpectSpec(BaseModel):
    """Expected result rows for a test case."""

    model_config = ConfigDict(extra="forbid")

    rows: list[dict[str, Any]] = Field(default_factory=list, description="Expected output rows.")


class TestCaseSpec(BaseModel):
    """A single model test case."""

    model_config = ConfigDict(extra="allow")

    name: str | None = Field(default=None, description="Optional test case name.")
    model: str = Field(description="Path to the model file (.sql or .py).")
    callable: str | None = Field(
        default=None,
        description="Optional callable name for Python model files. Defaults to file stem.",
    )
    log_level: str | None = Field(
        default=None,
        description="Optional per-test log level: DEBUG, INFO, WARN, ERROR, NONE.",
    )
    given: list[GivenInputSpec] = Field(default_factory=list, description="Input fixtures for referenced tables.")
    expect: ExpectSpec = Field(default_factory=ExpectSpec, description="Expected query result rows.")


class BundleSpec(BaseModel):
    """Bundle resolution options for a pipeline entry spec."""

    model_config = ConfigDict(extra="forbid")

    file: str | None = Field(default=None, description="Path to databricks.yml bundle file.")
    target: str | None = Field(default=None, description="Optional bundle target.")
    variables: dict[str, Any] = Field(default_factory=dict, description="Variable overrides.")


class PipelineRefSpec(BaseModel):
    """Alternative object syntax for pipeline selection."""

    model_config = ConfigDict(extra="forbid")

    ref: str | None = Field(default=None, description="Pipeline reference like pipelines.jaffle_shop_sql.")
    file: str | None = Field(default=None, description="Optional resource file path.")
    key: str | None = Field(default=None, description="Pipeline key in resource file.")

    @model_validator(mode="after")
    def validate_shape(self) -> "PipelineRefSpec":
        if self.ref:
            return self
        if self.file and self.key:
            return self
        raise ValueError("Pipeline object must define either ref or file+key")


class PipelineEntrySpec(BaseModel):
    """Top-level spec that bootstraps pipeline context and unit test discovery."""

    model_config = ConfigDict(extra="allow")

    suite: str | None = Field(default=None, description="Optional suite name.")
    log_level: str | None = Field(
        default=None,
        description="Default log level for tests in this pipeline spec: DEBUG, INFO, WARN, ERROR, NONE.",
    )
    bundle: BundleSpec = Field(default_factory=BundleSpec)
    pipeline: str | PipelineRefSpec = Field(description="Pipeline ref (pipelines.<key>) or object form.")
    defaults: dict[str, Any] = Field(default_factory=dict, description="Default substitutions for all tests.")
    tests: list[TestCaseSpec] = Field(default_factory=list, description="Optional inline tests in this file.")


class UnitSpec(BaseModel):
    """Colocated unit spec file discovered under pipeline code directories."""

    model_config = ConfigDict(extra="allow")

    log_level: str | None = Field(
        default=None,
        description="Default log level for tests in this unit spec: DEBUG, INFO, WARN, ERROR, NONE.",
    )
    tests: list[TestCaseSpec] = Field(default_factory=list, description="Unit test cases in this file.")
