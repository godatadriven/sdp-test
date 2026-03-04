"""Pytest configuration with local Apache Spark fixture."""

from __future__ import annotations

from typing import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide a local SparkSession fixture for tests."""
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("sdp-test-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("WARN")
    jvm = spark_session._jvm
    log_manager = jvm.org.apache.logging.log4j.LogManager
    log_manager.getLogger("SQLQueryContextLogger").setLevel(jvm.org.apache.logging.log4j.Level.FATAL)
    yield spark_session
    spark_session.stop()
