from __future__ import annotations

import os
from pathlib import Path

import pytest

JDK = Path.home() / ".local/opt/jdk17/Contents/Home"
if "JAVA_HOME" not in os.environ and JDK.exists():  # local dev convenience; CI sets JAVA_HOME
    os.environ["JAVA_HOME"] = str(JDK)

from lakehouse.config import Settings  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from lakehouse.spark import get_spark

    session = get_spark(Settings(_env_file=None, spark_master="local[1]", spark_shuffle_partitions=1))
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        lake_root=tmp_path / "lake",
        landing_root=tmp_path / "landing",
        spark_master="local[1]",
        spark_shuffle_partitions=1,
    )


def pytest_collection_modifyitems(config, items):
    if not os.getenv("TEST_PG_ADMIN_URL"):
        skip = pytest.mark.skip(reason="TEST_PG_ADMIN_URL not set")
        for item in items:
            if "postgres" in item.keywords:
                item.add_marker(skip)
