"""Pipeline configuration from environment variables / .env."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ConfigurationError(RuntimeError):
    pass


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Storage layout. Bronze/Silver/Quarantine live under LAKE_ROOT; CSV exports land in LANDING_ROOT.
    lake_root: Path = PROJECT_ROOT / "lake"
    landing_root: Path = PROJECT_ROOT / "data" / "landing"

    # Operational (OLTP) source database and analytics warehouse. No defaults: credentials
    # must come from the environment / .env (see .env.example).
    source_db_url: str | None = None
    warehouse_db_url: str | None = None

    spark_master: str = "local[2]"
    spark_driver_memory: str = "1g"
    spark_shuffle_partitions: int = 4

    dbt_project_dir: Path = PROJECT_ROOT / "dbt"
    dbt_target: str = "dev"

    # Fail the batch if more than this share of incoming rows is quarantined.
    max_quarantine_ratio: float = Field(default=0.05, ge=0, le=1)

    def require_databases(self) -> None:
        missing = [n for n in ("SOURCE_DB_URL", "WAREHOUSE_DB_URL") if not getattr(self, n.lower())]
        if missing:
            raise ConfigurationError(f"Set {' and '.join(missing)} (copy .env.example to .env).")

    @property
    def bronze_root(self) -> Path:
        return self.lake_root / "bronze"

    @property
    def silver_root(self) -> Path:
        return self.lake_root / "silver"

    @property
    def quarantine_root(self) -> Path:
        return self.lake_root / "quarantine"

    @property
    def metrics_root(self) -> Path:
        return self.lake_root / "_metrics"


@lru_cache
def get_settings() -> Settings:
    return Settings()
