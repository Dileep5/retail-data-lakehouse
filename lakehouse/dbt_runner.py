"""Run dbt as a subprocess with warehouse credentials injected via environment."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from lakehouse.config import Settings
from lakehouse.db import dbt_env

logger = logging.getLogger(__name__)


class DbtError(RuntimeError):
    pass


def _dbt_executable() -> str:
    candidate = Path(sys.executable).parent / "dbt"
    if candidate.exists():
        return str(candidate)
    found = shutil.which("dbt")
    if not found:
        raise DbtError("dbt executable not found; install dbt-postgres in this environment.")
    return found


def run_dbt(settings: Settings, *args: str) -> str:
    cmd = [_dbt_executable(), *args, "--project-dir", str(settings.dbt_project_dir),
           "--profiles-dir", str(settings.dbt_project_dir), "--target", settings.dbt_target]
    env = {**os.environ, **dbt_env(settings.warehouse_db_url), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    logger.info("running: dbt %s", " ".join(args))
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        tail = "\n".join(proc.stdout.splitlines()[-40:])
        raise DbtError(f"dbt {' '.join(args)} failed (exit {proc.returncode}):\n{tail}\n{proc.stderr[-2000:]}")
    return proc.stdout
