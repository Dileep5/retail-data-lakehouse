"""PostgreSQL helpers shared by the generator, loaders and reconciliation."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


@contextmanager
def connect(url: str, autocommit: bool = False):
    with psycopg.connect(url, autocommit=autocommit) as conn:
        yield conn


def ensure_database(url: str) -> None:
    """Create the database named in ``url`` if it does not exist (connects to 'postgres')."""
    params = conninfo_to_dict(url)
    dbname = params["dbname"]
    admin = make_conninfo(**{**params, "dbname": "postgres"})
    with psycopg.connect(admin, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))


def drop_database(url: str) -> None:
    params = conninfo_to_dict(url)
    admin = make_conninfo(**{**params, "dbname": "postgres"})
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(params["dbname"])))


def run_sql_file(url: str, filename: str) -> None:
    with connect(url) as conn:
        conn.execute((SQL_DIR / filename).read_text())
        conn.commit()


def dbt_env(url: str) -> dict[str, str]:
    """Translate a libpq URL into the DW_* variables used by dbt/profiles.yml."""
    p = conninfo_to_dict(url)
    return {
        "DW_HOST": p.get("host", "localhost"),
        "DW_PORT": str(p.get("port", 5432)),
        "DW_USER": p.get("user", "postgres"),
        "DW_PASSWORD": p.get("password", ""),
        "DW_DBNAME": p["dbname"],
    }
