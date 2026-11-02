"""Run the SQL reporting queries and write a Markdown analytics report.

Usage: python -m analytics.report [--out reports/analytics_report.md]
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import psycopg

from lakehouse.config import PROJECT_ROOT, get_settings

QUERY_DIR = Path(__file__).parent / "queries"


def run_queries(url: str) -> list[tuple[str, list[str], list[tuple]]]:
    results = []
    with psycopg.connect(url) as conn:
        for path in sorted(QUERY_DIR.glob("*.sql")):
            cur = conn.execute(path.read_text())
            results.append((path.stem, [d.name for d in cur.description], cur.fetchall()))
    return results


def to_markdown(results) -> str:
    lines = [f"# Retail Analytics Report\n\nGenerated {dt.datetime.now():%Y-%m-%d %H:%M} from the warehouse.\n"]
    for name, columns, rows in results:
        title = name.split("_", 1)[1].replace("_", " ").title()
        lines.append(f"## {title}\n")
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("|" + "---|" * len(columns))
        for row in rows:
            lines.append("| " + " | ".join("" if v is None else str(v) for v in row) + " |")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=PROJECT_ROOT / "reports" / "analytics_report.md")
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(to_markdown(run_queries(get_settings().warehouse_db_url)))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
