"""Command-line entry point.

    python -m lakehouse.cli init
    python -m lakehouse.cli run --batch-date 2025-11-01            # all steps
    python -m lakehouse.cli step silver --batch-date 2025-11-01    # one step (used by Airflow)
    python -m lakehouse.cli demo --days 3                          # 3 batches + an idempotent re-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys

from lakehouse.config import get_settings
from lakehouse.generate import START_DATE
from lakehouse.pipeline import STEP_FUNCTIONS, init, run_batch


def _date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date '{value}', expected YYYY-MM-DD") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="create databases and schemas")
    run = sub.add_parser("run", help="run every step for one batch")
    run.add_argument("--batch-date", type=_date, required=True)
    run.add_argument("--skip-generate", action="store_true")
    step = sub.add_parser("step", help="run a single step")
    step.add_argument("name", choices=list(STEP_FUNCTIONS))
    step.add_argument("--batch-date", type=_date, required=True)
    demo = sub.add_parser("demo", help="run consecutive batches and re-run the last one")
    demo.add_argument("--days", type=int, default=3)
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    settings = get_settings()

    if args.command == "init":
        init(settings)
        print("initialised source and warehouse databases")
    elif args.command == "run":
        print(json.dumps(run_batch(settings, args.batch_date, skip_generate=args.skip_generate), indent=2,
                         default=str))
    elif args.command == "step":
        init(settings)
        print(json.dumps(STEP_FUNCTIONS[args.name](settings, args.batch_date), indent=2, default=str))
    elif args.command == "demo":
        dates = [START_DATE + dt.timedelta(days=i) for i in range(args.days)]
        for d in dates:
            run_batch(settings, d)
            print(f"batch {d} succeeded")
        run_batch(settings, dates[-1])
        print(f"re-run of batch {dates[-1]} succeeded (idempotent)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
