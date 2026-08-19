#!/usr/bin/env python3
"""
CLI entry point for oppna-insynsregistret.

Usage:
    uv run run.py                # Incremental update (default, checks last 3 days)
    uv run run.py --full         # Full historical download (2016-07-03 to today)
    uv run run.py --lookback 30  # Check last 30 days
    uv run run.py --parquet      # Also export data/insynsregistret.parquet
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from insyn.client import MAR_INCEPTION_DATE
from insyn.updater import DatasetUpdater


def main():
    parser = argparse.ArgumentParser(
        description="Open Data pipeline for Swedish Finansinspektionen Insynsregister.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Perform a full historical extraction from MAR inception (2016-07-03) to today.",
    )
    parser.add_argument(
        "--lookback-days",
        "-l",
        type=int,
        default=3,
        help="Number of days to look back for incremental updates.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Custom start date (YYYY-MM-DD) for historical extraction.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Custom end date (YYYY-MM-DD) for historical extraction.",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=4,
        help="Number of concurrent worker threads.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory to store dataset files.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable disk caching.",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="Also export dataset as Parquet (requires duckdb).",
    )

    args = parser.parse_args()

    updater = DatasetUpdater(data_dir=args.data_dir)

    # If --full is requested or dataset does not exist yet
    if args.full or not updater.has_existing_dataset() or args.start_date:
        start_d = date.fromisoformat(args.start_date) if args.start_date else MAR_INCEPTION_DATE
        end_d = date.fromisoformat(args.end_date) if args.end_date else date.today()
        updater.fetch_full_history(
            start_date=start_d,
            end_date=end_d,
            workers=args.workers,
            no_cache=args.no_cache,
        )
    else:
        # Default: incremental update
        updater.update_incremental(lookback_days=args.lookback_days)

    if args.parquet:
        pq_path = updater.export_parquet()
        print(f"Exported Parquet: {pq_path}")


if __name__ == "__main__":
    main()
