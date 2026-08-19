# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests>=2.31.0",
#     "urllib3>=2.0.0",
#     "rich>=13.0.0",
#     "duckdb>=0.10.0",
# ]
# ///
"""
Fetch the complete Swedish Finansinspektionen Insynsregister (PDMR Transactions under MAR).

This script reverse-engineers the FI Market Search (marknadssok.fi.se) export API
to retrieve the dataset ~100x faster than HTML scraping (up to 1,000 rows per query
in CSV format rather than 10 rows per HTML page).

Features:
- Contiguous date windowing by Publiceringsdatum (publication date).
- Automatic adaptive bisection: if a date window reaches the 1,000-row export ceiling,
  it automatically splits the interval in half recursively to ensure no records are lost.
- Multi-threaded concurrent downloads with thread-safe rate-limiting and connection pooling.
- TLS 1.2 compatibility layer for reliable communication with FI's IIS server.
- Automatic resume / caching support to resume interrupted runs without re-fetching.
- Normalization: cleans non-breaking spaces (\\xa0), strips trailing empty columns,
  pads missing columns, and exports to UTF-8 CSV, Parquet, JSON Lines, or SQLite.
- Rich terminal UI with progress bars, live throughput (rows/sec), and summary stats.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sqlite3
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from urllib3.util import Retry

BASE_URL = "https://marknadssok.fi.se/Publiceringsklient/sv-SE/Search/Search"
DEFAULT_START_DATE = "2016-07-03"  # Date when EU MAR regulation came into force in Sweden
DEFAULT_WINDOW_DAYS = 14
MAX_EXPORT_ROWS = 1000

console = Console()


class TLS12Adapter(HTTPAdapter):
    """Custom SSL adapter forcing TLS 1.2 for compatibility with FI IIS server."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


def get_http_session(concurrency: int = 4) -> requests.Session:
    """Create a configured requests Session with TLS 1.2, connection pooling, and retry logic."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.4,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = TLS12Adapter(
        max_retries=retries,
        pool_connections=concurrency * 2,
        pool_maxsize=concurrency * 2,
    )
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
    })
    return session


def clean_cell(text: str) -> str:
    """Clean invisible characters, non-breaking spaces, and normalize internal newlines."""
    if not text:
        return ""
    cleaned = text.replace("\xa0", " ").strip()
    cleaned = cleaned.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    return cleaned


def parse_export_csv(raw_bytes: bytes) -> Tuple[List[str], List[List[str]]]:
    """
    Parse UTF-16LE CSV bytes from Finansinspektionen export.
    Properly handles trailing semicolons, missing trailing NULL columns, and embedded newlines.
    Returns (header_columns, list_of_cleaned_row_cells).
    """
    text = raw_bytes.decode("utf-16", errors="replace")
    if not text.strip():
        return [], []

    reader = csv.reader(io.StringIO(text), delimiter=";")
    parsed_rows = list(reader)
    if not parsed_rows:
        return [], []

    header = [clean_cell(col) for col in parsed_rows[0]]
    while header and header[-1] == "":
        header.pop()

    target_len = len(header)

    data_rows = []
    for row in parsed_rows[1:]:
        cleaned = [clean_cell(cell) for cell in row]
        if not any(cleaned):
            continue
        # Pad row to match full header length if FI truncated trailing empty fields
        if len(cleaned) < target_len:
            cleaned.extend([""] * (target_len - len(cleaned)))
        elif len(cleaned) > target_len:
            while len(cleaned) > target_len and cleaned[-1] == "":
                cleaned.pop()
            if len(cleaned) > target_len:
                cleaned = cleaned[:target_len]

        data_rows.append(cleaned)

    return header, data_rows


def fetch_interval(
    session: requests.Session,
    date_from: date,
    date_to: date,
    search_type: str = "Insyn",
    timeout: float = 12.0,
) -> Tuple[List[str], List[List[str]]]:
    """
    Fetch a single date range from FI.
    Returns (header, data_rows).
    """
    params = {
        "SearchFunctionType": search_type,
        "Publiceringsdatum.From": date_from.isoformat(),
        "Publiceringsdatum.To": date_to.isoformat(),
        "button": "export",
    }
    for attempt in range(5):
        try:
            resp = session.get(BASE_URL, params=params, timeout=timeout)
            if resp.status_code == 200 and resp.content:
                header, rows = parse_export_csv(resp.content)
                return header, rows
        except Exception as exc:
            time.sleep(0.3 * (attempt + 1))

    return [], []


def fetch_interval_adaptive(
    session: requests.Session,
    date_from: date,
    date_to: date,
    search_type: str = "Insyn",
    timeout: float = 12.0,
) -> Tuple[List[str], List[List[str]], int]:
    """
    Fetch a date interval with automatic recursive bisection if row count == MAX_EXPORT_ROWS.
    Returns (header, data_rows, query_count).
    """
    stack = [(date_from, date_to)]
    all_rows: List[List[str]] = []
    final_header: List[str] = []
    queries = 0

    while stack:
        s, e = stack.pop()
        queries += 1
        header, rows = fetch_interval(session, s, e, search_type=search_type, timeout=timeout)
        if header and not final_header:
            final_header = header

        if len(rows) >= MAX_EXPORT_ROWS and s < e:
            # Reached export ceiling: split in half
            mid = s + (e - s) // 2
            stack.append((mid + timedelta(days=1), e))
            stack.append((s, mid))
        else:
            all_rows.extend(rows)

    return final_header, all_rows, queries


def generate_date_windows(start: date, end: date, window_days: int) -> List[Tuple[date, date]]:
    """Partition date range into consecutive windows."""
    windows = []
    curr = start
    while curr <= end:
        w_end = min(end, curr + timedelta(days=window_days - 1))
        windows.append((curr, w_end))
        curr = w_end + timedelta(days=1)
    return windows


def make_row_dedup_key(row: List[str]) -> tuple:
    """Generate unique deduplication key for a transaction row."""
    # Columns typically: [0]=Publiceringsdatum, [1]=Emittent, [2]=LEI, [3]=Anmälningsskyldig,
    # [4]=Person i ledande ställning, [5]=Befattning, [11]=Karaktär, [14]=ISIN, [15]=Transaktionsdatum, [16]=Volym, [18]=Pris
    if len(row) >= 19:
        return (row[0], row[1], row[3], row[4], row[11], row[14], row[15], row[16], row[18])
    return tuple(row)


def save_to_csv(filepath: Path, header: List[str], rows: List[List[str]]) -> None:
    """Save rows to standard UTF-8 CSV with quotes where needed, strictly padded to header length."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    target_len = len(header)
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        if header:
            writer.writerow(header)
        for row in rows:
            if len(row) < target_len:
                row = row + [""] * (target_len - len(row))
            elif len(row) > target_len:
                row = row[:target_len]
            writer.writerow(row)


def save_to_jsonl(filepath: Path, header: List[str], rows: List[List[str]]) -> None:
    """Save rows to newline-delimited JSON (JSONL)."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for row in rows:
            record = {}
            for i, col in enumerate(header):
                record[col] = row[i] if i < len(row) else ""
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def save_to_sqlite(filepath: Path, header: List[str], rows: List[List[str]]) -> None:
    """Save rows into a SQLite database with indexed columns."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(filepath)
    cur = conn.cursor()

    col_names = []
    col_defs = []
    for h in header:
        safe_col = re.sub(r"[^\w]", "_", h)
        if not safe_col or safe_col[0].isdigit():
            safe_col = "col_" + safe_col
        col_names.append(safe_col)
        col_defs.append(f'"{safe_col}" TEXT')

    cur.execute(f"CREATE TABLE IF NOT EXISTS insyn ({', '.join(col_defs)})")
    placeholders = ", ".join(["?"] * len(col_names))

    cur.execute("BEGIN TRANSACTION")
    cur.executemany(
        f"INSERT INTO insyn ({', '.join(col_names)}) VALUES ({placeholders})",
        [row + [""] * (len(col_names) - len(row)) for row in rows],
    )
    conn.commit()

    for idx_col in ["Publiceringsdatum", "Emittent", "ISIN", "Transaktionsdatum", "LEI_kod"]:
        safe_idx = re.sub(r"[^\w]", "_", idx_col)
        if safe_idx in col_names:
            try:
                cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{safe_idx}" ON insyn ("{safe_idx}")')
            except Exception:
                pass
    conn.commit()
    conn.close()


def save_to_parquet(filepath: Path, csv_filepath: Path) -> None:
    """Convert generated CSV to strongly-typed Parquet using DuckDB."""
    import duckdb

    filepath.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT * FROM read_csv(
                '{csv_filepath.as_posix()}',
                delim=';',
                header=true,
                decimal_separator=',',
                strict_mode=true
            )
        ) TO '{filepath.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)


class CheckpointManager:
    """Manage download cache and checkpoints to allow resuming interrupted downloads."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.index_file = self.cache_dir / "index.json"
        self.completed_windows: set[str] = set()
        self._load()

    def _load(self):
        if self.index_file.exists():
            try:
                with open(self.index_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.completed_windows = set(data.get("completed", []))
            except Exception:
                self.completed_windows = set()

    def _save(self):
        with open(self.index_file, "w", encoding="utf-8") as f:
            json.dump({"completed": sorted(list(self.completed_windows))}, f, indent=2)

    def window_key(self, s: date, e: date) -> str:
        return f"{s.isoformat()}_{e.isoformat()}"

    def is_completed(self, s: date, e: date) -> bool:
        return self.window_key(s, e) in self.completed_windows

    def mark_completed(self, s: date, e: date, rows: List[List[str]], header: List[str]):
        key = self.window_key(s, e)
        chunk_path = self.cache_dir / f"{key}.json"
        with open(chunk_path, "w", encoding="utf-8") as f:
            json.dump({"header": header, "rows": rows}, f)
        self.completed_windows.add(key)
        self._save()

    def load_chunk(self, s: date, e: date) -> Tuple[List[str], List[List[str]]]:
        key = self.window_key(s, e)
        chunk_path = self.cache_dir / f"{key}.json"
        if chunk_path.exists():
            with open(chunk_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                header = [clean_cell(h) for h in data.get("header", [])]
                while header and header[-1] == "":
                    header.pop()
                target_len = len(header) if header else 22
                raw_rows = data.get("rows", [])
                cleaned_rows = []
                for r in raw_rows:
                    cr = [clean_cell(c) for c in r]
                    if len(cr) < target_len:
                        cr = cr + [""] * (target_len - len(cr))
                    elif len(cr) > target_len:
                        cr = cr[:target_len]
                    if any(cr):
                        cleaned_rows.append(cr)
                return header, cleaned_rows
        return [], []


def run_pipeline(
    start_date: date,
    end_date: date,
    window_days: int = DEFAULT_WINDOW_DAYS,
    workers: int = 4,
    output_path: Optional[Path] = None,
    output_format: str = "csv",
    cache_dir: Optional[Path] = None,
    no_cache: bool = False,
) -> Tuple[List[str], List[List[str]]]:
    """Execute the full multi-threaded extraction pipeline."""
    session = get_http_session(concurrency=workers)
    windows = generate_date_windows(start_date, end_date, window_days)

    if cache_dir is None:
        cache_dir = Path(".insyn_cache")
    checkpoint = None if no_cache else CheckpointManager(cache_dir)

    console.print(Panel.fit(
        f"[bold green]FI Insynsregister Extractor[/bold green]\n"
        f"Date Range: [cyan]{start_date}[/cyan] to [cyan]{end_date}[/cyan] ({(end_date - start_date).days + 1} days)\n"
        f"Windows: [cyan]{len(windows)}[/cyan] ({window_days}-day intervals)\n"
        f"Concurrency: [cyan]{workers} workers[/cyan] | Format: [cyan]{output_format}[/cyan]\n"
        f"Output Target: [cyan]{output_path or 'auto-generated'}[/cyan]",
        title="Configuration",
    ))

    # Identify windows that still need to be downloaded
    pending_windows: List[Tuple[date, date]] = []
    cached_rows: List[List[str]] = []
    common_header: List[str] = []

    if checkpoint:
        for s, e in windows:
            if checkpoint.is_completed(s, e):
                h, r = checkpoint.load_chunk(s, e)
                if h and not common_header:
                    common_header = h
                cached_rows.extend(r)
            else:
                pending_windows.append((s, e))
    else:
        pending_windows = list(windows)

    if cached_rows:
        console.print(f"[yellow]Loaded {len(cached_rows):,} rows from previous cache ({len(windows) - len(pending_windows)}/{len(windows)} windows).[/yellow]")

    fresh_rows: List[List[str]] = []
    total_queries = 0

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("• ETA:"),
        TimeRemainingColumn(),
        TextColumn("• [green]{task.fields[rows]:,} rows[/green] ({task.fields[speed]:.0f} r/s)"),
        console=console,
    )

    t_start = time.time()
    with progress:
        task_id = progress.add_task(
            "Fetching Insynsregister",
            total=len(windows),
            completed=len(windows) - len(pending_windows),
            rows=len(cached_rows),
            speed=0.0,
        )

        def worker_fetch(w_start: date, w_end: date):
            h, r, q = fetch_interval_adaptive(session, w_start, w_end)
            return w_start, w_end, h, r, q

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_window = {
                executor.submit(worker_fetch, s, e): (s, e)
                for s, e in pending_windows
            }

            for future in as_completed(future_to_window):
                w_start, w_end = future_to_window[future]
                try:
                    s, e, h, rows, q = future.result()
                    total_queries += q
                    if h and not common_header:
                        common_header = h

                    fresh_rows.extend(rows)
                    if checkpoint:
                        checkpoint.mark_completed(s, e, rows, h or common_header)

                    elapsed = time.time() - t_start
                    current_total = len(cached_rows) + len(fresh_rows)
                    speed = current_total / elapsed if elapsed > 0 else 0

                    progress.update(
                        task_id,
                        advance=1,
                        rows=current_total,
                        speed=speed,
                    )
                except Exception as exc:
                    console.print(f"[red]Error fetching window {w_start}..{w_end}: {exc}[/red]")

    all_rows = cached_rows + fresh_rows
    total_time = time.time() - t_start

    # Deduplicate rows by key
    seen_keys = set()
    deduped_rows = []
    for r in all_rows:
        k = make_row_dedup_key(r)
        if k not in seen_keys:
            seen_keys.add(k)
            deduped_rows.append(r)

    # Sort descending by publication date
    if deduped_rows:
        deduped_rows.sort(key=lambda r: r[0] if r else "", reverse=True)

    # Save to requested output file
    if output_path is None:
        ext = output_format if output_format != "sqlite" else "db"
        output_path = Path(f"insynsregistret_{start_date}_{end_date}.{ext}")

    if output_format == "csv":
        save_to_csv(output_path, common_header, deduped_rows)
    elif output_format == "jsonl":
        save_to_jsonl(output_path, common_header, deduped_rows)
    elif output_format in ("sqlite", "db"):
        save_to_sqlite(output_path, common_header, deduped_rows)
    elif output_format == "parquet":
        temp_csv = output_path.with_suffix(".temp.csv")
        save_to_csv(temp_csv, common_header, deduped_rows)
        save_to_parquet(output_path, temp_csv)
        if temp_csv.exists():
            temp_csv.unlink()

    # Render summary statistics
    file_size_mb = output_path.stat().st_size / (1024 * 1024) if output_path.exists() else 0.0

    summary_table = Table(title="Extraction Summary", show_header=True, header_style="bold magenta")
    summary_table.add_column("Metric", style="dim")
    summary_table.add_column("Value", justify="right", style="bold green")

    summary_table.add_row("Total Records Fetched", f"{len(all_rows):,}")
    summary_table.add_row("Unique Deduplicated Records", f"{len(deduped_rows):,}")
    summary_table.add_row("Duplicate Records Removed", f"{len(all_rows) - len(deduped_rows):,}")
    summary_table.add_row("Time Elapsed", f"{total_time:.2f}s")
    summary_table.add_row("Average Speed", f"{len(deduped_rows)/total_time:.1f} rows/s" if total_time > 0 else "N/A")
    summary_table.add_row("Output File", str(output_path))
    summary_table.add_row("File Size", f"{file_size_mb:.2f} MB")

    console.print()
    console.print(summary_table)

    return common_header, deduped_rows


def main():
    parser = argparse.ArgumentParser(
        description="Fast extraction of Swedish Finansinspektionen Insynsregister (PDMR transactions under MAR).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=DEFAULT_START_DATE,
        help="Start publication date (YYYY-MM-DD). Default: 2016-07-03 (MAR inception).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=date.today().isoformat(),
        help="End publication date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=DEFAULT_WINDOW_DAYS,
        help="Initial date window size in days (automatically bisects if >1000 records).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent worker threads.",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output file path (default: insynsregistret_<start>_<end>.<ext>).",
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=["csv", "jsonl", "sqlite", "parquet"],
        default="csv",
        help="Output file format.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(".insyn_cache"),
        help="Directory to store interval checkpoint chunks for resuming.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable disk caching and checkpointing.",
    )

    args = parser.parse_args()

    try:
        start_d = date.fromisoformat(args.start_date)
        end_d = date.fromisoformat(args.end_date)
    except ValueError as e:
        console.print(f"[bold red]Invalid date format:[/bold red] {e}. Please use YYYY-MM-DD.")
        sys.exit(1)

    if start_d > end_d:
        console.print("[bold red]Start date must be before or equal to end date.[/bold red]")
        sys.exit(1)

    run_pipeline(
        start_date=start_d,
        end_date=end_d,
        window_days=args.window_days,
        workers=args.workers,
        output_path=args.output,
        output_format=args.format,
        cache_dir=args.cache_dir,
        no_cache=args.no_cache,
    )


if __name__ == "__main__":
    main()
