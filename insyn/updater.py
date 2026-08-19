"""
Dataset updater supporting full historical download and fast incremental updates.
Operates purely on CSV using Python standard library, with optional Parquet export.
"""

from __future__ import annotations

import csv
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from insyn.client import EXPECTED_HEADER, MAR_INCEPTION_DATE, InsynClient

console = Console()


def make_row_dedup_key(row: List[str]) -> tuple:
    """Generate unique deduplication key for a transaction row."""
    # [0]=Publiceringsdatum, [1]=Emittent, [3]=Anmälningsskyldig, [4]=Person, [11]=Karaktär,
    # [14]=ISIN, [15]=Transaktionsdatum, [16]=Volym, [18]=Pris
    if len(row) >= 19:
        return (row[0], row[1], row[3], row[4], row[11], row[14], row[15], row[16], row[18])
    return tuple(row)


def generate_date_windows(start: date, end: date, window_days: int) -> List[Tuple[date, date]]:
    """Partition date range into consecutive intervals."""
    windows = []
    curr = start
    while curr <= end:
        w_end = min(end, curr + timedelta(days=window_days - 1))
        windows.append((curr, w_end))
        curr = w_end + timedelta(days=1)
    return windows


class CheckpointManager:
    """Manage disk checkpoints for resuming interrupted full-history extractions."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.index_file = self.cache_dir / "index.json"
        self.completed_windows: Set[str] = set()
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
                header = data.get("header", []) or list(EXPECTED_HEADER)
                target_len = len(header)
                raw_rows = data.get("rows", [])
                cleaned_rows = []
                for r in raw_rows:
                    if len(r) < target_len:
                        r = r + [""] * (target_len - len(r))
                    elif len(r) > target_len:
                        r = r[:target_len]
                    if any(r):
                        cleaned_rows.append(r)
                return header, cleaned_rows
        return [], []


class DatasetUpdater:
    """Manages CSV dataset storage and fast incremental/full updates."""

    def __init__(self, data_dir: Path = Path("data")):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.data_dir / "insynsregistret.csv"
        self.parquet_path = self.data_dir / "insynsregistret.parquet"

    def has_existing_dataset(self) -> bool:
        """Check if local CSV dataset file exists."""
        return self.csv_path.exists()

    def get_latest_publication_date(self) -> Optional[date]:
        """
        Query latest publication date from the existing CSV dataset.
        Since the CSV is sorted descending by publication date, the first data row has the latest date.
        """
        if not self.csv_path.exists():
            return None

        try:
            with open(self.csv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter=";")
                header = next(reader, None)
                first_row = next(reader, None)
                if first_row and first_row[0]:
                    pub_str = first_row[0].strip()
                    # e.g. "2026-08-18 21:43:25"
                    date_part = pub_str.split()[0]
                    return date.fromisoformat(date_part)
        except Exception as e:
            console.print(f"[yellow]Warning inspecting latest date from CSV: {e}[/yellow]")

        return None

    def load_existing_rows(self) -> List[List[str]]:
        """Load all rows from existing CSV dataset."""
        if not self.csv_path.exists():
            return []

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader, None)
            if not header:
                return []
            target_len = len(header)
            rows = []
            for r in reader:
                if len(r) < target_len:
                    r = r + [""] * (target_len - len(r))
                elif len(r) > target_len:
                    r = r[:target_len]
                if any(r):
                    rows.append(r)
            return rows

    def save_csv(self, header: List[str], rows: List[List[str]]) -> None:
        """Save dataset to clean standard UTF-8 CSV with minimal quoting."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        target_len = len(header)

        with open(self.csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for r in rows:
                if len(r) < target_len:
                    r = r + [""] * (target_len - len(r))
                elif len(r) > target_len:
                    r = r[:target_len]
                writer.writerow(r)

    def export_parquet(self, output_path: Optional[Path] = None) -> Path:
        """Optional helper to convert CSV to Parquet using DuckDB if available."""
        try:
            import duckdb
        except ImportError:
            raise RuntimeError(
                "DuckDB is required for Parquet export. Run with: uv run --extra parquet run.py --parquet"
            )

        if output_path is None:
            output_path = self.parquet_path

        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT * FROM read_csv(
                    '{self.csv_path.as_posix()}',
                    delim=';',
                    header=true,
                    decimal_separator=',',
                    strict_mode=true
                )
            ) TO '{output_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        con.close()
        return output_path

    def update_incremental(self, lookback_days: int = 3) -> Tuple[int, int]:
        """
        Fetch recent transactions (default last 3 days) and merge into the CSV dataset.
        Returns (new_records_count, total_records_count).
        """
        today = date.today()
        latest_date = self.get_latest_publication_date()

        if latest_date:
            start_d = max(MAR_INCEPTION_DATE, latest_date - timedelta(days=lookback_days))
        else:
            start_d = today - timedelta(days=lookback_days)

        console.print(Panel.fit(
            f"[bold green]FI Insynsregister Incremental Update[/bold green]\n"
            f"Query Range: [cyan]{start_d}[/cyan] to [cyan]{today}[/cyan] ({(today - start_d).days + 1} days)\n"
            f"Target CSV: [cyan]{self.csv_path}[/cyan]",
            title="Incremental Update",
        ))

        client = InsynClient(concurrency=2)
        header, fresh_rows, queries = client.fetch_interval_adaptive(start_d, today)
        if not header:
            header = list(EXPECTED_HEADER)

        existing_rows = self.load_existing_rows()

        # Deduplicate and merge in memory
        row_dict: Dict[tuple, List[str]] = {}
        for r in existing_rows:
            row_dict[make_row_dedup_key(r)] = r

        new_count = 0
        for r in fresh_rows:
            k = make_row_dedup_key(r)
            if k not in row_dict:
                new_count += 1
            row_dict[k] = r  # Updates revised status or adds new

        merged_rows = list(row_dict.values())
        merged_rows.sort(key=lambda r: r[0] if r else "", reverse=True)

        self.save_csv(header, merged_rows)

        csv_mb = self.csv_path.stat().st_size / (1024 * 1024)

        table = Table(title="Update Summary", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="dim")
        table.add_column("Value", justify="right", style="bold green")

        table.add_row("New Transactions Added", f"{new_count:,}")
        table.add_row("Total Transactions", f"{len(merged_rows):,}")
        table.add_row("CSV File Size", f"{csv_mb:.2f} MB")

        console.print(table)
        return new_count, len(merged_rows)

    def fetch_full_history(
        self,
        start_date: date = MAR_INCEPTION_DATE,
        end_date: Optional[date] = None,
        window_days: int = 14,
        workers: int = 4,
        cache_dir: Path = Path(".insyn_cache"),
        no_cache: bool = False,
    ) -> Tuple[int, int]:
        """Fetch the complete dataset from start_date to end_date."""
        if end_date is None:
            end_date = date.today()

        client = InsynClient(concurrency=workers)
        windows = generate_date_windows(start_date, end_date, window_days)
        checkpoint = None if no_cache else CheckpointManager(cache_dir)

        console.print(Panel.fit(
            f"[bold green]FI Insynsregister Full Historical Extraction[/bold green]\n"
            f"Date Range: [cyan]{start_date}[/cyan] to [cyan]{end_date}[/cyan] ({(end_date - start_date).days + 1} days)\n"
            f"Windows: [cyan]{len(windows)}[/cyan] ({window_days}-day intervals) | Workers: [cyan]{workers}[/cyan]",
            title="Full History",
        ))

        pending_windows = []
        cached_rows = []
        common_header = list(EXPECTED_HEADER)

        if checkpoint:
            for s, e in windows:
                if checkpoint.is_completed(s, e):
                    h, r = checkpoint.load_chunk(s, e)
                    if h:
                        common_header = h
                    cached_rows.extend(r)
                else:
                    pending_windows.append((s, e))
        else:
            pending_windows = list(windows)

        if cached_rows:
            console.print(f"[yellow]Loaded {len(cached_rows):,} rows from cache ({len(windows)-len(pending_windows)}/{len(windows)} windows).[/yellow]")

        fresh_rows = []
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
                h, r, q = client.fetch_interval_adaptive(w_start, w_end)
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
                        if h:
                            common_header = h
                        fresh_rows.extend(rows)
                        if checkpoint:
                            checkpoint.mark_completed(s, e, rows, h or common_header)

                        elapsed = time.time() - t_start
                        current_total = len(cached_rows) + len(fresh_rows)
                        speed = current_total / elapsed if elapsed > 0 else 0
                        progress.update(task_id, advance=1, rows=current_total, speed=speed)
                    except Exception as exc:
                        console.print(f"[red]Error fetching {w_start}..{w_end}: {exc}[/red]")

        all_rows = cached_rows + fresh_rows
        seen_keys = set()
        deduped = []
        for r in all_rows:
            k = make_row_dedup_key(r)
            if k not in seen_keys:
                seen_keys.add(k)
                deduped.append(r)

        deduped.sort(key=lambda r: r[0] if r else "", reverse=True)
        self.save_csv(common_header, deduped)

        csv_mb = self.csv_path.stat().st_size / (1024 * 1024)
        total_time = time.time() - t_start

        table = Table(title="Extraction Summary", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="dim")
        table.add_column("Value", justify="right", style="bold green")

        table.add_row("Total Unique Transactions", f"{len(deduped):,}")
        table.add_row("Time Elapsed", f"{total_time:.2f}s")
        table.add_row("CSV Output", f"{self.csv_path} ({csv_mb:.2f} MB)")

        console.print(table)
        return len(deduped), len(deduped)
